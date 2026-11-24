//! The collection entry point as used by an embedding application.
#![cfg(feature = "tokio")]

use plexus::{Application, Connection, pair};
use std::io;

#[plexus::activation]
pub mod counter {
    pub struct Counter(pub u32);

    impl Counter {
        pub fn value(&self) -> u32 {
            self.0
        }
    }
}

#[plexus::activation]
pub mod label {
    pub struct Label;

    impl Label {
        pub fn value(&self) -> String {
            String::from("kept")
        }
    }
}

#[tokio::test]
async fn mixed_initializer_and_pre_start_edits_reach_the_listener() {
    let mut builds = 0;
    let app = plexus::main(plexus::activations![
        {
            builds += 1;
            counter::Counter(1)
        },
        label::Label,
    ]);
    assert_eq!(builds, 1);
    let roots = app.activations();
    roots.replace(counter::Counter(2)).unwrap();
    app.with_listener(|server| async move {
        let (near, far) = pair();
        tokio::spawn(server.serve(far));
        let (client, drive) = Connection::new(near, None);
        tokio::spawn(drive);
        assert_eq!(client.call("counter.value", ()).await.unwrap().get(), "2");
        assert_eq!(
            client.call("label.value", ()).await.unwrap().get(),
            r#""kept""#
        );
        Ok(())
    })
    .run_async()
    .await
    .unwrap();
}

#[tokio::test]
async fn duplicate_names_fail_before_any_listener_is_created() {
    let app = plexus::main([counter::Counter(1), counter::Counter(2)]);
    let error = app
        .with_listener(|_| -> std::future::Ready<io::Result<()>> {
            panic!("listener created for invalid configuration")
        })
        .run_async()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
}

#[test]
fn accepts_empty_lists_vectors_and_the_original_single_activation_signature() {
    let _: fn(counter::Counter) -> io::Result<()> = plexus::main::<counter::Counter>;
    let empty: Application = plexus::main(plexus::activations![]);
    empty.activations().insert(counter::Counter(3)).unwrap();
    let app = plexus::main(vec![counter::Counter(4)]);
    app.activations().remove("counter").unwrap();
}
