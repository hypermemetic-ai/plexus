//! The bytes of every kind of frame, exactly.

use plexus_proto::{Body, CallId, Counter, Frame, Numbering};
use serde_json::value::RawValue;

fn raw(s: &str) -> Box<RawValue> {
    RawValue::from_string(s.to_owned()).unwrap()
}
fn text(f: &Frame, n: Numbering) -> String {
    String::from_utf8(f.encode(n)).unwrap()
}

#[test]
fn an_item_is_eleven_bytes_around_seven() {
    let f = Frame::item(CallId(7), r#"{"n":1}"#).unwrap();
    assert_eq!(text(&f, Numbering::Counted), r#"[7,{"n":1}]"#);
    assert_eq!(f.encode(Numbering::Counted).len(), 11);
}

#[test]
fn every_kind_counted() {
    let c = Numbering::Counted;
    assert_eq!(text(&Frame::hello("2f323690b0c10f6d", false), c), r#"[0,0,{"interface":"2f323690b0c10f6d"}]"#);
    assert_eq!(text(&Frame::open(CallId(7), "tickets.health", None), c), r#"[7,1,["tickets.health"]]"#);
    assert_eq!(text(&Frame::open(CallId(7), "tickets.get", Some(raw(r#"{"id":"KILN-2"}"#))), c), r#"[7,1,["tickets.get",{"id":"KILN-2"}]]"#);
    assert_eq!(text(&Frame { call: CallId(7), seq: 0, body: Body::Progress(raw("0.5")) }, c), "[7,3,0.5]");
    assert_eq!(text(&Frame::end(CallId(7)), c), "[7]");
    assert_eq!(text(&Frame::failed(CallId(7), "forbidden", "no grant"), c), r#"[7,4,{"code":"forbidden","message":"no grant"}]"#);
    assert_eq!(text(&Frame { call: CallId(7), seq: 0, body: Body::Cancel }, c), "[7,5,null]");
    assert_eq!(text(&Frame { call: CallId(7), seq: 0, body: Body::Credit(32) }, c), "[7,6,32]");
    assert_eq!(text(&Frame { call: CallId(7), seq: 0, body: Body::Resume(412) }, c), "[7,7,412]");
}

#[test]
fn numbered_writes_seq_and_never_the_short_forms() {
    let n = Numbering::Numbered;
    let mut item = Frame::item(CallId(7), r#"{"n":1}"#).unwrap();
    item.seq = 3;
    assert_eq!(text(&item, n), r#"[7,2,{"n":1},3]"#);
    let mut end = Frame::end(CallId(7));
    end.seq = 4;
    assert_eq!(text(&end, n), "[7,4,null,4]");
}

#[test]
fn what_is_written_is_read() {
    let frames = [
        Frame::hello("2f323690b0c10f6d", true),
        Frame::open(CallId(7), "tickets.get", Some(raw(r#"{"id":"KILN-2"}"#))),
        Frame::open(CallId(8), "tickets.health", None),
        Frame::item(CallId(7), r#"{"n":1}"#).unwrap(),
        Frame::item(CallId(7), "5").unwrap(),
        Frame::item(CallId(7), "[1,2,3]").unwrap(),
        Frame { call: CallId(7), seq: 0, body: Body::Progress(raw(r#""half""#)) },
        Frame::failed(CallId(7), "forbidden", "no grant"),
        Frame::end(CallId(8)),
        Frame { call: CallId(9), seq: 0, body: Body::Cancel },
        Frame { call: CallId(9), seq: 0, body: Body::Credit(32) },
        Frame { call: CallId(9), seq: 0, body: Body::Resume(412) },
    ];
    for numbering in [Numbering::Counted, Numbering::Numbered] {
        for (i, f) in frames.iter().enumerate() {
            let mut f = f.clone();
            f.seq = if numbering == Numbering::Numbered { i as u64 + 1 } else { 0 };
            let bytes = f.encode(numbering);
            let back = Frame::decode(&bytes).unwrap_or_else(|e| panic!("{}: {e}", String::from_utf8_lossy(&bytes)));
            assert_eq!(back.encode(numbering), bytes, "{}", String::from_utf8_lossy(&bytes));
            assert_eq!(back.seq, f.seq);
            assert_eq!(back.call, f.call);
        }
    }
}

#[test]
fn an_item_whose_body_is_a_number_is_still_an_item() {
    // `[7,5]` is an item whose body is 5, never a cancel: a kind is only
    // read from a frame of three elements.
    let f = Frame::decode(b"[7,5]").unwrap();
    assert!(matches!(f.body, Body::Item(ref r) if r.get() == "5"));
}

#[test]
fn what_is_not_a_frame_is_refused() {
    for bad in ["", "{}", "[]", "7", r#"["a",1]"#, "[7,9,null]", "[7,1,{}]", "[7,2,1,2,3]", "[7,2,1,\"x\"]", "[-1]"] {
        assert!(Frame::decode(bad.as_bytes()).is_err(), "{bad} was read as a frame");
    }
}

#[test]
fn both_ends_count_the_same() {
    let (mut sent, mut got) = (Counter::new(), Counter::new());
    let mut script = vec![
        Frame::open(CallId(7), "tickets.watch", None),
        Frame::item(CallId(7), "1").unwrap(),
        Frame::open(CallId(8), "tickets.health", None),
        Frame::item(CallId(7), "2").unwrap(),
        Frame::item(CallId(8), "1").unwrap(),
        Frame::end(CallId(8)),
        Frame::item(CallId(7), "3").unwrap(),
        Frame::end(CallId(7)),
    ];
    let mut seqs = vec![];
    for f in &mut script {
        sent.stamp(f);
        let mut back = Frame::decode(&f.encode(Numbering::Counted)).unwrap();
        assert_eq!(back.seq, 0);
        got.stamp(&mut back);
        assert_eq!(back.seq, f.seq);
        seqs.push(f.seq);
    }
    assert_eq!(seqs, [0, 1, 0, 2, 1, 2, 3, 4]);
    assert_eq!((sent.open_calls(), got.open_calls()), (0, 0));
}
