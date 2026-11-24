use plexus::Grant;
use serde::Deserialize;
use std::{env, fs, process::Command};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    org: String,
    owner: String,
    agent: String,
}

type Entry = Grant<String, String, Target>;
const FILE: &str = r#"
    // Issuer-qualified IDs are bound before use.
    principal(builder = "key:public-id")
    principal(me = "oidc:operator")
    grant(principal = builder, method = agent.connect,
          target = { org: "acme:", owner: me, agent: "notes", },);
"#;

#[test]
fn parses_exact_fields_and_rejects_partial_files() {
    let grants = Entry::parse(FILE).unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].principal, "key:public-id");
    assert_eq!(grants[0].method, "agent.connect");
    assert_eq!(grants[0].target.owner, "oidc:operator");
    for (from, to) in [
        ("owner: me", "owner: missing"),
        ("org:", "orgname:"),
        ("agent: \"notes\"", "agent: \"notes\", agent: \"second\""),
        ("method = agent.connect,", ""),
        ("principal(me", "principal(builder"),
        ("principal = builder", "subject = builder"),
    ] {
        let error = Entry::parse(&FILE.replace(from, to)).unwrap_err();
        assert!(error.to_string().contains("entry"), "{error}");
    }
    assert!(Entry::parse(&format!("{FILE} garbage(")).is_err());
}

// Give each case a private process environment, even under a parallel runner.
#[test]
fn env_selects_defaults_or_a_complete_file() {
    const VAR: &str = "PLEXUS_GRANT_FILE_TEST_PATH";
    const CASE: &str = "PLEXUS_GRANT_FILE_TEST_CASE";
    if let Ok(case) = env::var(CASE) {
        let defaults = Entry::parse(FILE).unwrap();
        let result = Entry::load_file_from_env(VAR, Some(defaults.clone()));
        match case.as_str() {
            "unset" => assert_eq!(result.unwrap(), defaults),
            "valid" => assert_eq!(result.unwrap()[0].target.agent, "work"),
            "empty" => assert!(result.unwrap().is_empty()),
            "bad" | "missing" => assert!(result.unwrap_err().to_string().contains(VAR)),
            _ => unreachable!(),
        }
        return;
    }
    let dir = env::temp_dir().join(format!("plexus-grant-file-{}", std::process::id()));
    fs::create_dir(&dir).unwrap();
    for (case, data) in [
        ("unset", None),
        ("valid", Some(FILE.replace("notes", "work"))),
        ("empty", Some(String::new())),
        ("bad", Some("invalid()".into())),
        ("missing", None),
    ] {
        let path = dir.join(case);
        if let Some(data) = data {
            fs::write(&path, data).unwrap();
        }
        let mut cmd = Command::new(env::current_exe().unwrap());
        cmd.args(["--exact", "env_selects_defaults_or_a_complete_file"])
            .env(CASE, case)
            .env_remove(VAR);
        if case != "unset" {
            cmd.env(VAR, &path);
        }
        assert!(cmd.status().unwrap().success(), "{case}");
    }
    fs::remove_dir_all(dir).unwrap();
}
