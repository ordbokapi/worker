use assert_cmd::Command;
use predicates::prelude::*;

// Integration tests to ensure main binary runs and prints help.

#[test]
fn test_help() {
    let mut cmd = Command::cargo_bin("ordbokapi-worker").unwrap();
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Ordbok API worker service"));
}
