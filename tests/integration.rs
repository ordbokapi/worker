use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;

// Integration tests to ensure main binary runs and prints help.

#[test]
fn test_help() {
    let mut cmd = cargo_bin_cmd!("ordbokapi-worker");
    cmd.arg("--help");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Ordbok API worker service"));
}
