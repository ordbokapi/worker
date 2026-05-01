// SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This file is part of Ordbok API.
//
// Ordbok API is free software: you can redistribute it and/or modify it under
// the terms of the GNU Affero General Public License as published by the Free
// Software Foundation, either version 3 of the License, or (at your option) any
// later version.
//
// Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
// A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
// details.
//
// You should have received a copy of the GNU Affero General Public License
// along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.

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

#[test]
fn test_version() {
    let mut cmd = cargo_bin_cmd!("ordbokapi-worker");
    cmd.arg("--version");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}
