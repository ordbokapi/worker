#!/bin/sh

# SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Ordbok API.
#
# Ordbok API is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.

features_arg="--all-features"

# Show help message
for arg in "$@"; do
  case "$arg" in
    -\?|-h|--help|help)
      echo "Usage: $0 [feature [feature …]]"
      echo
      echo "Runs clippy and fmt checks on the project."
      echo
      echo "  [feature]   Optional. Feature to enable for the build."
      echo "              If not provided, all features are enabled."
      echo "              \"none\" can be provided to build without any features."
      echo "  -h, --help  Show this help message."
      exit 0
      ;;
    none)
      features_arg=""
      shift
      ;;
  esac
done

# Accept multiple optional features from cmd line, otherwise build with all features.
if [ "$features_arg" != "" ] && [ "$#" -gt 0 ]; then
  IFS=,
  features_arg="--features $*"
  IFS=' '
fi

# Only use colors if connected directly to a terminal
if [ -t 1 ]; then
  dim=$(tput dim)
  reset=$(tput sgr0)
else
  dim=""
  reset=""
fi

run() {
  echo >&2
  echo "$dim> $*$reset" >&2
  echo >&2
  "$@"
}

# shellcheck disable=SC2086
run cargo clippy $features_arg -- -D clippy::all -D clippy::pedantic -D clippy::nursery -A clippy::missing_errors_doc -A clippy::missing_panics_doc -A clippy::doc_markdown -A clippy::module_name_repetitions -D warnings
run cargo fmt -- --check
