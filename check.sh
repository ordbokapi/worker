#!/bin/sh

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
run cargo clippy $features_arg -- -D clippy::suspicious -D clippy::style -D clippy::complexity -D clippy::perf -D clippy::dbg_macro -D clippy::todo -D clippy::unimplemented -D warnings
run cargo fmt -- --check
