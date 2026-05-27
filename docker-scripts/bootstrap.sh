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

set -eu

bootstrap_sentinel="/bootstrap/.ordbokapi-bootstrap-complete"

is_truthy() {
  case "$(echo "${1:-}" | tr '[:upper:]' '[:lower:]')" in
  1 | true | yes | on)
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

force_resync="${BOOTSTRAP_FORCE_RESYNC:-}"

if is_truthy "$force_resync"; then
  echo "Forced resync requested."
  rm -f "$bootstrap_sentinel"
fi

if [ -f "$bootstrap_sentinel" ]; then
  echo "Bootstrap already completed, skipping restore/reindex."
  exit 0
fi

export PGPASSWORD="${POSTGRES_PASSWORD:-password}"

until pg_isready -h db -U ordbokapi -d ordbokapi >/dev/null 2>&1; do
  sleep 1
done

until curl -fsS "${MEILI_URL:-http://meilisearch:7700}/health" >/dev/null 2>&1; do
  sleep 1
done

table_count() {
  psql --host=db --username=ordbokapi --dbname=ordbokapi --tuples-only --no-align -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
}

human_bytes() {
  value="$1"

  if command -v numfmt >/dev/null 2>&1; then
    numfmt --to=iec-i --suffix=B "$value"
  else
    printf '%s bytes' "$value"
  fi
}

download_with_progress() {
  url="$1"
  output_path="$2"
  url_no_query="${url%%\?*}"
  file_name="${url_no_query##*/}"

  if [ -z "$file_name" ]; then
    file_name="$(basename "$output_path")"
  fi

  tmp_headers="$(mktemp)"
  total_bytes=0

  if curl -fsSLI "$url" -o "$tmp_headers"; then
    total_bytes="$(awk 'BEGIN{IGNORECASE=1} /^Content-Length:/ {print $2}' "$tmp_headers" | tr -d '\r' | tail -n1)"
    case "$total_bytes" in
    '' | *[!0-9]*) total_bytes=0 ;;
    esac
  fi

  rm -f "$tmp_headers"

  download_tmp="${output_path}.part"

  rm -f "$output_path" "$download_tmp"
  curl -fsSL "$url" -o "$download_tmp" &

  dl_pid="$!"
  max_downloaded=0
  printed_progress=0

  while kill -0 "$dl_pid" 2>/dev/null; do
    downloaded=0

    if [ -f "$download_tmp" ]; then
      downloaded="$(wc -c <"$download_tmp" | tr -d '[:space:]')"
      case "$downloaded" in
      '' | *[!0-9]*) downloaded=0 ;;
      esac
    fi

    if [ "$downloaded" -lt "$max_downloaded" ]; then
      downloaded="$max_downloaded"
    else
      max_downloaded="$downloaded"
    fi

    if [ "$downloaded" -gt 0 ]; then
      printed_progress=1

      if [ "$total_bytes" -gt 0 ]; then
        pct=$((downloaded * 100 / total_bytes))
        downloaded_human="$(human_bytes "$downloaded")"
        total_human="$(human_bytes "$total_bytes")"

        echo "  ${file_name}: ${downloaded_human}/${total_human} (${pct}%)"
      else
        downloaded_human="$(human_bytes "$downloaded")"

        echo "  ${file_name}: ${downloaded_human}"
      fi
    fi

    sleep 2
  done

  if ! wait "$dl_pid"; then
    rm -f "$download_tmp"
    return 1
  fi

  final_bytes="$(wc -c <"$download_tmp" | tr -d '[:space:]')"

  case "$final_bytes" in
  '' | *[!0-9]*) final_bytes=0 ;;
  esac

  final_human="$(human_bytes "$final_bytes")"

  if [ "$total_bytes" -gt 0 ]; then
    total_human="$(human_bytes "$total_bytes")"

    echo "  ${file_name}: complete (${final_human}/${total_human})"
  else
    echo "  ${file_name}: complete (${final_human})"
  fi

  mv "$download_tmp" "$output_path"
}

restore_snapshot() {
  manifest_url="$1"

  echo "Downloading snapshot manifest…"

  if ! download_with_progress "$manifest_url" /tmp/ordbokapi.manifest.json; then
    echo "Failed to download snapshot manifest from $manifest_url"
    echo "Check that SNAPSHOT_PUBLIC_MANIFEST_URL points to a valid manifest and is publicly readable."
    exit 1
  fi

  manifest_version="$(jq -r '.manifest_version // empty' /tmp/ordbokapi.manifest.json)"
  expected_sha256="$(jq -r '.sha256 // empty' /tmp/ordbokapi.manifest.json)"

  if [ "$manifest_version" != "1" ]; then
    echo "Manifest version $manifest_version not supported. Expected version 1."
    exit 1
  fi

  if [ -z "$expected_sha256" ]; then
    echo "Manifest does not include sha256 sum, skipping snapshot restore."
    return
  fi

  case "$manifest_url" in
  */manifest.json) ;;
  *)
    echo "SNAPSHOT_PUBLIC_MANIFEST_URL must end with /manifest.json"
    exit 1
    ;;
  esac

  manifest_dir="${manifest_url%/*}"
  snapshot_url="${manifest_dir}/postgres.dump"

  echo "Downloading PostgreSQL snapshot from manifest key…"

  if ! download_with_progress "$snapshot_url" /tmp/ordbokapi.postgres.dump; then
    echo "Failed to download PostgreSQL snapshot from $snapshot_url"
    echo "Check that postgres.dump exists next to the manifest and is publicly readable."
    exit 1
  fi

  echo "Verifying snapshot checksum…"

  actual_sha256="$(sha256sum /tmp/ordbokapi.postgres.dump | awk '{print $1}')"

  if [ "$actual_sha256" != "$expected_sha256" ]; then
    echo "Snapshot checksum mismatch: expected $expected_sha256 got $actual_sha256"
    exit 1
  fi

  echo "Restoring PostgreSQL snapshot…"
  restore_started_at="$(date +%s)"

  if ! pg_restore \
    --verbose \
    --clean \
    --if-exists \
    --no-owner \
    --no-privileges \
    --host=db \
    --username=ordbokapi \
    --dbname=ordbokapi \
    /tmp/ordbokapi.postgres.dump; then
    echo "PostgreSQL restore failed."
    exit 1
  fi
}

current_table_count="$(table_count)"

if [ -n "${SNAPSHOT_PUBLIC_MANIFEST_URL:-}" ]; then
  if [ "${current_table_count:-0}" -eq 0 ] || is_truthy "$force_resync"; then
    restore_snapshot "$SNAPSHOT_PUBLIC_MANIFEST_URL"

    current_table_count="$(table_count)"
  else
    echo "PostgreSQL already has user tables, skipping snapshot restore."
  fi
else
  echo "SNAPSHOT_PUBLIC_MANIFEST_URL is not set, skipping PostgreSQL snapshot restore."
fi

if [ "${current_table_count:-0}" -eq 0 ]; then
  echo "PostgreSQL has no user tables yet, skipping Meilisearch bootstrap."
  exit 0
fi

echo "Rebuilding Meilisearch indexes from PostgreSQL…"
ordbokapi-worker reindex

touch "$bootstrap_sentinel"
echo "Bootstrap complete."
