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

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow, bail};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use chrono::{DateTime, Duration, Utc};
use redis::aio::ConnectionManager;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, PgPool};
use tracing::{debug, info, warn};

use crate::jobs::QUEUE_NAMESPACES;

const STATE_SCOPE: &str = "_global";
const LAST_PUBLISHED_CURSOR_KEY: &str = "snapshot_last_published_cursor";
const SETTLED_CURSOR_KEY: &str = "snapshot_settled_cursor";
const SETTLED_SINCE_KEY: &str = "snapshot_settled_since";
const SNAPSHOT_PUBLISH_LOCK_KEY: i64 = 6_532_612_785_587_035_142;
pub const POLL_SCHEDULE: &str = "0 */5 * * * *";
const SNAPSHOT_EXCLUDE_TABLE_DATA: &[&str] = &["job_outbox"];

struct SnapshotPublishLock {
    conn: PgConnection,
}

impl SnapshotPublishLock {
    async fn try_acquire(database_url: &str) -> Result<Option<Self>> {
        let mut conn = PgConnection::connect(database_url).await?;
        let acquired: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
            .bind(SNAPSHOT_PUBLISH_LOCK_KEY)
            .fetch_one(&mut conn)
            .await?;

        if acquired.0 {
            Ok(Some(Self { conn }))
        } else {
            conn.close().await?;
            Ok(None)
        }
    }

    async fn release(mut self) -> Result<()> {
        let _: (bool,) = sqlx::query_as("SELECT pg_advisory_unlock($1)")
            .bind(SNAPSHOT_PUBLISH_LOCK_KEY)
            .fetch_one(&mut self.conn)
            .await?;
        self.conn.close().await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SnapshotConfig {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub prefix: String,
    pub retention: usize,
    pub settled_window: Duration,
    pub pg_dump_bin: String,
    pub database_url: String,
    pub force_path_style: bool,
}

#[derive(Clone)]
pub struct SettledSnapshotState {
    pub cursor: Option<DateTime<Utc>>,
    pub outbox_pending: i64,
    pub article_pending: i64,
    pub bibliography_pending: i64,
    pub place_pending: i64,
    pub redis_pending: i64,
    pub redis_scheduled: i64,
    pub active_jobs: usize,
    pub blocking_active_jobs: Vec<String>,
}

#[derive(Serialize)]
struct SnapshotManifest {
    manifest_version: u32,
    created_at: String,
    cursor: String,
    sha256: String,
    size_bytes: u64,
    worker_version: String,
}

impl SnapshotConfig {
    pub fn from_env(database_url: &str) -> Result<Option<Self>> {
        let bucket = std::env::var("SNAPSHOT_S3_BUCKET").ok();
        let access_key_id = std::env::var("SNAPSHOT_S3_ACCESS_KEY_ID").ok();
        let secret_access_key = std::env::var("SNAPSHOT_S3_SECRET_ACCESS_KEY").ok();

        if bucket.is_none() && access_key_id.is_none() && secret_access_key.is_none() {
            return Ok(None);
        }

        let bucket =
            bucket.context("SNAPSHOT_S3_BUCKET is required when snapshot publishing is enabled")?;
        let access_key_id = access_key_id
            .context("SNAPSHOT_S3_ACCESS_KEY_ID is required when snapshot publishing is enabled")?;
        let secret_access_key = secret_access_key.context(
            "SNAPSHOT_S3_SECRET_ACCESS_KEY is required when snapshot publishing is enabled",
        )?;

        let prefix = std::env::var("SNAPSHOT_S3_PREFIX").unwrap_or_default();
        let retention = std::env::var("SNAPSHOT_RETENTION")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(3)
            .max(1);
        let settled_window = Duration::seconds(
            std::env::var("SNAPSHOT_SETTLE_SECONDS")
                .ok()
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(120)
                .max(0),
        );

        Ok(Some(Self {
            bucket,
            endpoint: std::env::var("SNAPSHOT_S3_ENDPOINT").ok(),
            region: std::env::var("SNAPSHOT_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            access_key_id,
            secret_access_key,
            prefix,
            retention,
            settled_window,
            pg_dump_bin: std::env::var("PG_DUMP_BIN").unwrap_or_else(|_| "pg_dump".to_string()),
            database_url: database_url.to_string(),
            force_path_style: std::env::var("SNAPSHOT_S3_FORCE_PATH_STYLE")
                .ok()
                .is_none_or(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on")),
        }))
    }

    pub async fn maybe_publish(
        &self,
        db: &PgPool,
        redis_conn: &ConnectionManager,
        active_jobs: &[String],
    ) -> Result<()> {
        let Some(lock) = SnapshotPublishLock::try_acquire(&self.database_url).await? else {
            debug!(
                "Snapshot publish skipped: another worker instance currently holds the publish lock."
            );
            return Ok(());
        };

        let result = self.maybe_publish_locked(db, redis_conn, active_jobs).await;
        if let Err(e) = lock.release().await {
            warn!("Failed to release snapshot publish lock cleanly: {e:#}");
            if result.is_ok() {
                return Err(e);
            }
        }
        result
    }

    async fn maybe_publish_locked(
        &self,
        db: &PgPool,
        redis_conn: &ConnectionManager,
        active_jobs: &[String],
    ) -> Result<()> {
        let state = collect_state(db, redis_conn, active_jobs).await?;

        if !is_settled(&state) {
            clear_state(db, SETTLED_CURSOR_KEY).await?;
            clear_state(db, SETTLED_SINCE_KEY).await?;
            debug!(
                "Snapshot publish skipped: unsettled (outbox={}, article_pending={}, bibliography_pending={}, place_pending={}, redis_pending={}, redis_scheduled={}, active_jobs={}, blocking_active_jobs={:?}).",
                state.outbox_pending,
                state.article_pending,
                state.bibliography_pending,
                state.place_pending,
                state.redis_pending,
                state.redis_scheduled,
                state.active_jobs,
                state.blocking_active_jobs,
            );
            return Ok(());
        }

        let Some(cursor) = state.cursor else {
            debug!("Snapshot publish skipped: no content cursor available yet.");
            return Ok(());
        };

        let cursor_str = cursor.to_rfc3339();
        let last_published = load_state(db, LAST_PUBLISHED_CURSOR_KEY).await?;
        if last_published.as_deref() == Some(cursor_str.as_str()) {
            return Ok(());
        }

        let settled_cursor = load_state(db, SETTLED_CURSOR_KEY).await?;
        let settled_since = load_state(db, SETTLED_SINCE_KEY)
            .await?
            .as_deref()
            .and_then(parse_rfc3339_utc);

        if settled_cursor.as_deref() != Some(cursor_str.as_str()) {
            store_state(db, SETTLED_CURSOR_KEY, &cursor_str).await?;
            store_state(db, SETTLED_SINCE_KEY, &Utc::now().to_rfc3339()).await?;
            info!(
                "Snapshot publish: settled state observed at cursor {cursor_str}, waiting for stability window."
            );
            return Ok(());
        }

        let Some(settled_since) = settled_since else {
            store_state(db, SETTLED_SINCE_KEY, &Utc::now().to_rfc3339()).await?;
            return Ok(());
        };

        if Utc::now() - settled_since < self.settled_window {
            return Ok(());
        }

        self.publish(db, cursor).await?;
        store_state(db, LAST_PUBLISHED_CURSOR_KEY, &cursor_str).await?;
        store_state(db, SETTLED_SINCE_KEY, &Utc::now().to_rfc3339()).await?;
        Ok(())
    }

    async fn publish(&self, db: &PgPool, cursor: DateTime<Utc>) -> Result<()> {
        info!(
            "Publishing PostgreSQL snapshot for cursor {}…",
            cursor.to_rfc3339()
        );

        let temp_dir =
            tempfile::tempdir().context("Failed to create temporary snapshot directory")?;
        let dump_path = temp_dir.path().join("ordbokapi.postgres.dump");
        create_pg_dump(&self.pg_dump_bin, &self.database_url, &dump_path)?;

        let size_bytes = std::fs::metadata(&dump_path)
            .context("Failed to read dump metadata")?
            .len();
        let sha256 = sha256_hex(&dump_path)?;

        let snapshot_id = cursor.format("%Y-%m-%dT%H-%M-%SZ").to_string();
        let prefix = normalized_prefix(&self.prefix);
        let postgres_key = format!("{prefix}{snapshot_id}/postgres.dump");
        let manifest_key = format!("{prefix}{snapshot_id}/manifest.json");
        let latest_postgres_key = format!("{prefix}latest/postgres.dump");
        let latest_manifest_key = format!("{prefix}latest/manifest.json");

        let manifest = SnapshotManifest {
            manifest_version: 1,
            created_at: Utc::now().to_rfc3339(),
            cursor: cursor.to_rfc3339(),
            sha256,
            size_bytes,
            worker_version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;

        let client = self.s3_client().await;
        upload_file(&client, &self.bucket, &postgres_key, &dump_path).await?;
        upload_file(&client, &self.bucket, &latest_postgres_key, &dump_path).await?;
        upload_bytes(
            &client,
            &self.bucket,
            &manifest_key,
            manifest_bytes.clone(),
            "application/json",
        )
        .await?;
        upload_bytes(
            &client,
            &self.bucket,
            &latest_manifest_key,
            manifest_bytes,
            "application/json",
        )
        .await?;
        prune_old_snapshots(&client, &self.bucket, &prefix, self.retention).await?;

        let _ = db;
        info!(
            "Published PostgreSQL snapshot to s3://{}/{}.",
            self.bucket, postgres_key
        );
        Ok(())
    }

    async fn s3_client(&self) -> S3Client {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(self.region.clone()))
            .credentials_provider(Credentials::new(
                self.access_key_id.clone(),
                self.secret_access_key.clone(),
                None,
                None,
                "ordbokapi-worker",
            ))
            .load()
            .await;

        let mut builder = aws_sdk_s3::config::Builder::from(&shared)
            .region(Region::new(self.region.clone()))
            .force_path_style(self.force_path_style);

        if let Some(endpoint) = &self.endpoint {
            builder = builder.endpoint_url(endpoint);
        }

        S3Client::from_conf(builder.build())
    }
}

fn parse_rfc3339_utc(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

async fn collect_state(
    db: &PgPool,
    redis_conn: &ConnectionManager,
    active_jobs: &[String],
) -> Result<SettledSnapshotState> {
    let (outbox_pending, article_pending, bibliography_pending, place_pending): (i64, i64, i64, i64) =
        sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM job_outbox WHERE processed_at IS NULL),
                (SELECT COUNT(*) FROM articles WHERE sync_status IN ('pending_fetch', 'pending_index')),
                (SELECT COUNT(*) FROM bibliography WHERE sync_status = 'pending_fetch'),
                (SELECT COUNT(*) FROM places WHERE sync_status = 'pending_fetch')",
        )
        .fetch_one(db)
        .await?;

    let article_cursor: Option<(DateTime<Utc>,)> =
        sqlx::query_as("SELECT MAX(modified_at) FROM articles")
            .fetch_optional(db)
            .await?;
    let metadata_cursor: Option<(DateTime<Utc>,)> =
        sqlx::query_as("SELECT MAX(modified_at) FROM dictionary_metadata")
            .fetch_optional(db)
            .await?;
    let bibliography_cursor: Option<(DateTime<Utc>,)> =
        sqlx::query_as("SELECT MAX(fetched_at) FROM bibliography")
            .fetch_optional(db)
            .await?;
    let place_cursor: Option<(DateTime<Utc>,)> =
        sqlx::query_as("SELECT MAX(fetched_at) FROM places")
            .fetch_optional(db)
            .await?;

    let cursor = [
        article_cursor,
        metadata_cursor,
        bibliography_cursor,
        place_cursor,
    ]
    .into_iter()
    .flatten()
    .map(|(ts,)| ts)
    .max();

    let (redis_pending, redis_scheduled) = redis_counts(redis_conn).await?;

    let blocking_active_jobs = active_jobs
        .iter()
        .filter(|job| is_blocking_active_job(job))
        .cloned()
        .collect::<Vec<_>>();
    let active_jobs = blocking_active_jobs.len();

    Ok(SettledSnapshotState {
        cursor,
        outbox_pending,
        article_pending,
        bibliography_pending,
        place_pending,
        redis_pending,
        redis_scheduled,
        active_jobs,
        blocking_active_jobs,
    })
}

fn is_blocking_active_job(job: &str) -> bool {
    if job.starts_with("Publish snapshot") {
        return false;
    }

    if job.starts_with("Sweep stuck items") || job.starts_with("Sending Matrix message") {
        return false;
    }

    true
}

const fn is_settled(state: &SettledSnapshotState) -> bool {
    state.outbox_pending == 0
        && state.article_pending == 0
        && state.bibliography_pending == 0
        && state.place_pending == 0
        && state.redis_pending == 0
        && state.redis_scheduled == 0
        && state.active_jobs == 0
}

async fn redis_counts(redis_conn: &ConnectionManager) -> Result<(i64, i64)> {
    let mut conn = redis_conn.clone();
    let mut pipe = redis::pipe();
    for namespace in QUEUE_NAMESPACES {
        pipe.cmd("LLEN").arg(format!("{namespace}:active"));
        pipe.cmd("ZCARD").arg(format!("{namespace}:scheduled"));
    }

    let counts: Vec<i64> = pipe.query_async(&mut conn).await?;
    let mut pending = 0;
    let mut scheduled = 0;
    for chunk in counts.chunks(2) {
        if let Some(value) = chunk.first() {
            pending += *value;
        }
        if let Some(value) = chunk.get(1) {
            scheduled += *value;
        }
    }
    Ok((pending, scheduled))
}

fn create_pg_dump(pg_dump_bin: &str, database_url: &str, dump_path: &Path) -> Result<()> {
    let mut cmd = Command::new(pg_dump_bin);
    cmd.arg("--format=custom")
        .arg("--compress=9")
        .arg("--no-owner")
        .arg("--no-privileges")
        .arg("--file")
        .arg(dump_path);

    for table in SNAPSHOT_EXCLUDE_TABLE_DATA {
        cmd.arg("--exclude-table-data").arg(table);
    }

    let status = cmd
        .arg(database_url)
        .status()
        .with_context(|| format!("Failed to execute {pg_dump_bin}"))?;

    if status.success() {
        Ok(())
    } else {
        bail!("pg_dump exited with status {status}");
    }
}

fn sha256_hex(path: &Path) -> Result<String> {
    let mut file = File::open(path).context("Failed to open snapshot dump for hashing")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8192];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(base16ct::lower::encode_string(&hasher.finalize()))
}

async fn upload_file(client: &S3Client, bucket: &str, key: &str, path: &Path) -> Result<()> {
    let body = ByteStream::from_path(PathBuf::from(path)).await?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .content_type("application/octet-stream")
        .send()
        .await?;
    Ok(())
}

async fn upload_bytes(
    client: &S3Client,
    bucket: &str,
    key: &str,
    bytes: Vec<u8>,
    content_type: &str,
) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(bytes))
        .content_type(content_type)
        .send()
        .await?;
    Ok(())
}

async fn prune_old_snapshots(
    client: &S3Client,
    bucket: &str,
    prefix: &str,
    retention: usize,
) -> Result<()> {
    let snapshot_prefix = normalized_prefix(prefix);
    let response = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&snapshot_prefix)
        .send()
        .await?;

    let mut snapshot_ids = response
        .contents()
        .iter()
        .filter_map(|obj| obj.key())
        .filter_map(|key| key.strip_prefix(&snapshot_prefix))
        .filter_map(|key| key.split('/').next())
        .filter(|snapshot_id| *snapshot_id != "latest")
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();

    snapshot_ids.sort();
    snapshot_ids.dedup();

    if snapshot_ids.len() <= retention {
        return Ok(());
    }

    let to_delete = snapshot_ids[..snapshot_ids.len() - retention].to_vec();
    for snapshot_id in to_delete {
        let delete_prefix = format!("{snapshot_prefix}{snapshot_id}/");
        let response = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&delete_prefix)
            .send()
            .await?;

        let objects = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .map(|key| {
                ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .map_err(|e| anyhow!("Failed to build object identifier: {e}"))
            })
            .collect::<Result<Vec<_>>>()?;

        if objects.is_empty() {
            continue;
        }

        let delete = Delete::builder()
            .set_objects(Some(objects))
            .build()
            .map_err(|e| anyhow!("Failed to build delete request: {e}"))?;

        client
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;

        warn!("Pruned old snapshot {snapshot_id} from bucket {}.", bucket);
    }

    Ok(())
}

fn normalized_prefix(prefix: &str) -> String {
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", prefix.trim_matches('/'))
    }
}

async fn load_state(db: &PgPool, key: &str) -> Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT value FROM sync_state WHERE dictionary = $1 AND key = $2")
            .bind(STATE_SCOPE)
            .bind(key)
            .fetch_optional(db)
            .await?;

    Ok(row.map(|(value,)| value))
}

async fn store_state(db: &PgPool, key: &str, value: &str) -> Result<()> {
    sqlx::query(
        "INSERT INTO sync_state (dictionary, key, value)
         VALUES ($1, $2, $3)
         ON CONFLICT (dictionary, key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(STATE_SCOPE)
    .bind(key)
    .bind(value)
    .execute(db)
    .await?;

    Ok(())
}

async fn clear_state(db: &PgPool, key: &str) -> Result<()> {
    sqlx::query("DELETE FROM sync_state WHERE dictionary = $1 AND key = $2")
        .bind(STATE_SCOPE)
        .bind(key)
        .execute(db)
        .await?;

    Ok(())
}
