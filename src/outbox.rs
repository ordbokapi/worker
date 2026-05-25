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

use anyhow::Result;
use apalis::prelude::*;
use apalis_redis::RedisStorage;
use serde_json::Value;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::jobs::{
    BatchIndexJob, FetchArticleJob, FetchBibliographyJob, FetchPlaceJob, IndexArticleJob,
    ResolveInlineCodeJob,
};

/// Redis storages needed for the outbox poller to dispatch jobs.
pub struct OutboxStorages {
    pub fetch_article: RedisStorage<FetchArticleJob>,
    pub index_article: RedisStorage<IndexArticleJob>,
    pub batch_index: RedisStorage<BatchIndexJob>,
    pub fetch_bibliography: RedisStorage<FetchBibliographyJob>,
    pub fetch_place: RedisStorage<FetchPlaceJob>,
    pub resolve_inline_code: RedisStorage<ResolveInlineCodeJob>,
}

/// Polls the job_outbox table and pushes jobs to Redis queues.
pub async fn run_outbox_poller(db: PgPool, storages: OutboxStorages) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));

    loop {
        interval.tick().await;

        if let Err(e) = poll_once(&db, &storages).await {
            error!("Outbox poll error: {e:#}");
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

async fn poll_once(db: &PgPool, storages: &OutboxStorages) -> Result<()> {
    let entries: Vec<(i64, String, Value)> = sqlx::query_as(
        "SELECT id, job_type, payload FROM job_outbox
         WHERE processed_at IS NULL
         ORDER BY id
         LIMIT 200
         FOR UPDATE SKIP LOCKED",
    )
    .fetch_all(db)
    .await?;

    if entries.is_empty() {
        return Ok(());
    }

    let mut processed_ids: Vec<i64> = Vec::with_capacity(entries.len());
    let mut coalesced_batch_keys: Vec<String> = Vec::new();

    for (id, job_type, payload) in &entries {
        if job_type == "batch_index" {
            if let Some(arr) = payload["article_keys"].as_array() {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        coalesced_batch_keys.push(s.to_string());
                    }
                }
            }
            processed_ids.push(*id);
            continue;
        }

        let result = dispatch_job(db, job_type, payload, storages).await;

        match result {
            Ok(()) => processed_ids.push(*id),
            Err(e) => {
                warn!("Failed to dispatch outbox entry {id} of type {job_type}: {e:#}");
            }
        }
    }

    if !coalesced_batch_keys.is_empty() {
        coalesced_batch_keys.sort_unstable();
        coalesced_batch_keys.dedup();
        info!(
            "Outbox: coalesced batch_index with {} unique article keys.",
            coalesced_batch_keys.len()
        );
        let mut storage = storages.batch_index.clone();
        if let Err(e) = storage
            .push(BatchIndexJob {
                article_keys: coalesced_batch_keys,
            })
            .await
        {
            error!("Failed to push coalesced batch_index: {e}");
        }
    }

    if !processed_ids.is_empty() {
        sqlx::query("UPDATE job_outbox SET processed_at = now() WHERE id = ANY($1)")
            .bind(&processed_ids)
            .execute(db)
            .await?;
        debug!("Outbox: dispatched {} jobs.", processed_ids.len());
    }

    Ok(())
}

/// Dispatch a single job to the appropriate Redis queue based on its type.
async fn dispatch_job(
    db: &PgPool,
    job_type: &str,
    payload: &Value,
    storages: &OutboxStorages,
) -> Result<()> {
    match job_type {
        "fetch_article" => {
            let dictionary = payload["dictionary"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let article_id = payload["article_id"].as_i64().unwrap_or_default();
            if !crate::storage::ensure_article_pending_fetch(db, &dictionary, article_id).await? {
                return Ok(());
            }
            let mut storage = storages.fetch_article.clone();
            storage
                .push(FetchArticleJob {
                    dictionary,
                    article_id,
                })
                .await
                .map_err(|e| anyhow::anyhow!("Redis push failed: {e}"))?;
        }
        "index_article" => {
            let dictionary = payload["dictionary"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let article_id = payload["article_id"].as_i64().unwrap_or_default();
            let mut storage = storages.index_article.clone();
            storage
                .push(IndexArticleJob {
                    dictionary,
                    article_id,
                })
                .await
                .map_err(|e| anyhow::anyhow!("Redis push failed: {e}"))?;
        }
        "batch_index" => {
            let keys: Vec<String> = payload["article_keys"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
                        .collect()
                })
                .unwrap_or_default();
            let mut storage = storages.batch_index.clone();
            storage
                .push(BatchIndexJob { article_keys: keys })
                .await
                .map_err(|e| anyhow::anyhow!("Redis push failed: {e}"))?;
        }
        "fetch_bibliography" => {
            let bibl_id = payload["bibl_id"].as_i64().unwrap_or_default();
            if !crate::storage::ensure_bibl_pending_fetch(db, bibl_id).await? {
                return Ok(());
            }
            let mut storage = storages.fetch_bibliography.clone();
            storage
                .push(FetchBibliographyJob { bibl_id })
                .await
                .map_err(|e| anyhow::anyhow!("Redis push failed: {e}"))?;
        }
        "fetch_place" => {
            let place_id = payload["place_id"].as_i64().unwrap_or_default();
            if !crate::storage::ensure_place_pending_fetch(db, place_id).await? {
                return Ok(());
            }
            let mut storage = storages.fetch_place.clone();
            storage
                .push(FetchPlaceJob { place_id })
                .await
                .map_err(|e| anyhow::anyhow!("Redis push failed: {e}"))?;
        }
        "resolve_inline_code" => {
            let code = payload["code"].as_str().unwrap_or_default().to_string();
            let mut storage = storages.resolve_inline_code.clone();
            storage
                .push(ResolveInlineCodeJob { code })
                .await
                .map_err(|e| anyhow::anyhow!("Redis push failed: {e}"))?;
        }
        other => {
            warn!("Unknown outbox job_type: {other}");
        }
    }
    Ok(())
}

/// Periodically sweep and retry stuck items.
#[allow(clippy::too_many_lines)]
pub async fn run_sweep(db: &PgPool) -> Result<()> {
    let stale_threshold = chrono::Utc::now() - chrono::Duration::minutes(5);

    // If stuff's been changed recently, the queue is still active, so don't
    // sweep yet. Wait until nothing's really happening to sweep.
    let recent_activity: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM articles
         WHERE sync_status IN ('pending_fetch', 'pending_index')
           AND status_changed_at > now() - INTERVAL '5 minutes'",
    )
    .fetch_one(db)
    .await?;

    if recent_activity.0 > 50 {
        debug!(
            "{} changes in last 5 minutes, skipping sweep.",
            recent_activity.0
        );

        sqlx::query("DELETE FROM job_outbox WHERE processed_at < now() - INTERVAL '1 hour'")
            .execute(db)
            .await?;

        return Ok(());
    }

    let stuck_fetch: Vec<(String, i64)> = sqlx::query_as(
        "SELECT dictionary, id FROM articles
         WHERE sync_status = 'pending_fetch' AND status_changed_at < $1
         LIMIT 500",
    )
    .bind(stale_threshold)
    .fetch_all(db)
    .await?;

    let stuck_index: Vec<(String, i64)> = sqlx::query_as(
        "SELECT dictionary, id FROM articles
         WHERE sync_status = 'pending_index' AND status_changed_at < $1
         LIMIT 500",
    )
    .bind(stale_threshold)
    .fetch_all(db)
    .await?;

    let stuck_bibl: Vec<(i64,)> = sqlx::query_as(
        "SELECT id FROM bibliography
         WHERE sync_status = 'pending_fetch' AND status_changed_at < $1
         LIMIT 200",
    )
    .bind(stale_threshold)
    .fetch_all(db)
    .await?;

    let stuck_places: Vec<(i64,)> = sqlx::query_as(
        "SELECT id FROM places
         WHERE sync_status = 'pending_fetch' AND status_changed_at < $1
         LIMIT 200",
    )
    .bind(stale_threshold)
    .fetch_all(db)
    .await?;

    let total = stuck_fetch.len() + stuck_index.len() + stuck_bibl.len() + stuck_places.len();
    if total == 0 {
        return Ok(());
    }

    info!(
        "Sweep: found {} stuck items (fetch:{}, index:{}, bibl:{}, places:{})",
        total,
        stuck_fetch.len(),
        stuck_index.len(),
        stuck_bibl.len(),
        stuck_places.len()
    );

    let mut tx = db.begin().await?;

    // Mark status_changed_at so sweep isn't repeated immediately.
    if !stuck_fetch.is_empty() {
        let dicts: Vec<&str> = stuck_fetch.iter().map(|(d, _)| d.as_str()).collect();
        let ids: Vec<i64> = stuck_fetch.iter().map(|(_, id)| *id).collect();
        sqlx::query(
            "UPDATE articles SET status_changed_at = now()
             WHERE (dictionary, id) IN (SELECT * FROM UNNEST($1::text[], $2::bigint[]))",
        )
        .bind(&dicts)
        .bind(&ids)
        .execute(&mut *tx)
        .await?;

        for (dict, article_id) in &stuck_fetch {
            crate::storage::write_outbox_fetch_article(&mut tx, dict, *article_id).await?;
        }
    }

    if !stuck_index.is_empty() {
        let dicts: Vec<&str> = stuck_index.iter().map(|(d, _)| d.as_str()).collect();
        let ids: Vec<i64> = stuck_index.iter().map(|(_, id)| *id).collect();
        sqlx::query(
            "UPDATE articles SET status_changed_at = now()
             WHERE (dictionary, id) IN (SELECT * FROM UNNEST($1::text[], $2::bigint[]))",
        )
        .bind(&dicts)
        .bind(&ids)
        .execute(&mut *tx)
        .await?;

        for (dict, article_id) in &stuck_index {
            crate::storage::write_outbox_index_article(&mut tx, dict, *article_id).await?;
        }
    }

    if !stuck_bibl.is_empty() {
        let ids: Vec<i64> = stuck_bibl.iter().map(|(id,)| *id).collect();
        sqlx::query("UPDATE bibliography SET status_changed_at = now() WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        for (bibl_id,) in &stuck_bibl {
            crate::storage::write_outbox_fetch_bibl(&mut tx, *bibl_id).await?;
        }
    }

    if !stuck_places.is_empty() {
        let ids: Vec<i64> = stuck_places.iter().map(|(id,)| *id).collect();
        sqlx::query("UPDATE places SET status_changed_at = now() WHERE id = ANY($1)")
            .bind(&ids)
            .execute(&mut *tx)
            .await?;

        for (place_id,) in &stuck_places {
            crate::storage::write_outbox_fetch_place(&mut tx, *place_id).await?;
        }
    }

    tx.commit().await?;

    sqlx::query("DELETE FROM job_outbox WHERE processed_at < now() - INTERVAL '1 hour'")
        .execute(db)
        .await?;

    Ok(())
}
