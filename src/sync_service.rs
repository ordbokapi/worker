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

use anyhow::{Result, anyhow};
use apalis::prelude::*;
use apalis_core::backend::Vacuum;
use apalis_redis::{ConnectionManager, RedisConfig, RedisStorage};
use futures::StreamExt;
use meilisearch_sdk::client::Client as MeiliClient;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::extraction::{self};
use crate::indexing;
#[cfg(feature = "matrix_notifs")]
use crate::jobs::SendMatrixMessageJob;
use crate::jobs::{
    BackfillInlineRefsJob, BatchIndexJob, FetchArticleJob, FetchArticleListJob,
    FetchBibliographyJob, FetchDictionaryMetadataJob, FetchPlaceJob, ResolveInlineCodeJob,
};
use crate::state::{SyncStatus, UibDictionary};
use crate::storage;
use crate::uib_client::UibClient;

/// Main service struct.
#[derive(Clone)]
pub struct SyncService {
    pub db: PgPool,
    pub meili: MeiliClient,
    pub http: UibClient,
    pub redis_conn: ConnectionManager,
    pub inline_refs_backfill_count: Arc<AtomicU64>,
    #[cfg(feature = "matrix_notifs")]
    pub matrix_message_storage: RedisStorage<SendMatrixMessageJob>,
}

impl SyncService {
    /// Queue a Matrix notification message for delivery.
    #[cfg(feature = "matrix_notifs")]
    pub async fn queue_matrix_message(&self, msg: &str) {
        let env_name = std::env::var("ENVIRONMENT").unwrap_or_default();
        let prefixed = if env_name.is_empty() {
            msg.to_string()
        } else {
            format!("[{env_name}] {msg}")
        };
        let mut storage = self.matrix_message_storage.clone();
        if let Err(e) = storage
            .push(SendMatrixMessageJob { message: prefixed })
            .await
        {
            warn!("Failed to queue Matrix message: {e}");
        }
    }

    /// Handle the article list fetch.
    pub async fn handle_fetch_article_list(&self, job: FetchArticleListJob) -> Result<()> {
        let dict = UibDictionary::parse(&job.dictionary)
            .ok_or_else(|| anyhow!("Unknown dictionary: {}", job.dictionary))?;

        if job.force {
            info!("[{dict}] Full resync requested.");
        }

        info!("[{dict}] Fetching article list from UiB API…");
        let article_list = self.http.fetch_article_list(dict).await?;
        info!(
            "[{dict}] {} articles returned from UiB.",
            article_list.len()
        );

        let existing_metadata_map = storage::get_all_article_metadata(&self.db, dict).await?;
        info!(
            "[{dict}] {} existing articles in DB.",
            existing_metadata_map.len()
        );

        let mut to_fetch: Vec<(i64, Option<i64>, String)> = Vec::new();
        let mut new_id_set = HashSet::new();
        #[cfg(feature = "matrix_notifs")]
        let mut new_count = 0u32;
        #[cfg(feature = "matrix_notifs")]
        let mut updated_count = 0u32;

        for raw_meta in &article_list {
            let Some(meta) = extraction::parse_article_list_entry(raw_meta) else {
                continue;
            };
            new_id_set.insert(meta.article_id);

            if let Some(existing) = existing_metadata_map.get(&meta.article_id) {
                if !job.force {
                    if existing.sync_status != SyncStatus::Idle {
                        continue;
                    }

                    if existing.revision == meta.revision && existing.updated_at == meta.updated_at
                    {
                        continue;
                    }
                }

                debug!(
                    "[{dict}] Article {} ({}) changed.",
                    meta.article_id, meta.primary_lemma
                );

                #[cfg(feature = "matrix_notifs")]
                {
                    updated_count += 1;
                }
            } else {
                debug!(
                    "[{dict}] New article {} ({}).",
                    meta.article_id, meta.primary_lemma
                );

                #[cfg(feature = "matrix_notifs")]
                {
                    new_count += 1;
                }
            }

            to_fetch.push((meta.article_id, meta.revision, meta.updated_at));
        }

        let mut missing_count = 0u32;
        for (existing_id, ex_meta) in &existing_metadata_map {
            if !new_id_set.contains(existing_id) && ex_meta.sync_status == SyncStatus::Idle {
                debug!(
                    "[{dict}] Article {} ({}) no longer in UiB list.",
                    existing_id, ex_meta.primary_lemma
                );
                to_fetch.push((*existing_id, None, String::new()));
                missing_count += 1;
            }
        }

        if missing_count > 0 {
            warn!(
                "[{dict}] {} articles no longer in UiB list, will re-fetch to check.",
                missing_count
            );
        }

        let mut tx = self.db.begin().await?;

        for (article_id, revision, updated_at) in &to_fetch {
            storage::write_outbox_fetch_article_with_meta(
                &mut tx,
                dict.as_str(),
                *article_id,
                *revision,
                updated_at,
            )
            .await?;
        }

        tx.commit().await?;

        info!("[{dict}] Enqueued {} article fetch jobs.", to_fetch.len(),);

        #[cfg(feature = "matrix_notifs")]
        {
            let msg = format!(
                "[{dict}] **Synkroniserer artiklar med UiB.**\n**Nye:** {new_count}\n**Oppdaterte:** {updated_count}\n**Manglar i UiB-lista:** {missing_count}"
            );
            self.queue_matrix_message(&msg).await;
        }

        Ok(())
    }

    /// Handle fetching a single article from UiB.
    pub async fn handle_fetch_article(&self, job: FetchArticleJob) -> Result<()> {
        let dict = UibDictionary::parse(&job.dictionary)
            .ok_or_else(|| anyhow!("Unknown dictionary: {}", job.dictionary))?;

        let status: Option<(String,)> =
            sqlx::query_as("SELECT sync_status FROM articles WHERE dictionary = $1 AND id = $2")
                .bind(dict.as_str())
                .bind(job.article_id)
                .fetch_optional(&self.db)
                .await?;

        match status {
            Some((s,)) if s == "pending_fetch" => {}
            Some((s,)) => {
                debug!(
                    "[{dict}] Article {} not in pending_fetch with status {s}, skipping.",
                    job.article_id
                );
                return Ok(());
            }
            None => {
                debug!(
                    "[{dict}] Article {} not found, creating placeholder.",
                    job.article_id
                );
            }
        }

        debug!("[{dict}] Fetching article {} from UiB…", job.article_id);
        let article_data = match self.http.fetch_article(dict, job.article_id).await {
            Ok(data) => data,
            Err(e) if is_not_found(&e) => {
                warn!("[{dict}] Article {} not found upstream.", job.article_id);
                storage::reset_article_to_idle(&self.db, dict, job.article_id).await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let analysis = extraction::analyze_article(dict, &article_data);

        let revision = job.revision.unwrap_or(0);
        let updated_at = if job.updated_at.is_empty() {
            article_data
                .get("updated")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        } else {
            job.updated_at.clone()
        };

        let result = storage::store_article(
            &self.db,
            dict,
            job.article_id,
            &article_data,
            &analysis,
            revision,
            &updated_at,
        )
        .await?;

        debug!(
            "[{dict}] Article {} ({}) stored. Pending index: bibl:{}, places:{}, related:{}",
            job.article_id,
            analysis.primary_lemma,
            result.bibl_fetched,
            result.places_fetched,
            result.related_fetched,
        );

        Ok(())
    }

    /// Handle batch indexing.
    pub async fn handle_batch_index(&self, job: BatchIndexJob) -> Result<()> {
        let article_keys: Vec<(String, i64)> = job
            .article_keys
            .iter()
            .filter_map(|k| {
                let (dict, id_str) = k.split_once(':')?;
                let id = id_str.parse::<i64>().ok()?;
                Some((dict.to_string(), id))
            })
            .collect();

        if article_keys.is_empty() {
            return Ok(());
        }

        debug!("Batch indexing {} articles…", article_keys.len());
        indexing::batch_index_articles(&self.meili, &self.db, &article_keys).await?;
        debug!("Batch index complete.");
        Ok(())
    }

    /// Handle fetching dictionary metadata.
    pub async fn handle_fetch_dict_metadata(&self, job: FetchDictionaryMetadataJob) -> Result<()> {
        let dict = UibDictionary::parse(&job.dictionary)
            .ok_or_else(|| anyhow!("Unknown dictionary: {}", job.dictionary))?;

        info!("[{dict}] Fetching dictionary metadata from UiB…");

        let concepts = self.http.fetch_concepts(dict).await?;
        let word_classes = self.http.fetch_word_classes(dict).await?;
        let word_subclasses = self.http.fetch_word_subclasses(dict).await?;

        let dict_str = dict.as_str();

        for (key, data) in [
            ("concepts", &concepts),
            ("word_classes", &word_classes),
            ("word_subclasses", &word_subclasses),
        ] {
            sqlx::query(
                "INSERT INTO dictionary_metadata (dictionary, key, data, modified_at)
                 VALUES ($1, $2, $3, now())
                 ON CONFLICT (dictionary, key) DO UPDATE SET data = $3, modified_at = now()",
            )
            .bind(dict_str)
            .bind(key)
            .bind(data)
            .execute(&self.db)
            .await?;
        }

        info!("[{dict}] Dictionary metadata stored.");
        Ok(())
    }

    /// Handle fetching a bibliography entry.
    pub async fn handle_fetch_bibliography(&self, job: FetchBibliographyJob) -> Result<()> {
        let status: Option<(String,)> =
            sqlx::query_as("SELECT sync_status FROM bibliography WHERE id = $1")
                .bind(job.bibl_id)
                .fetch_optional(&self.db)
                .await?;

        match status {
            Some((s,)) if s == "pending_fetch" => {}
            _ => {
                debug!(
                    "Bibliography {} not in pending_fetch, skipping.",
                    job.bibl_id
                );
                return Ok(());
            }
        }

        let entry = match self.http.fetch_bibliography(job.bibl_id).await {
            Ok(v) => v,
            Err(e) if is_not_found(&e) => {
                warn!("Bibliography {} not found.", job.bibl_id);
                storage::mark_entity_not_found(&self.db, "bibliography", job.bibl_id).await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        storage::store_bibliography(&self.db, job.bibl_id, &entry).await?;
        indexing::index_bibliography(&self.meili, job.bibl_id, &entry).await?;

        let code = entry.get("code").and_then(|v| v.as_str()).unwrap_or("");
        if !code.is_empty() {
            let resolved = storage::resolve_inline_ref_as_bibl(&self.db, job.bibl_id, code).await?;
            if resolved > 0 {
                debug!(
                    "Resolved {resolved} inline refs for code '{code}' → bibl {}",
                    job.bibl_id
                );
            }
        }

        let affected = storage::mark_articles_for_reindex_by_bibl(&self.db, job.bibl_id).await?;
        self.enqueue_reindex_if_needed(&affected, &format!("bibl-{}", job.bibl_id), || {
            format!("Bibliography {} synced.", job.bibl_id)
        })
        .await?;

        Ok(())
    }

    /// Handle fetching a place entry.
    pub async fn handle_fetch_place(&self, job: FetchPlaceJob) -> Result<()> {
        let status: Option<(String,)> =
            sqlx::query_as("SELECT sync_status FROM places WHERE id = $1")
                .bind(job.place_id)
                .fetch_optional(&self.db)
                .await?;

        match status {
            Some((s,)) if s == "pending_fetch" => {}
            _ => {
                debug!("Place {} not in pending_fetch, skipping.", job.place_id);
                return Ok(());
            }
        }

        let entry = match self.http.fetch_place(job.place_id).await {
            Ok(v) => v,
            Err(e) if is_not_found(&e) => {
                warn!("Place {} not found.", job.place_id);
                storage::mark_entity_not_found(&self.db, "places", job.place_id).await?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        storage::store_place(&self.db, job.place_id, &entry).await?;
        indexing::index_place(&self.meili, job.place_id, &entry).await?;
        self.enqueue_child_place_fetches(&entry).await?;

        let place_name = entry
            .get("place_name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !place_name.is_empty() {
            let resolved =
                storage::resolve_inline_place_by_name(&self.db, job.place_id, place_name).await?;
            if resolved > 0 {
                debug!(
                    "Resolved {resolved} inline refs for place '{place_name}' → ID {}",
                    job.place_id
                );
            }
        }

        let affected = storage::mark_articles_for_reindex_by_place(&self.db, job.place_id).await?;
        self.enqueue_reindex_if_needed(&affected, &format!("place-{}", job.place_id), || {
            format!("Place {} synced.", job.place_id)
        })
        .await?;

        Ok(())
    }

    /// Handle resolving an inline code.
    pub async fn handle_resolve_inline_code(&self, job: ResolveInlineCodeJob) -> Result<()> {
        if !self.http.has_clarino_key() {
            debug!(
                "No CLARINO_API_KEY set, cannot resolve inline code '{}'.",
                job.code
            );
            return Ok(());
        }

        let still_unresolved: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM inline_ref_parse WHERE code = $1 AND ref_type IS NULL LIMIT 1",
        )
        .bind(&job.code)
        .fetch_optional(&self.db)
        .await?;

        if still_unresolved.is_none() {
            debug!("Inline code '{}' already resolved, skipping.", job.code);
            return Ok(());
        }

        debug!("Resolving inline code '{}'…", job.code);

        if let Ok(bibl_entries) = self.http.fetch_bibliography_by_code(&job.code).await
            && let Some(entry) = bibl_entries.first()
            && let Some(bibl_id) = entry.get("bibl_id").and_then(serde_json::Value::as_i64)
        {
            let exists: Option<(i64,)> = sqlx::query_as(
                "SELECT id FROM bibliography WHERE id = $1 AND sync_status = 'idle'",
            )
            .bind(bibl_id)
            .fetch_optional(&self.db)
            .await?;

            if exists.is_none() {
                storage::store_bibliography(&self.db, bibl_id, entry).await?;
                indexing::index_bibliography(&self.meili, bibl_id, entry).await?;
            }

            let resolved =
                storage::resolve_inline_ref_as_bibl(&self.db, bibl_id, &job.code).await?;
            if resolved > 0 {
                debug!(
                    "Resolved inline code '{}' as bibliography ID {bibl_id}",
                    job.code
                );
            }
            return Ok(());
        }

        let name_candidates: Vec<&str> = if let Some(stripped) = job.code.strip_suffix('M') {
            vec![stripped, &job.code]
        } else {
            vec![&job.code]
        };

        for name in name_candidates {
            if let Ok(Some((place_id, entry))) = self.http.fetch_place_by_name(name).await {
                self.ensure_place_stored(place_id, &entry).await?;

                let resolved =
                    storage::resolve_inline_place_by_name(&self.db, place_id, name).await?;
                if resolved > 0 {
                    debug!(
                        "Resolved inline code '{}' as place '{name}', ID {place_id}",
                        job.code
                    );
                }
                return Ok(());
            }
        }

        debug!("Could not resolve inline code '{}'.", job.code);
        Ok(())
    }

    /// Handle inline reference backfill for a batch of articles.
    pub async fn handle_backfill_inline_refs(&self, job: BackfillInlineRefsJob) -> Result<()> {
        let rows: Vec<(i64, Value)> = sqlx::query_as(
            "SELECT id, data FROM articles WHERE dictionary = 'no' AND id = ANY($1)",
        )
        .bind(&job.article_ids)
        .fetch_all(&self.db)
        .await?;

        let mut all_unresolved: HashSet<String> = HashSet::new();
        let mut tx = self.db.begin().await?;

        for (article_id, data) in &rows {
            let refs = extraction::extract_inline_refs(data);
            if refs.is_empty() {
                continue;
            }

            let (resolved_ids, unresolved_codes) =
                storage::store_inline_refs(&mut tx, "no", *article_id, &refs).await?;

            if !resolved_ids.is_empty() {
                sqlx::query(
                    "INSERT INTO article_bibliography (dictionary, article_id, bibl_id)
                     SELECT 'no', $1, unnest($2::bigint[])
                     ON CONFLICT DO NOTHING",
                )
                .bind(article_id)
                .bind(&resolved_ids)
                .execute(&mut *tx)
                .await?;
            }

            all_unresolved.extend(unresolved_codes);
        }

        for code in &all_unresolved {
            storage::write_outbox_resolve_code(&mut tx, code).await?;
        }

        tx.commit().await?;

        let count = self
            .inline_refs_backfill_count
            .fetch_add(job.article_ids.len() as u64, Ordering::Relaxed)
            + job.article_ids.len() as u64;
        if count.is_multiple_of(5000) {
            info!("{count} articles backfilled with inline reference data.");
        } else if job.article_ids.len() < 1000 {
            info!("Inline reference backfill complete: {count} articles processed.");
        }
        if !all_unresolved.is_empty() {
            debug!(
                "Backfill batch produced {} unresolved codes to look up.",
                all_unresolved.len()
            );
        }

        Ok(())
    }

    /// Handle periodic sweep.
    pub async fn handle_sweep(&self) -> Result<()> {
        crate::outbox::run_sweep(&self.db).await?;
        self.cleanup_orphans().await;
        self.vacuum_queues().await;
        Ok(())
    }

    /// Pick up orphaned jobs that are stuck to workers that died.
    async fn cleanup_orphans(&self) {
        let script = redis::Script::new(
            r"
            local workers_set = KEYS[1]
            local active_list = KEYS[2]
            local signal_list = KEYS[3]
            local cutoff = ARGV[1]

            local dead_workers = redis.call('zrangebyscore', workers_set, 0, cutoff)
            local total = 0

            for _, inflight_key in ipairs(dead_workers) do
                local jobs = redis.call('smembers', inflight_key)
                if #jobs > 0 then
                    redis.call('rpush', active_list, unpack(jobs))
                    redis.call('del', inflight_key)
                    total = total + #jobs
                end
                redis.call('zrem', workers_set, inflight_key)
            end

            if total > 0 then
                redis.call('del', signal_list)
                redis.call('lpush', signal_list, 1)
            end

            return total
            ",
        );

        let cutoff = chrono::Utc::now().timestamp() - 300;

        for ns in crate::jobs::QUEUE_NAMESPACES {
            let config = RedisConfig::default().set_namespace(ns);
            let mut conn = self.redis_conn.clone();

            let result: Result<i64, _> = script
                .key(config.workers_set())
                .key(config.active_jobs_list())
                .key(config.signal_list())
                .arg(cutoff)
                .invoke_async(&mut conn)
                .await;

            match result {
                Ok(0) => {}
                Ok(n) => info!("Re-enqueued {n} orphaned jobs from {ns}"),
                Err(e) => warn!("Failed to reenqueue orphaned jobs in {ns}: {e}"),
            }
        }
    }

    /// Clean up old jobs.
    async fn vacuum_queues(&self) {
        for ns in crate::jobs::QUEUE_NAMESPACES {
            let config = RedisConfig::default().set_namespace(ns);
            let mut storage =
                RedisStorage::<()>::new_with_config(self.redis_conn.clone(), config.clone());
            match storage.vacuum().await {
                Ok(0) => {}
                Ok(n) => info!("Vacuumed {n} terminal jobs from {ns}"),
                Err(e) => warn!("Failed to vacuum {ns}: {e}"),
            }

            // Apalis only cleans data and metadata, but not the sorted sets
            // that track completed jobs.
            let sets = [
                config.done_jobs_set(),
                config.dead_jobs_set(),
                config.failed_jobs_set(),
            ];
            let mut conn = self.redis_conn.clone();
            for set_key in &sets {
                let removed: Result<u64, _> =
                    redis::cmd("DEL").arg(set_key).query_async(&mut conn).await;
                match removed {
                    Ok(1) => debug!("Cleared sorted set {set_key}"),
                    Ok(_) => {}
                    Err(e) => warn!("Failed to clear {set_key}: {e}"),
                }
            }
        }
    }

    /// Check if we need to perform an initial sync for each dictionary.
    pub async fn initial_sync(
        &self,
        fetch_article_list_storage: &RedisStorage<FetchArticleListJob>,
        fetch_dict_metadata_storage: &RedisStorage<FetchDictionaryMetadataJob>,
    ) -> Result<()> {
        for dict in UibDictionary::all() {
            let dict_str = dict.as_str();

            let has_articles: (bool,) =
                sqlx::query_as("SELECT EXISTS(SELECT 1 FROM articles WHERE dictionary = $1)")
                    .bind(dict_str)
                    .fetch_one(&self.db)
                    .await?;

            let already_queued: Option<(String,)> = sqlx::query_as(
                "SELECT value FROM sync_state WHERE dictionary = $1 AND key = 'initial_queue_done'",
            )
            .bind(dict_str)
            .fetch_optional(&self.db)
            .await?;

            if !has_articles.0 && already_queued.is_none() {
                let inserted: Option<(String,)> = sqlx::query_as(
                    "INSERT INTO sync_state (dictionary, key, value)
                     VALUES ($1, 'initial_queue_done', '1')
                     ON CONFLICT (dictionary, key) DO NOTHING
                     RETURNING value",
                )
                .bind(dict_str)
                .fetch_optional(&self.db)
                .await?;

                if inserted.is_some() {
                    warn!("[{dict}] No articles in DB, queueing initial article fetch.");
                    let mut storage = fetch_article_list_storage.clone();
                    storage
                        .push(FetchArticleListJob {
                            dictionary: dict_str.to_string(),
                            force: false,
                        })
                        .await
                        .map_err(|e| anyhow!("Failed to enqueue: {e}"))?;
                }
            }

            let has_concepts: Option<(String,)> = sqlx::query_as(
                "SELECT key FROM dictionary_metadata WHERE dictionary = $1 AND key = 'concepts'",
            )
            .bind(dict_str)
            .fetch_optional(&self.db)
            .await?;

            if has_concepts.is_none() {
                warn!("[{dict}] Missing dictionary metadata, enqueueing fetch.");
                let mut storage = fetch_dict_metadata_storage.clone();
                storage
                    .push(FetchDictionaryMetadataJob {
                        dictionary: dict_str.to_string(),
                    })
                    .await
                    .map_err(|e| anyhow!("Failed to enqueue: {e}"))?;
            }
        }
        Ok(())
    }

    /// Enqueue initial bibliography sync for entries not yet fetched.
    pub async fn enqueue_initial_bibliography_sync(&self) -> Result<()> {
        let Some(ids) = self
            .fetch_unfetched_entity_ids(
                "SELECT DISTINCT ab.bibl_id FROM article_bibliography ab
                 LEFT JOIN bibliography b ON ab.bibl_id = b.id
                 WHERE b.id IS NULL OR b.sync_status = 'pending_fetch'",
                "bibliography",
            )
            .await?
        else {
            return Ok(());
        };

        let mut tx = self.db.begin().await?;
        for id in &ids {
            storage::write_outbox_fetch_bibl(&mut tx, *id).await?;
        }
        tx.commit().await?;

        info!("Enqueued {} bibliography fetch jobs.", ids.len());
        Ok(())
    }

    /// Enqueue initial place sync for entries not yet fetched.
    pub async fn enqueue_initial_place_sync(&self) -> Result<()> {
        let Some(ids) = self
            .fetch_unfetched_entity_ids(
                "SELECT DISTINCT ap.place_id FROM article_place ap
                 LEFT JOIN places p ON ap.place_id = p.id
                 WHERE p.id IS NULL OR p.sync_status = 'pending_fetch'",
                "place",
            )
            .await?
        else {
            return Ok(());
        };

        let mut tx = self.db.begin().await?;
        for id in &ids {
            storage::write_outbox_fetch_place(&mut tx, *id).await?;
        }
        tx.commit().await?;

        info!("Enqueued {} place fetch jobs.", ids.len());
        Ok(())
    }

    /// Query for unfetched entity IDs.
    async fn fetch_unfetched_entity_ids(
        &self,
        query: &str,
        entity_name: &str,
    ) -> Result<Option<Vec<i64>>> {
        if !self.http.has_clarino_key() {
            info!("No CLARINO_API_KEY set, skipping initial {entity_name} sync.");
            return Ok(None);
        }

        let rows: Vec<(i64,)> = sqlx::query_as(query).fetch_all(&self.db).await?;

        if rows.is_empty() {
            info!("No unfetched {entity_name} IDs.");
            return Ok(None);
        }

        let ids: Vec<i64> = rows.into_iter().map(|(id,)| id).collect();
        info!(
            "Found {} unfetched {entity_name} IDs, enqueueing…",
            ids.len()
        );
        Ok(Some(ids))
    }

    /// Enqueue backfill jobs for inline reference data.
    pub async fn enqueue_inline_refs_backfill(
        &self,
        backfill_storage: &RedisStorage<BackfillInlineRefsJob>,
    ) -> Result<()> {
        let already_done: Option<(String,)> = sqlx::query_as(
            "SELECT value FROM sync_state WHERE dictionary = 'no' AND key = 'inline_refs_backfill_enqueued'",
        )
        .fetch_optional(&self.db)
        .await?;

        if already_done.is_some() {
            debug!("Inline reference backfill already enqueued, skipping.");
            return Ok(());
        }

        let has_articles: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM articles WHERE dictionary = 'no')")
                .fetch_one(&self.db)
                .await?;

        if !has_articles.0 {
            return Ok(());
        }

        info!("Enqueueing inline reference backfill jobs…");

        let mut stream = sqlx::query_as::<_, (i64,)>(
            "SELECT id FROM articles WHERE dictionary = 'no' ORDER BY id",
        )
        .fetch(&self.db);

        let mut batch: Vec<i64> = Vec::with_capacity(1000);
        let mut count = 0usize;
        let mut storage = backfill_storage.clone();

        while let Some(row) = stream.next().await {
            let (article_id,) = row?;
            batch.push(article_id);

            if batch.len() >= 1000 {
                let batch_to_send = std::mem::take(&mut batch);
                storage
                    .push(BackfillInlineRefsJob {
                        article_ids: batch_to_send,
                    })
                    .await
                    .map_err(|e| anyhow!("Failed to enqueue backfill batch: {e}"))?;
                count += 1000;
            }
        }

        if !batch.is_empty() {
            count += batch.len();
            storage
                .push(BackfillInlineRefsJob { article_ids: batch })
                .await
                .map_err(|e| anyhow!("Failed to enqueue backfill batch: {e}"))?;
        }

        sqlx::query(
            "INSERT INTO sync_state (dictionary, key, value)
             VALUES ('no', 'inline_refs_backfill_enqueued', '1')
             ON CONFLICT (dictionary, key) DO NOTHING",
        )
        .execute(&self.db)
        .await?;

        info!("Enqueued backfill for {count} articles.");
        Ok(())
    }

    /// Enqueue a full resync for specific dictionaries.
    pub async fn enqueue_resync(
        &self,
        dictionaries: &[UibDictionary],
        fetch_article_list_storage: &RedisStorage<FetchArticleListJob>,
        fetch_dict_metadata_storage: &RedisStorage<FetchDictionaryMetadataJob>,
    ) -> Result<()> {
        for dict in dictionaries {
            info!("[{dict}] Enqueuing full resync…");

            let mut storage = fetch_article_list_storage.clone();
            storage
                .push(FetchArticleListJob {
                    dictionary: dict.as_str().to_string(),
                    force: true,
                })
                .await
                .map_err(|e| anyhow!("Failed to enqueue: {e}"))?;

            let mut meta_storage = fetch_dict_metadata_storage.clone();
            meta_storage
                .push(FetchDictionaryMetadataJob {
                    dictionary: dict.as_str().to_string(),
                })
                .await
                .map_err(|e| anyhow!("Failed to enqueue: {e}"))?;
        }
        Ok(())
    }

    /// Store and index a place if not already present, then enqueue fetches for
    /// any child places it references.
    async fn ensure_place_stored(&self, place_id: i64, entry: &Value) -> Result<()> {
        let exists: Option<(i64,)> =
            sqlx::query_as("SELECT id FROM places WHERE id = $1 AND sync_status = 'idle'")
                .bind(place_id)
                .fetch_optional(&self.db)
                .await?;

        if exists.is_none() {
            storage::store_place(&self.db, place_id, entry).await?;
            indexing::index_place(&self.meili, place_id, entry).await?;
            self.enqueue_child_place_fetches(entry).await?;
        }

        Ok(())
    }

    /// Extract child place IDs from a place entry and enqueue fetch jobs for
    /// new ones.
    async fn enqueue_child_place_fetches(&self, entry: &Value) -> Result<()> {
        let child_ids = extraction::extract_child_place_ids(entry);
        if !child_ids.is_empty() {
            let child_vec: Vec<i64> = child_ids.into_iter().collect();
            let mut tx = self.db.begin().await?;
            for place_id in &child_vec {
                storage::write_outbox_fetch_place(&mut tx, *place_id).await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }

    /// Enqueue re-indexing for a batch of articles.
    async fn enqueue_reindex_if_needed(
        &self,
        affected: &[(String, i64)],
        batch_key: &str,
        context_msg: impl FnOnce() -> String,
    ) -> Result<()> {
        if affected.is_empty() {
            debug!("{}.", context_msg());
        } else {
            debug!(
                "{}, {} articles marked for re-index.",
                context_msg(),
                affected.len()
            );
            let mut tx = self.db.begin().await?;
            storage::write_outbox_batch_index(&mut tx, batch_key, affected).await?;
            tx.commit().await?;
        }
        Ok(())
    }
}

/// Check if an error is a 404 Not Found HTTP response.
fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<reqwest::Error>()
        .and_then(reqwest::Error::status)
        == Some(reqwest::StatusCode::NOT_FOUND)
}
