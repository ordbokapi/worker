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
use apalis_redis::RedisStorage;
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
    FetchBibliographyJob, FetchDictionaryMetadataJob, FetchPlaceJob, IndexArticleJob,
    ResolveInlineCodeJob,
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

        let mut to_fetch: Vec<i64> = Vec::new();
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
                if existing.sync_status != SyncStatus::Idle {
                    continue;
                }

                if existing.revision == meta.revision && existing.updated_at == meta.updated_at {
                    continue;
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

            to_fetch.push(meta.article_id);
        }

        let mut missing_count = 0u32;
        for (existing_id, ex_meta) in &existing_metadata_map {
            if !new_id_set.contains(existing_id) && ex_meta.sync_status == SyncStatus::Idle {
                debug!(
                    "[{dict}] Article {} ({}) no longer in UiB list.",
                    existing_id, ex_meta.primary_lemma
                );
                to_fetch.push(*existing_id);
                missing_count += 1;
            }
        }

        if missing_count > 0 {
            warn!(
                "[{dict}] {} articles no longer in UiB list, will re-fetch to check-",
                missing_count
            );
        }

        let mut tx = self.db.begin().await?;
        let marked = storage::mark_articles_pending_fetch(&mut tx, dict, &to_fetch).await?;

        for article_id in &marked {
            storage::write_outbox_fetch_article(&mut tx, dict.as_str(), *article_id).await?;
        }

        tx.commit().await?;

        info!(
            "[{dict}] Enqueued {} article fetch jobs, {} already in flight.",
            marked.len(),
            to_fetch.len() - marked.len()
        );

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

        info!("[{dict}] Fetching article {} from UiB…", job.article_id);
        let article_data = self.http.fetch_article(dict, job.article_id).await?;

        let analysis = extraction::analyze_article(dict, &article_data);

        let revision = article_data
            .get("revision")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let updated_at = article_data
            .get("updated")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

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

        info!(
            "[{dict}] Article {} ({}) stored. Pending index: bibl:{}, places:{}, related:{}",
            job.article_id,
            analysis.primary_lemma,
            result.bibl_fetched,
            result.places_fetched,
            result.related_fetched,
        );

        Ok(())
    }

    /// Handle indexing a single article.
    pub async fn handle_index_article(&self, job: IndexArticleJob) -> Result<()> {
        let dict = UibDictionary::parse(&job.dictionary)
            .ok_or_else(|| anyhow!("Unknown dictionary: {}", job.dictionary))?;

        let status: Option<(String,)> =
            sqlx::query_as("SELECT sync_status FROM articles WHERE dictionary = $1 AND id = $2")
                .bind(dict.as_str())
                .bind(job.article_id)
                .fetch_optional(&self.db)
                .await?;

        match status {
            Some((s,)) if s == "pending_index" => {}
            _ => {
                debug!(
                    "[{dict}] Article {} not pending index, skipping index.",
                    job.article_id
                );
                return Ok(());
            }
        }

        indexing::index_article(&self.meili, &self.db, dict, job.article_id).await?;
        debug!("[{dict}] Article {} indexed.", job.article_id);
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

        info!("Batch indexing {} articles…", article_keys.len());
        indexing::batch_index_articles(&self.meili, &self.db, &article_keys).await?;
        info!("Batch index complete.");
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
                info!(
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
                info!(
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

        info!("Resolving inline code '{}'…", job.code);

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
                info!(
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
                    info!(
                        "Resolved inline code '{}' as place '{name}', ID {place_id}",
                        job.code
                    );
                }
                return Ok(());
            }
        }

        info!("Could not resolve inline code '{}'.", job.code);
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
                store_inline_refs_in_tx(&mut tx, "no", *article_id, &refs).await?;

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
        crate::outbox::run_sweep(&self.db).await
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
        let to_fetch = storage::mark_bibl_pending_fetch(&mut tx, &ids).await?;
        for id in &to_fetch {
            storage::write_outbox_fetch_bibl(&mut tx, *id).await?;
        }
        tx.commit().await?;

        info!("Enqueued {} bibliography fetch jobs.", to_fetch.len());
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
        let to_fetch = storage::mark_places_pending_fetch(&mut tx, &ids).await?;
        for id in &to_fetch {
            storage::write_outbox_fetch_place(&mut tx, *id).await?;
        }
        tx.commit().await?;

        info!("Enqueued {} place fetch jobs.", to_fetch.len());
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
                storage
                    .push(BackfillInlineRefsJob {
                        article_ids: batch.clone(),
                    })
                    .await
                    .map_err(|e| anyhow!("Failed to enqueue backfill batch: {e}"))?;
                count += batch.len();
                batch.clear();
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
        // Reset all articles for these dictionaries to pending_fetch.
        for dict in dictionaries {
            info!("[{dict}] Resetting articles to pending_fetch for resync…");

            sqlx::query(
                "UPDATE articles SET sync_status = 'pending_fetch', status_changed_at = now()
                 WHERE dictionary = $1",
            )
            .bind(dict.as_str())
            .execute(&self.db)
            .await?;

            let mut storage = fetch_article_list_storage.clone();
            storage
                .push(FetchArticleListJob {
                    dictionary: dict.as_str().to_string(),
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
            let to_fetch = storage::mark_places_pending_fetch(&mut tx, &child_vec).await?;
            for place_id in &to_fetch {
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
            info!("{}.", context_msg());
        } else {
            info!(
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

/// Store inline references for an article.
#[allow(clippy::too_many_lines)]
async fn store_inline_refs_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    dict_str: &str,
    article_id: i64,
    refs: &[extraction::InlineRef],
) -> Result<(Vec<i64>, Vec<String>)> {
    use std::collections::HashMap;

    sqlx::query("DELETE FROM inline_ref_parse WHERE dictionary = $1 AND article_id = $2")
        .bind(dict_str)
        .bind(article_id)
        .execute(&mut **tx)
        .await?;

    if refs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let codes: Vec<&str> = refs
        .iter()
        .map(|r| r.code.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let resolved_bibl: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, code FROM bibliography WHERE code = ANY($1)")
            .bind(&codes)
            .fetch_all(&mut **tx)
            .await?;

    let mut code_to_bibl: HashMap<&str, i64> = HashMap::new();
    for (id, code) in &resolved_bibl {
        code_to_bibl.entry(code.as_str()).or_insert(*id);
    }

    let unresolved_codes: Vec<&str> = codes
        .iter()
        .filter(|c| !code_to_bibl.contains_key(*c))
        .copied()
        .collect();

    let mut code_to_place: HashMap<&str, i64> = HashMap::new();
    if !unresolved_codes.is_empty() {
        let place_names: Vec<String> = unresolved_codes
            .iter()
            .flat_map(|c| {
                let mut names = vec![c.to_string()];
                if let Some(stripped) = c.strip_suffix('M') {
                    names.push(stripped.to_string());
                }
                names
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let resolved_places: Vec<(i64, String)> =
            sqlx::query_as("SELECT id, place_name FROM places WHERE place_name = ANY($1)")
                .bind(&place_names)
                .fetch_all(&mut **tx)
                .await?;

        let place_name_to_id: HashMap<&str, i64> = resolved_places
            .iter()
            .map(|(id, name)| (name.as_str(), *id))
            .collect();

        for code in &unresolved_codes {
            if let Some(&pid) = place_name_to_id.get(*code) {
                code_to_place.insert(code, pid);
            } else if let Some(stripped) = code.strip_suffix('M')
                && let Some(&pid) = place_name_to_id.get(stripped)
            {
                code_to_place.insert(code, pid);
            }
        }
    }

    let mut resolved_bibl_ids = Vec::new();

    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO inline_ref_parse \
         (dictionary, article_id, quote_content, offset_start, offset_end, code, spec, ref_type, bibl_id, place_id) ",
    );
    qb.push_values(refs, |mut b, r| {
        let bibl_id = code_to_bibl.get(r.code.as_str()).copied();
        let place_id = code_to_place.get(r.code.as_str()).copied();
        let ref_type = if bibl_id.is_some() {
            Some("bibl")
        } else if place_id.is_some() {
            Some("place")
        } else {
            None
        };

        if let Some(id) = bibl_id {
            resolved_bibl_ids.push(id);
        }

        b.push_bind(dict_str.to_owned())
            .push_bind(article_id)
            .push_bind(r.quote_content.clone())
            .push_bind(i32::try_from(r.offset_start).unwrap_or(i32::MAX))
            .push_bind(i32::try_from(r.offset_end).unwrap_or(i32::MAX))
            .push_bind(r.code.clone())
            .push_bind(r.spec.clone())
            .push_bind(ref_type)
            .push_bind(bibl_id)
            .push_bind(place_id);
    });
    qb.build().execute(&mut **tx).await?;

    let still_unresolved: Vec<String> = refs
        .iter()
        .filter(|r| {
            !code_to_bibl.contains_key(r.code.as_str())
                && !code_to_place.contains_key(r.code.as_str())
        })
        .map(|r| r.code.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    Ok((resolved_bibl_ids, still_unresolved))
}
