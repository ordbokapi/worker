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
use apalis_redis::{ConnectionManager, RedisStorage};
use clap::ValueEnum;
use futures::StreamExt;
use meilisearch_sdk::client::Client as MeiliClient;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

use crate::meili;

/// Dictionary variants for UiB
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum UibDictionary {
    Bokmål,
    Nynorsk,
    NorskOrdbok,
}

impl UibDictionary {
    /// Return all possible dictionary variants.
    pub fn all() -> &'static [Self] {
        &[Self::Bokmål, Self::Nynorsk, Self::NorskOrdbok]
    }

    /// Convert to string for usage in database keys or for the UiB API path, etc.
    pub fn as_str(&self) -> &'static str {
        match self {
            UibDictionary::Bokmål => "bm",
            UibDictionary::Nynorsk => "nn",
            UibDictionary::NorskOrdbok => "no",
        }
    }
}

impl ValueEnum for UibDictionary {
    fn value_variants<'a>() -> &'a [Self] {
        Self::all()
    }

    fn from_str(input: &str, _ignore_case: bool) -> Result<Self, String> {
        match input {
            "bm" => Ok(Self::Bokmål),
            "nn" => Ok(Self::Nynorsk),
            "no" => Ok(Self::NorskOrdbok),
            _ => Err(format!("Unknown dictionary: {}", input)),
        }
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(clap::builder::PossibleValue::new(self.as_str()))
    }
}

/// A minimal struct to store the article metadata we need to check for changes.
#[derive(Debug)]
pub struct ArticleMetadata {
    pub article_id: i64,
    pub primary_lemma: String,
    pub revision: Option<i64>,
    pub updated_at: String,
}

/// Fetch the full article list for a dictionary from UiB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchArticleListJob {
    pub dictionary: String,
}

/// Fetch a single article from UiB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchArticleJob {
    pub dictionary: String,
    pub article_id: i64,
    pub primary_lemma: String,
    pub revision: Option<i64>,
    pub updated_at: String,
}

/// Fetch dictionary metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchDictionaryMetadataJob {
    pub dictionary: String,
}

/// Send a Matrix notification message.
#[cfg(feature = "matrix_notifs")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMatrixMessageJob {
    pub message: String,
}

/// Fetch a single bibliography entry from the word bank API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchBibliographyJob {
    pub bibl_id: i64,
}

/// Fetch a single place entry from the word bank API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchPlaceJob {
    pub place_id: i64,
}

/// Backfill inline refs for one or more articles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillInlineRefsJob {
    pub article_ids: Vec<i64>,
}

/// Attempt to resolve an unresolved inline code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveInlineCodeJob {
    pub code: String,
}

/// Drain the pending reindex set and batch index all accumulated articles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainPendingReindexJob;

/// Main service that handles fetch and store logic.
#[derive(Clone)]
pub struct ArticleSyncService {
    pub db: PgPool,
    pub meili: MeiliClient,
    pub redis_conn: ConnectionManager,
    pub clarino_api_key: Option<String>,
    pub fetch_article_list_storage: RedisStorage<FetchArticleListJob>,
    pub fetch_article_storage: RedisStorage<FetchArticleJob>,
    pub fetch_dict_metadata_storage: RedisStorage<FetchDictionaryMetadataJob>,
    pub fetch_bibliography_storage: RedisStorage<FetchBibliographyJob>,
    pub fetch_place_storage: RedisStorage<FetchPlaceJob>,
    pub backfill_inline_refs_storage: RedisStorage<BackfillInlineRefsJob>,
    pub resolve_inline_code_storage: RedisStorage<ResolveInlineCodeJob>,
    pub inline_refs_backfill_count: Arc<AtomicU64>,
    pub drain_pending_reindex_storage: RedisStorage<DrainPendingReindexJob>,
    #[cfg(feature = "matrix_notifs")]
    pub matrix_message_storage: RedisStorage<SendMatrixMessageJob>,
}

impl ArticleSyncService {
    const DEDUPE_TTL: u64 = 24 * 60 * 60;
    const PENDING_REINDEX_KEY: &str = "ordbokapi:pending-reindex";

    fn parse_dict(s: &str) -> Result<UibDictionary> {
        UibDictionary::from_str(s, false).map_err(|e| anyhow!("{e}"))
    }

    /// Mark an article as queued.
    async fn try_mark_article_queued(&self, dict: &str, article_id: i64) -> Result<bool> {
        let key = format!("ordbokapi:article-queued:{dict}:{article_id}");
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(Self::DEDUPE_TTL)
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Redis SET NX EX failed: {e}"))?;
        Ok(result.is_some())
    }

    /// Try to mark articles as queued.
    async fn try_mark_articles_queued(&self, dict: &str, article_ids: &[i64]) -> Result<Vec<bool>> {
        let mut pipe = redis::pipe();
        for &id in article_ids {
            let key = format!("ordbokapi:article-queued:{dict}:{id}");
            pipe.cmd("SET")
                .arg(key)
                .arg(1)
                .arg("NX")
                .arg("EX")
                .arg(Self::DEDUPE_TTL);
        }
        let results: Vec<Option<String>> = pipe
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Redis pipeline SET NX EX failed: {e}"))?;
        Ok(results.iter().map(|r| r.is_some()).collect())
    }

    /// Mark a bibliography entry as queued.
    async fn try_mark_bibl_queued(&self, bibl_id: i64) -> Result<bool> {
        let key = format!("ordbokapi:bibl-queued:{bibl_id}");
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(Self::DEDUPE_TTL)
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Redis SET NX EX failed: {e}"))?;
        Ok(result.is_some())
    }

    /// Mark a place entry as queued.
    async fn try_mark_place_queued(&self, place_id: i64) -> Result<bool> {
        let key = format!("ordbokapi:place-queued:{place_id}");
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(Self::DEDUPE_TTL)
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Redis SET NX EX failed: {e}"))?;
        Ok(result.is_some())
    }

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

    /// Fire off a job to do the full article-list sync for a dictionary.
    pub async fn enqueue_sync_articles(&self, dict: UibDictionary) -> Result<()> {
        let key = format!("ordbokapi:article-list-queued:{}", dict.as_str());
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(Self::DEDUPE_TTL)
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Redis SET NX EX failed: {e}"))?;
        if result.is_none() {
            info!("[{:?}] FetchArticleListJob already queued, skipping", dict);
            return Ok(());
        }
        let mut storage = self.fetch_article_list_storage.clone();
        storage
            .push(FetchArticleListJob {
                dictionary: dict.as_str().to_string(),
            })
            .await
            .map_err(|e| anyhow!("Failed to enqueue FetchArticleList: {e}"))?;
        Ok(())
    }

    /// Fire off a job to do the dictionary metadata sync.
    pub async fn enqueue_sync_dict_metadata(&self, dict: UibDictionary) -> Result<()> {
        let mut storage = self.fetch_dict_metadata_storage.clone();
        storage
            .push(FetchDictionaryMetadataJob {
                dictionary: dict.as_str().to_string(),
            })
            .await
            .map_err(|e| anyhow!("Failed to enqueue FetchDictionaryMetadata: {e}"))?;
        Ok(())
    }

    /// Fire off a job to fetch a single article.
    pub async fn enqueue_sync_article(
        &self,
        dict: UibDictionary,
        metadata: &ArticleMetadata,
    ) -> Result<()> {
        let mut storage = self.fetch_article_storage.clone();
        storage
            .push(FetchArticleJob {
                dictionary: dict.as_str().to_string(),
                article_id: metadata.article_id,
                primary_lemma: metadata.primary_lemma.clone(),
                revision: metadata.revision,
                updated_at: metadata.updated_at.clone(),
            })
            .await
            .map_err(|e| anyhow!("Failed to enqueue FetchArticle: {e}"))?;
        Ok(())
    }

    /// Job handler for "FetchArticleList".
    pub async fn handle_sync_articles(&self, job: FetchArticleListJob) -> Result<()> {
        let dict = Self::parse_dict(&job.dictionary)?;
        info!("[{:?}] Fetching article list from UiB API", dict);

        let mut article_list = self.fetch_article_list_from_uib(dict).await?;
        info!(
            "[{:?}] {} articles returned from UiB",
            dict,
            article_list.len()
        );

        let existing_metadata_map = self.get_all_article_metadata(dict).await?;
        info!(
            "[{:?}] We have {} existing articles in DB",
            dict,
            existing_metadata_map.len()
        );

        // Prepare a vector to hold all the JSON payloads for articles we need to sync.
        let mut to_enqueue = Vec::new();
        let mut new_id_set = std::collections::HashSet::new();
        #[cfg(feature = "matrix_notifs")]
        let mut new_count = 0u32;
        #[cfg(feature = "matrix_notifs")]
        let mut updated_count = 0u32;

        for raw_meta in article_list.drain(..) {
            let meta = Self::convert_raw_article_metadata(raw_meta)?;
            new_id_set.insert(meta.article_id);

            if let Some(existing) = existing_metadata_map.get(&meta.article_id) {
                if existing.revision == meta.revision && existing.updated_at == meta.updated_at {
                    continue;
                }
                debug!(
                    "[{:?}] Article {} ({}) changed",
                    dict, meta.article_id, meta.primary_lemma
                );
                #[cfg(feature = "matrix_notifs")]
                {
                    updated_count += 1;
                }
            } else {
                debug!(
                    "[{:?}] New article {} ({})",
                    dict, meta.article_id, meta.primary_lemma
                );
                #[cfg(feature = "matrix_notifs")]
                {
                    new_count += 1;
                }
            }

            to_enqueue.push(FetchArticleJob {
                dictionary: dict.as_str().to_string(),
                article_id: meta.article_id,
                primary_lemma: meta.primary_lemma,
                revision: meta.revision,
                updated_at: meta.updated_at,
            });
        }

        // Detect articles no longer in UiB's new list. We should manually check these
        // by fetching since they may possibly still exist if fetched directly.
        let mut missing_count = 0u32;
        for (existing_id, ex_meta) in &existing_metadata_map {
            if !new_id_set.contains(existing_id) {
                debug!(
                    "[{:?}] Article {} ({}) no longer in UiB list",
                    dict, existing_id, ex_meta.primary_lemma
                );

                // Enqueue a fetch job for this article
                to_enqueue.push(FetchArticleJob {
                    dictionary: dict.as_str().to_string(),
                    article_id: *existing_id,
                    primary_lemma: ex_meta.primary_lemma.clone(),
                    revision: ex_meta.revision,
                    updated_at: ex_meta.updated_at.clone(),
                });
                missing_count += 1;
            }
        }

        if missing_count > 0 {
            warn!(
                "[{:?}] {} articles no longer in UiB list, will re-fetch to check for updates",
                dict, missing_count
            );
        }

        let candidates = to_enqueue.len();
        let mut enqueued = 0usize;

        // Perform a single pipelined batch enqueue
        if !to_enqueue.is_empty() {
            // Mark all articles as queued so queue_related_articles won't
            // re-enqueue them.
            let article_ids: Vec<i64> = to_enqueue.iter().map(|j| j.article_id).collect();
            let newly_marked = self
                .try_mark_articles_queued(dict.as_str(), &article_ids)
                .await?;

            let mut storage = self.fetch_article_storage.clone();
            for (job, is_new) in to_enqueue.into_iter().zip(newly_marked.iter()) {
                if *is_new {
                    storage
                        .push(job)
                        .await
                        .map_err(|e| anyhow!("Failed to enqueue FetchArticle: {e}"))?;
                    enqueued += 1;
                }
            }
        }

        info!(
            "[{:?}] Enqueued {} article fetch jobs, {} already queued.",
            dict,
            enqueued,
            candidates - enqueued
        );

        #[cfg(feature = "matrix_notifs")]
        {
            let msg = format!(
                "[{:?}] **Synkroniserer artiklar med UiB.**\n**Nye:** {}\n**Oppdaterte:** {}\n**Manglar i UiB-lista:** {}",
                dict, new_count, updated_count, missing_count
            );
            self.queue_matrix_message(&msg).await;
        }

        Ok(())
    }

    /// Job handler for "FetchArticle".
    pub async fn handle_sync_article(&self, job: FetchArticleJob) -> Result<()> {
        let dict = Self::parse_dict(&job.dictionary)?;

        info!(
            "[{:?}] Fetching article {} ({}) from UiB",
            dict, job.article_id, job.primary_lemma
        );

        let article_json = self.fetch_article_from_uib(dict, job.article_id).await?;

        // If partial_meta is incomplete, reconstruct more precise metadata.
        let primary_lemma = if job.primary_lemma.is_empty() {
            Self::find_first_lemma(&article_json)
        } else {
            job.primary_lemma.clone()
        };
        let revision = job.revision.unwrap_or(0);
        let updated_at = if job.updated_at.is_empty() {
            article_json
                .get("updated")
                .and_then(|v| v.as_str())
                .unwrap_or("1970-01-01T00:00:00Z")
                .to_string()
        } else {
            job.updated_at.clone()
        };

        // Store in PostgreSQL.
        self.store_article(
            dict,
            job.article_id,
            &article_json,
            &primary_lemma,
            revision,
            &updated_at,
        )
        .await?;

        // Index in Meilisearch.
        self.index_article(dict, job.article_id, &article_json)
            .await?;

        self.enqueue_bibliography_for_article(&article_json).await?;
        self.enqueue_places_for_article(dict, &article_json).await?;

        info!(
            "[{:?}] Article {} ({}) stored and indexed",
            dict, job.article_id, primary_lemma
        );

        // Then queue related articles
        self.queue_related_articles(dict, job.article_id, &article_json)
            .await?;

        Ok(())
    }

    /// Job handler for "FetchDictionaryMetadata".
    pub async fn handle_sync_dictionary_metadata(
        &self,
        job: FetchDictionaryMetadataJob,
    ) -> Result<()> {
        let dict = Self::parse_dict(&job.dictionary)?;
        info!("[{:?}] Fetching dictionary metadata from UiB", dict);

        let concepts = self.fetch_concepts(dict).await?;
        let word_classes = self.fetch_word_classes(dict).await?;
        let word_subclasses = self.fetch_word_subclasses(dict).await?;

        let dict_str = dict.as_str();

        sqlx::query(
            "INSERT INTO dictionary_metadata (dictionary, key, data, modified_at)
             VALUES ($1, 'concepts', $2, now())
             ON CONFLICT (dictionary, key) DO UPDATE SET data = $2, modified_at = now()",
        )
        .bind(dict_str)
        .bind(&concepts)
        .execute(&self.db)
        .await?;

        sqlx::query(
            "INSERT INTO dictionary_metadata (dictionary, key, data, modified_at)
             VALUES ($1, 'word_classes', $2, now())
             ON CONFLICT (dictionary, key) DO UPDATE SET data = $2, modified_at = now()",
        )
        .bind(dict_str)
        .bind(&word_classes)
        .execute(&self.db)
        .await?;

        sqlx::query(
            "INSERT INTO dictionary_metadata (dictionary, key, data, modified_at)
             VALUES ($1, 'word_subclasses', $2, now())
             ON CONFLICT (dictionary, key) DO UPDATE SET data = $2, modified_at = now()",
        )
        .bind(dict_str)
        .bind(&word_subclasses)
        .execute(&self.db)
        .await?;

        info!("[{:?}] Dictionary metadata stored in DB", dict);
        Ok(())
    }

    /// On startup, check if we need to do an initial sync for each dictionary.
    pub async fn initial_sync(&self) -> Result<()> {
        for dict in UibDictionary::all() {
            let dict_str = dict.as_str();

            // Check if there are any articles for this dictionary
            let has_articles: (bool,) =
                sqlx::query_as("SELECT EXISTS(SELECT 1 FROM articles WHERE dictionary = $1)")
                    .bind(dict_str)
                    .fetch_one(&self.db)
                    .await?;

            // Check if we've already queued the initial article fetch
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
                    warn!(
                        "[{:?}] No articles in DB, queueing initial article fetch.",
                        dict
                    );
                    self.enqueue_sync_articles(*dict).await?;
                }
            }

            // Check if we have dictionary metadata
            let has_concepts: Option<(String,)> = sqlx::query_as(
                "SELECT key FROM dictionary_metadata WHERE dictionary = $1 AND key = 'concepts'",
            )
            .bind(dict_str)
            .fetch_optional(&self.db)
            .await?;

            if has_concepts.is_none() {
                warn!("[{:?}] Missing dictionary metadata, queueing fetch", dict);
                self.enqueue_sync_dict_metadata(*dict).await?;
            }
        }
        Ok(())
    }

    /// Fetch the list of articles from UiB.
    async fn fetch_article_list_from_uib(&self, dict: UibDictionary) -> Result<Vec<Value>> {
        let url = format!("https://ord.uib.no/{}/fil/article.json", dict.as_str());
        let resp = reqwest::get(&url).await?.error_for_status()?;
        let data: Value = resp.json().await?;
        // Suppose the response is an array of arrays, each representing raw metadata
        // e.g. [ [articleId, lemma, revision, updatedAt], … ]
        let arr = data
            .as_array()
            .ok_or_else(|| anyhow!("Expected JSON array"))?;
        Ok(arr.clone())
    }

    /// Fetch a single article from UiB.
    async fn fetch_article_from_uib(&self, dict: UibDictionary, article_id: i64) -> Result<Value> {
        let url = format!(
            "https://ord.uib.no/{}/article/{}.json",
            dict.as_str(),
            article_id
        );
        let resp = reqwest::get(&url).await?.error_for_status()?;
        let data: Value = resp.json().await?;
        Ok(data)
    }

    /// Fetch dictionary concepts.
    async fn fetch_concepts(&self, dict: UibDictionary) -> Result<Value> {
        let url = format!("https://ord.uib.no/{}/concepts.json", dict.as_str());
        let resp = reqwest::get(&url).await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    /// Fetch dictionary word classes.
    async fn fetch_word_classes(&self, dict: UibDictionary) -> Result<Value> {
        let url = format!("https://ord.uib.no/{}/fil/word_class.json", dict.as_str());
        let resp = reqwest::get(&url).await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    /// Fetch dictionary word subclasses.
    async fn fetch_word_subclasses(&self, dict: UibDictionary) -> Result<Value> {
        let url = format!(
            "https://ord.uib.no/{}/fil/sub_word_class.json",
            dict.as_str()
        );
        let resp = reqwest::get(&url).await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    /// Convert raw article metadata to a struct.
    fn convert_raw_article_metadata(raw: Value) -> Result<ArticleMetadata> {
        // raw is e.g. [articleId, primaryLemma, revision, updatedAt]
        let arr = raw
            .as_array()
            .ok_or_else(|| anyhow!("Metadata is not an array"))?;
        let article_id = arr
            .first()
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("No articleId"))?;
        let lemma = arr
            .get(1)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let revision = Some(arr.get(2).and_then(|v| v.as_i64()).unwrap_or(0));
        let updated_at = arr
            .get(3)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(ArticleMetadata {
            article_id,
            primary_lemma: lemma,
            revision,
            updated_at,
        })
    }

    /// Store article data in JSON at "article:<dict>:<id>" plus the metadata in
    /// "dictionary:<dict>:articles" as a hash from articleId -> JSON.
    async fn store_article(
        &self,
        dict: UibDictionary,
        article_id: i64,
        article_data: &Value,
        primary_lemma: &str,
        revision: i64,
        updated_at: &str,
    ) -> Result<()> {
        let dict_str = dict.as_str();
        let bibl_ids: Vec<i64> = Self::extract_bibl_ids(article_data).into_iter().collect();
        let (dialect_place_ids, attestation_place_ids) = Self::extract_place_ids(article_data);
        let dialect_ids_vec: Vec<i64> = dialect_place_ids.into_iter().collect();
        let attestation_ids_vec: Vec<i64> = attestation_place_ids.into_iter().collect();
        let inline_refs = Self::extract_inline_refs(article_data);

        let mut tx = self.db.begin().await?;

        sqlx::query(
            "INSERT INTO articles (dictionary, id, data, primary_lemma, revision, updated_at, modified_at)
             VALUES ($1, $2, $3, $4, $5, $6, now())
             ON CONFLICT (dictionary, id) DO UPDATE
             SET data = $3, primary_lemma = $4, revision = $5, updated_at = $6, modified_at = now()",
        )
        .bind(dict_str)
        .bind(article_id)
        .bind(article_data)
        .bind(primary_lemma)
        .bind(revision)
        .bind(updated_at)
        .execute(&mut *tx)
        .await?;

        sqlx::query("DELETE FROM article_bibliography WHERE dictionary = $1 AND article_id = $2")
            .bind(dict_str)
            .bind(article_id)
            .execute(&mut *tx)
            .await?;

        if !bibl_ids.is_empty() {
            sqlx::query(
                "INSERT INTO article_bibliography (dictionary, article_id, bibl_id) \
                 SELECT $1, $2, unnest($3::bigint[]) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(dict_str)
            .bind(article_id)
            .bind(&bibl_ids)
            .execute(&mut *tx)
            .await?;
        }

        let (inline_resolved_bibl_ids, unresolved_codes) = self
            .store_inline_refs(&mut tx, dict_str, article_id, &inline_refs)
            .await?;
        if !inline_resolved_bibl_ids.is_empty() {
            sqlx::query(
                "INSERT INTO article_bibliography (dictionary, article_id, bibl_id) \
                 SELECT $1, $2, unnest($3::bigint[]) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(dict_str)
            .bind(article_id)
            .bind(&inline_resolved_bibl_ids)
            .execute(&mut *tx)
            .await?;
        }

        sqlx::query("DELETE FROM article_place WHERE dictionary = $1 AND article_id = $2")
            .bind(dict_str)
            .bind(article_id)
            .execute(&mut *tx)
            .await?;

        if !dialect_ids_vec.is_empty() {
            sqlx::query(
                "INSERT INTO article_place (dictionary, article_id, place_id, context) \
                 SELECT $1, $2, unnest($3::bigint[]), 'dialect' \
                 ON CONFLICT DO NOTHING",
            )
            .bind(dict_str)
            .bind(article_id)
            .bind(&dialect_ids_vec)
            .execute(&mut *tx)
            .await?;
        }

        if !attestation_ids_vec.is_empty() {
            sqlx::query(
                "INSERT INTO article_place (dictionary, article_id, place_id, context) \
                 SELECT $1, $2, unnest($3::bigint[]), 'attestation' \
                 ON CONFLICT DO NOTHING",
            )
            .bind(dict_str)
            .bind(article_id)
            .bind(&attestation_ids_vec)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        if !unresolved_codes.is_empty() {
            self.enqueue_resolve_inline_codes(&unresolved_codes).await?;
        }

        Ok(())
    }

    /// Index an article into Meilisearch.
    async fn index_article(
        &self,
        dict: UibDictionary,
        article_id: i64,
        article_data: &Value,
    ) -> Result<()> {
        let bib = self.load_article_bibliography(article_data).await?;
        let places = self.load_article_place_data(dict, article_id).await?;
        let doc = meili::build_search_document(
            dict.as_str(),
            article_id,
            article_data,
            Some(&bib),
            Some(&places),
        );
        let idx = self.meili.index(meili::index_name(dict.as_str()));

        idx.add_or_replace(&[doc], Some("id"))
            .await
            .map_err(|e| anyhow!("Failed to index article in Meilisearch: {e}"))?;

        Ok(())
    }

    /// Load place metadata for an article, split by context.
    async fn load_article_place_data(
        &self,
        dict: UibDictionary,
        article_id: i64,
    ) -> Result<meili::ArticlePlaceData> {
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT place_id, context FROM article_place WHERE dictionary = $1 AND article_id = $2",
        )
        .bind(dict.as_str())
        .bind(article_id)
        .fetch_all(&self.db)
        .await?;

        if rows.is_empty() {
            return Ok(meili::ArticlePlaceData::default());
        }

        let mut dialect_ids = Vec::new();
        let mut attestation_ids = Vec::new();
        let mut all_ids = Vec::new();
        for (pid, ctx) in &rows {
            all_ids.push(*pid);
            match ctx.as_str() {
                "dialect" => dialect_ids.push(*pid),
                "attestation" => attestation_ids.push(*pid),
                _ => {}
            }
        }

        let unique_ids: Vec<i64> = all_ids
            .iter()
            .copied()
            .collect::<HashSet<i64>>()
            .into_iter()
            .collect();
        let place_rows: Vec<(i64, String, String, String)> = sqlx::query_as(
            "SELECT id, place_name, place_name_full, place_type FROM places WHERE id = ANY($1)",
        )
        .bind(&unique_ids)
        .fetch_all(&self.db)
        .await?;

        let place_map: std::collections::HashMap<i64, (String, String, String)> = place_rows
            .into_iter()
            .map(|(id, name, full_name, place_type)| (id, (name, full_name, place_type)))
            .collect();

        Ok(meili::build_article_place_data_split(
            &dialect_ids,
            &attestation_ids,
            &place_map,
        ))
    }

    /// Load bibliography metadata for an article.
    async fn load_article_bibliography(
        &self,
        article_data: &Value,
    ) -> Result<meili::ArticleBibliography> {
        let bibl_ids = Self::extract_bibl_ids(article_data);
        if bibl_ids.is_empty() {
            return Ok(meili::ArticleBibliography {
                older_source: meili::BibliographyCategory {
                    codes: vec![],
                    authors: vec![],
                    titles: vec![],
                    years: vec![],
                },
                written_form_source: meili::BibliographyCategory {
                    codes: vec![],
                    authors: vec![],
                    titles: vec![],
                    years: vec![],
                },
                attestation_source: meili::BibliographyCategory {
                    codes: vec![],
                    authors: vec![],
                    titles: vec![],
                    years: vec![],
                },
                all: meili::BibliographyCategory {
                    codes: vec![],
                    authors: vec![],
                    titles: vec![],
                    years: vec![],
                },
            });
        }

        let ids_vec: Vec<i64> = bibl_ids.into_iter().collect();
        let rows: Vec<(i64, String, String, String, String)> = sqlx::query_as(
            "SELECT id, code, author, title, year FROM bibliography WHERE id = ANY($1)",
        )
        .bind(&ids_vec)
        .fetch_all(&self.db)
        .await?;

        let bib_map: std::collections::HashMap<i64, (String, String, String, String)> = rows
            .into_iter()
            .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
            .collect();

        Ok(meili::build_article_bibliography(article_data, &bib_map))
    }

    /// Retrieve all known articles from PostgreSQL.
    async fn get_all_article_metadata(
        &self,
        dict: UibDictionary,
    ) -> Result<HashMap<i64, ArticleMetadata>> {
        let dict_str = dict.as_str();

        let rows: Vec<(i64, String, i64, String)> = sqlx::query_as(
            "SELECT id, primary_lemma, revision, updated_at FROM articles WHERE dictionary = $1",
        )
        .bind(dict_str)
        .fetch_all(&self.db)
        .await?;

        let mut map = HashMap::new();
        for (id, primary_lemma, revision, updated_at) in rows {
            map.insert(
                id,
                ArticleMetadata {
                    article_id: id,
                    primary_lemma,
                    revision: Some(revision),
                    updated_at,
                },
            );
        }
        Ok(map)
    }

    /// Gets the first lemma from the article JSON. Defaults to an empty string if not found.
    fn find_first_lemma(article_json: &Value) -> String {
        article_json
            .get("lemmas")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|lemma| lemma.get("lemma"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    }

    /// Extract all bibl_id values from an article.
    fn extract_bibl_ids(article: &Value) -> HashSet<i64> {
        let mut ids = HashSet::new();
        Self::collect_bibl_ids(article, &mut ids);
        ids
    }

    fn collect_bibl_ids(value: &Value, ids: &mut HashSet<i64>) {
        match value {
            Value::Object(map) => {
                if let Some(bibl_id) = map.get("bibl_id").and_then(|v| v.as_i64()) {
                    ids.insert(bibl_id);
                }
                for v in map.values() {
                    Self::collect_bibl_ids(v, ids);
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    Self::collect_bibl_ids(v, ids);
                }
            }
            _ => {}
        }
    }

    /// Enqueue bibliography fetch jobs for any bibliography IDs in an article
    /// that are not already present in the database.
    async fn enqueue_bibliography_for_article(&self, article: &Value) -> Result<()> {
        let bibl_ids = Self::extract_bibl_ids(article);
        if bibl_ids.is_empty() {
            return Ok(());
        }
        self.enqueue_bibliography_ids(&bibl_ids).await
    }

    /// Enqueue bibliography fetch jobs for IDs not yet in the database.
    pub async fn enqueue_bibliography_ids(&self, bibl_ids: &HashSet<i64>) -> Result<()> {
        if self.clarino_api_key.is_none() {
            debug!("No CLARINO_API_KEY set, skipping bibliography enqueue.");
            return Ok(());
        }

        let ids_vec: Vec<i64> = bibl_ids.iter().copied().collect();
        let existing: Vec<(i64,)> =
            sqlx::query_as("SELECT id FROM bibliography WHERE id = ANY($1)")
                .bind(&ids_vec)
                .fetch_all(&self.db)
                .await?;

        let existing_set: HashSet<i64> = existing.into_iter().map(|(id,)| id).collect();
        let missing: Vec<i64> = ids_vec
            .into_iter()
            .filter(|id| !existing_set.contains(id))
            .collect();

        if missing.is_empty() {
            return Ok(());
        }

        let mut storage = self.fetch_bibliography_storage.clone();
        let mut enqueued = 0usize;
        for bibl_id in missing {
            if self.try_mark_bibl_queued(bibl_id).await? {
                storage.push(FetchBibliographyJob { bibl_id }).await?;
                enqueued += 1;
            }
        }

        if enqueued > 0 {
            info!("Enqueued {enqueued} bibliography fetch jobs");
        }

        Ok(())
    }

    /// Enqueue bibliography jobs for all articles.
    pub async fn enqueue_initial_bibliography_sync(&self) -> Result<()> {
        if self.clarino_api_key.is_none() {
            info!("No CLARINO_API_KEY set, skipping initial bibliography sync.");
            return Ok(());
        }

        let rows: Vec<(i64,)> = sqlx::query_as("SELECT DISTINCT bibl_id FROM article_bibliography")
            .fetch_all(&self.db)
            .await?;

        let all_bibl_ids: HashSet<i64> = rows.into_iter().map(|(id,)| id).collect();

        if all_bibl_ids.is_empty() {
            info!("No bibliography IDs found in articles.");
            return Ok(());
        }

        info!(
            "Found {} unique bibliography IDs across all articles, enqueueing missing entries…",
            all_bibl_ids.len()
        );

        self.enqueue_bibliography_ids(&all_bibl_ids).await?;

        info!("Initial bibliography sync jobs enqueued.");
        Ok(())
    }

    /// Enqueue backfill jobs for inline bibliography parse data for existing
    /// Norsk Ordbok articles.
    pub async fn enqueue_inline_refs_backfill(&self) -> Result<()> {
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

        info!("Enqueueing inline reference backfill jobs for Norsk Ordbok articles…");

        let mut stream = sqlx::query_as::<_, (i64,)>(
            "SELECT id FROM articles WHERE dictionary = 'no' ORDER BY id",
        )
        .fetch(&self.db);

        let mut batch: Vec<i64> = Vec::with_capacity(1000);
        let mut count = 0usize;

        while let Some(row) = stream.next().await {
            let (article_id,) = row?;
            batch.push(article_id);

            if batch.len() >= 1000 {
                self.push_backfill_batch(&batch).await?;
                count += batch.len();
                batch.clear();
            }
        }

        if !batch.is_empty() {
            count += batch.len();
            self.push_backfill_batch(&batch).await?;
        }

        sqlx::query(
            "INSERT INTO sync_state (dictionary, key, value) \
             VALUES ('no', 'inline_refs_backfill_enqueued', '1') \
             ON CONFLICT (dictionary, key) DO NOTHING",
        )
        .execute(&self.db)
        .await?;

        info!("Enqueued {count} inline reference backfill jobs.");

        Ok(())
    }

    async fn push_backfill_batch(&self, ids: &[i64]) -> Result<()> {
        let mut storage = self.backfill_inline_refs_storage.clone();
        storage
            .push(BackfillInlineRefsJob {
                article_ids: ids.to_vec(),
            })
            .await
            .map_err(|e| anyhow!("Failed to enqueue backfill batch: {e}"))?;
        Ok(())
    }

    /// Enqueue resolve jobs for unresolved inline codes.
    async fn enqueue_resolve_inline_codes(&self, codes: &[String]) -> Result<()> {
        let mut storage = self.resolve_inline_code_storage.clone();
        for code in codes {
            let key = format!("ordbokapi:resolve-code-queued:{code}");
            let result: Option<String> = redis::cmd("SET")
                .arg(&key)
                .arg("1")
                .arg("NX")
                .arg("EX")
                .arg(Self::DEDUPE_TTL)
                .query_async(&mut self.redis_conn.clone())
                .await?;

            if result.is_some() {
                storage
                    .push(ResolveInlineCodeJob { code: code.clone() })
                    .await
                    .map_err(|e| anyhow!("Failed to enqueue resolve job: {e}"))?;
            }
        }
        Ok(())
    }

    /// Handle an inline reference backfill job for one or more articles.
    pub async fn handle_backfill_inline_refs(&self, article_ids: &[i64]) -> Result<()> {
        let rows: Vec<(i64, Value)> = sqlx::query_as(
            "SELECT id, data FROM articles WHERE dictionary = 'no' AND id = ANY($1)",
        )
        .bind(article_ids)
        .fetch_all(&self.db)
        .await?;

        let mut all_unresolved: HashSet<String> = HashSet::new();
        let mut tx = self.db.begin().await?;

        for (article_id, data) in &rows {
            let refs = Self::extract_inline_refs(data);
            if refs.is_empty() {
                continue;
            }

            let (resolved_ids, unresolved_codes) = self
                .store_inline_refs(&mut tx, "no", *article_id, &refs)
                .await?;

            if !resolved_ids.is_empty() {
                sqlx::query(
                    "INSERT INTO article_bibliography (dictionary, article_id, bibl_id) \
                     SELECT 'no', $1, unnest($2::bigint[]) \
                     ON CONFLICT DO NOTHING",
                )
                .bind(article_id)
                .bind(&resolved_ids)
                .execute(&mut *tx)
                .await?;
            }

            all_unresolved.extend(unresolved_codes);
        }

        tx.commit().await?;

        if !all_unresolved.is_empty() {
            let codes: Vec<String> = all_unresolved.into_iter().collect();
            self.enqueue_resolve_inline_codes(&codes).await?;
        }

        let count = self
            .inline_refs_backfill_count
            .fetch_add(article_ids.len() as u64, Ordering::Relaxed)
            + article_ids.len() as u64;
        self.log_backfill_progress(count);

        Ok(())
    }

    fn log_backfill_progress(&self, count: u64) {
        if count.is_multiple_of(5000) {
            info!("{count} articles backfilled with inline reference data.");
        }
    }

    /// Attempt to resolve an unresolved inline code.
    pub async fn handle_resolve_inline_code(&self, code: &str) -> Result<()> {
        let api_key = match self.clarino_api_key.as_deref() {
            Some(k) => k,
            None => {
                debug!("No CLARINO_API_KEY set, cannot resolve inline code '{code}'.");
                return Ok(());
            }
        };

        let still_unresolved: Option<(i32,)> = sqlx::query_as(
            "SELECT 1 FROM inline_ref_parse WHERE code = $1 AND ref_type IS NULL LIMIT 1",
        )
        .bind(code)
        .fetch_optional(&self.db)
        .await?;

        if still_unresolved.is_none() {
            return Ok(());
        }

        if let Ok(bibl_entries) = self.fetch_bibl_by_code(api_key, code).await
            && let Some(entry) = bibl_entries.first()
            && let Some(bibl_id) = entry.get("bibl_id").and_then(|v| v.as_i64())
        {
            let exists: Option<(i64,)> =
                sqlx::query_as("SELECT id FROM bibliography WHERE id = $1")
                    .bind(bibl_id)
                    .fetch_optional(&self.db)
                    .await?;

            if exists.is_none() {
                self.store_bibliography_entry(bibl_id, entry).await?;
                self.index_bibliography_entry(bibl_id, entry).await?;
            }

            self.resolve_inline_ref_as_bibl(bibl_id, code).await?;
            info!("Resolved inline code {code} as bibliography ID {bibl_id}.");
            return Ok(());
        }

        let place_name = if let Some(stripped) = code.strip_suffix('M') {
            stripped
        } else {
            code
        };

        if let Ok(Some((place_id, entry))) = self.fetch_place_by_name(api_key, place_name).await {
            let exists: Option<(i64,)> = sqlx::query_as("SELECT id FROM places WHERE id = $1")
                .bind(place_id)
                .fetch_optional(&self.db)
                .await?;

            if exists.is_none() {
                self.store_place_entry(place_id, &entry).await?;
                self.index_place_entry(place_id, &entry).await?;

                let child_ids = Self::extract_child_place_ids(&entry);
                if !child_ids.is_empty() {
                    self.enqueue_place_ids(&child_ids).await?;
                }
            }

            self.resolve_inline_place_by_name(place_id, place_name)
                .await?;
            info!("Resolved inline code {code} as place {place_name}, ID {place_id}.");
            return Ok(());
        }

        if place_name != code
            && let Ok(Some((place_id, entry))) = self.fetch_place_by_name(api_key, code).await
        {
            let exists: Option<(i64,)> = sqlx::query_as("SELECT id FROM places WHERE id = $1")
                .bind(place_id)
                .fetch_optional(&self.db)
                .await?;

            if exists.is_none() {
                self.store_place_entry(place_id, &entry).await?;
                self.index_place_entry(place_id, &entry).await?;

                let child_ids = Self::extract_child_place_ids(&entry);
                if !child_ids.is_empty() {
                    self.enqueue_place_ids(&child_ids).await?;
                }
            }

            self.resolve_inline_place_by_name(place_id, code).await?;
            info!("Resolved inline code '{code}' as place (place_id {place_id}).");
            return Ok(());
        }

        debug!("Could not resolve inline code '{code}' as bibliography or place.");
        Ok(())
    }

    /// Fetch bibliography entries by code.
    async fn fetch_bibl_by_code(&self, api_key: &str, code: &str) -> Result<Vec<Value>> {
        let encoded = urlencoding::encode(code);
        let url = format!("https://clarino.uib.no/ordbank-api-prod/bibl?code={encoded}");
        let resp = reqwest::Client::new()
            .get(&url)
            .header("x-api-key", api_key)
            .send()
            .await?
            .error_for_status()?;
        let data: Vec<Value> = resp.json().await?;
        Ok(data)
    }

    /// Fetch places by name.
    async fn fetch_place_by_name(&self, api_key: &str, name: &str) -> Result<Option<(i64, Value)>> {
        let encoded = urlencoding::encode(name);
        let url = format!("https://clarino.uib.no/ordbank-api-prod/place?place_name={encoded}");
        let resp = reqwest::Client::new()
            .get(&url)
            .header("x-api-key", api_key)
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let resp = resp.error_for_status()?;
        let data: Value = resp.json().await?;

        if let Some(obj) = data.as_object()
            && let Some((id_str, entry)) = obj.iter().next()
            && let Ok(place_id) = id_str.parse::<i64>()
        {
            return Ok(Some((place_id, entry.clone())));
        }

        Ok(None)
    }

    /// Handle a single bibliography fetch job.
    pub async fn handle_sync_bibliography(&self, bibl_id: i64) -> Result<()> {
        let api_key = self
            .clarino_api_key
            .as_deref()
            .ok_or_else(|| anyhow!("CLARINO_API_KEY not set"))?;

        let entry = self.fetch_bibliography_entry(api_key, bibl_id).await?;
        self.store_bibliography_entry(bibl_id, &entry).await?;
        self.index_bibliography_entry(bibl_id, &entry).await?;

        // Resolve any pending inline refs that match this entry's code.
        let code = entry.get("code").and_then(|v| v.as_str()).unwrap_or("");
        if !code.is_empty() {
            self.resolve_inline_ref_as_bibl(bibl_id, code).await?;
        }

        // Re-index articles that reference this bibliography ID so their
        // bibliography fields are up to date.
        self.reindex_articles_for_bibl(bibl_id).await?;

        info!("Bibliography entry {bibl_id} synced");
        Ok(())
    }

    /// Re-index all articles that reference a given bibliography ID.
    async fn reindex_articles_for_bibl(&self, bibl_id: i64) -> Result<()> {
        let rows: Vec<(String, i64, Value)> = sqlx::query_as(
            "SELECT a.dictionary, a.id, a.data FROM articles a \
             INNER JOIN article_bibliography ab \
             ON a.dictionary = ab.dictionary AND a.id = ab.article_id \
             WHERE ab.bibl_id = $1",
        )
        .bind(bibl_id)
        .fetch_all(&self.db)
        .await?;

        if rows.is_empty() {
            return Ok(());
        }

        info!(
            "Re-indexing {} articles for bibliography {bibl_id}",
            rows.len()
        );

        for (dict_str, article_id, data) in &rows {
            let dict = Self::parse_dict(dict_str)?;
            if let Err(e) = self.index_article(dict, *article_id, data).await {
                warn!("Failed to re-index article {article_id} for bibl {bibl_id}: {e}");
            }
        }

        Ok(())
    }

    /// Mark articles affected by a place for deferred batch reindex.
    async fn mark_articles_for_reindex(&self, place_id: i64) -> Result<()> {
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT dictionary, article_id FROM article_place WHERE place_id = $1")
                .bind(place_id)
                .fetch_all(&self.db)
                .await?;

        if rows.is_empty() {
            return Ok(());
        }

        let members: Vec<String> = rows
            .iter()
            .map(|(dict, id)| format!("{dict}:{id}"))
            .collect();

        redis::cmd("SADD")
            .arg(Self::PENDING_REINDEX_KEY)
            .arg(&members)
            .query_async::<i64>(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Failed to SADD pending reindex: {e}"))?;

        Ok(())
    }

    /// Schedule a drain job if one isn't already queued.
    async fn schedule_drain_reindex(&self) -> Result<()> {
        let key = "ordbokapi:drain-reindex-queued";
        let result: Option<String> = redis::cmd("SET")
            .arg(key)
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(300u64)
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Redis SET NX failed: {e}"))?;

        if result.is_some() {
            let mut storage = self.drain_pending_reindex_storage.clone();
            storage
                .push(DrainPendingReindexJob)
                .await
                .map_err(|e| anyhow!("Failed to enqueue DrainPendingReindex: {e}"))?;
        }

        Ok(())
    }

    /// Batch reindex articles that have pending place updates.
    pub async fn handle_drain_pending_reindex(&self) -> Result<()> {
        let place_queue_active = self
            .fetch_place_storage
            .clone()
            .fetch_by_queue()
            .await
            .map(|stats| {
                stats
                    .iter()
                    .filter(|st| st.title == "PENDING_JOBS" || st.title == "RUNNING_JOBS")
                    .filter_map(|st| st.value.parse::<i64>().ok())
                    .sum::<i64>()
            })
            .unwrap_or(0);

        if place_queue_active > 0 {
            info!("Place queue still has {place_queue_active} jobs, deferring drain reindex.");
            // Clear dedup key so schedule_drain_reindex can enqueue.
            redis::cmd("DEL")
                .arg("ordbokapi:drain-reindex-queued")
                .query_async::<i64>(&mut self.redis_conn.clone())
                .await
                .ok();
            return Ok(());
        }

        redis::cmd("DEL")
            .arg("ordbokapi:drain-reindex-queued")
            .query_async::<i64>(&mut self.redis_conn.clone())
            .await
            .ok();

        let members: Vec<String> = redis::cmd("SMEMBERS")
            .arg(Self::PENDING_REINDEX_KEY)
            .query_async(&mut self.redis_conn.clone())
            .await
            .map_err(|e| anyhow!("Failed to SMEMBERS pending reindex: {e}"))?;

        if members.is_empty() {
            return Ok(());
        }

        redis::cmd("DEL")
            .arg(Self::PENDING_REINDEX_KEY)
            .query_async::<i64>(&mut self.redis_conn.clone())
            .await
            .ok();

        let mut by_dict: HashMap<String, Vec<i64>> = HashMap::new();
        for member in &members {
            if let Some((dict, id_str)) = member.split_once(':')
                && let Ok(id) = id_str.parse::<i64>()
            {
                by_dict.entry(dict.to_string()).or_default().push(id);
            }
        }

        let total: usize = by_dict.values().map(|v| v.len()).sum();
        info!(
            "Batch reindexing {total} articles across {} dictionaries",
            by_dict.len(),
        );

        let all_article_keys: Vec<(String, i64)> = by_dict
            .iter()
            .flat_map(|(dict, ids)| ids.iter().map(move |id| (dict.clone(), *id)))
            .collect();

        let mut article_place_map: HashMap<(String, i64), (Vec<i64>, Vec<i64>)> = HashMap::new();
        let mut needed_place_ids: HashSet<i64> = HashSet::new();

        for chunk in all_article_keys.chunks(5000) {
            let dict_ids: Vec<&str> = chunk.iter().map(|(d, _)| d.as_str()).collect();
            let art_ids: Vec<i64> = chunk.iter().map(|(_, id)| *id).collect();

            let rows: Vec<(String, i64, i64, String)> = sqlx::query_as(
                "SELECT ap.dictionary, ap.article_id, ap.place_id, ap.context \
                 FROM article_place ap \
                 WHERE (ap.dictionary, ap.article_id) IN \
                 (SELECT * FROM UNNEST($1::text[], $2::bigint[]))",
            )
            .bind(&dict_ids)
            .bind(&art_ids)
            .fetch_all(&self.db)
            .await?;

            for (dict, article_id, place_id, context) in rows {
                needed_place_ids.insert(place_id);
                let entry = article_place_map.entry((dict, article_id)).or_default();
                match context.as_str() {
                    "dialect" => entry.0.push(place_id),
                    "attestation" => entry.1.push(place_id),
                    _ => {}
                }
            }
        }

        let place_id_vec: Vec<i64> = needed_place_ids.into_iter().collect();
        let place_rows: Vec<(i64, String, String, String)> = sqlx::query_as(
            "SELECT id, place_name, place_name_full, place_type FROM places \
             WHERE id = ANY($1::bigint[])",
        )
        .bind(&place_id_vec)
        .fetch_all(&self.db)
        .await?;

        let place_map: HashMap<i64, (String, String, String)> = place_rows
            .into_iter()
            .map(|(id, name, full_name, ptype)| (id, (name, full_name, ptype)))
            .collect();

        let bib_rows: Vec<(i64, String, String, String, String)> =
            sqlx::query_as("SELECT id, code, author, title, year FROM bibliography")
                .fetch_all(&self.db)
                .await?;

        let bib_map: HashMap<i64, (String, String, String, String)> = bib_rows
            .into_iter()
            .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
            .collect();

        for (dict, ids) in &by_dict {
            let idx = self.meili.index(meili::index_name(dict));

            let mut batch: Vec<meili::ArticleSearchDocument> = Vec::with_capacity(5000);
            let mut tasks = Vec::new();

            for chunk in ids.chunks(5000) {
                let rows: Vec<(i64, Value)> = sqlx::query_as(
                    "SELECT id, data FROM articles WHERE dictionary = $1 AND id = ANY($2::bigint[])",
                )
                .bind(dict.as_str())
                .bind(chunk)
                .fetch_all(&self.db)
                .await?;

                for (id, data) in &rows {
                    let bib = meili::build_article_bibliography(data, &bib_map);
                    let (dialect_ids, attestation_ids) = article_place_map
                        .get(&(dict.clone(), *id))
                        .cloned()
                        .unwrap_or_default();
                    let places = meili::build_article_place_data_split(
                        &dialect_ids,
                        &attestation_ids,
                        &place_map,
                    );
                    batch.push(meili::build_search_document(
                        dict,
                        *id,
                        data,
                        Some(&bib),
                        Some(&places),
                    ));

                    if batch.len() >= 5000 {
                        tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
                        batch.clear();
                    }
                }
            }

            if !batch.is_empty() {
                tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
            }

            let timeout = Some(std::time::Duration::from_secs(300));
            for task in tasks {
                task.wait_for_completion(&self.meili, None, timeout).await?;
            }
        }

        info!("Batch reindex complete.");

        Ok(())
    }

    /// Fetch a single bibliography entry.
    async fn fetch_bibliography_entry(&self, api_key: &str, bibl_id: i64) -> Result<Value> {
        let url = format!("https://clarino.uib.no/ordbank-api-prod/bibl/{bibl_id}");
        let resp = reqwest::Client::new()
            .get(&url)
            .header("x-api-key", api_key)
            .send()
            .await?
            .error_for_status()?;
        let data: Value = resp.json().await?;
        let entry = data
            .as_array()
            .and_then(|arr| arr.first())
            .cloned()
            .ok_or_else(|| anyhow!("Empty response for bibliography {bibl_id}"))?;
        Ok(entry)
    }

    /// Store a bibliography entry in PostgreSQL.
    async fn store_bibliography_entry(&self, bibl_id: i64, entry: &Value) -> Result<()> {
        let code = entry.get("code").and_then(|v| v.as_str()).unwrap_or("");
        let author = entry.get("author").and_then(|v| v.as_str()).unwrap_or("");
        let title = entry.get("title").and_then(|v| v.as_str()).unwrap_or("");
        let year = entry.get("year").and_then(|v| v.as_str()).unwrap_or("");
        let empty_arr = Value::Array(vec![]);
        let fields = entry.get("fields").unwrap_or(&empty_arr);

        sqlx::query(
            "INSERT INTO bibliography (id, code, author, title, year, fields, fetched_at)
             VALUES ($1, $2, $3, $4, $5, $6, now())
             ON CONFLICT (id) DO UPDATE
             SET code = $2, author = $3, title = $4, year = $5, fields = $6, fetched_at = now()",
        )
        .bind(bibl_id)
        .bind(code)
        .bind(author)
        .bind(title)
        .bind(year)
        .bind(fields)
        .execute(&self.db)
        .await?;

        Ok(())
    }

    /// Index a bibliography entry in Meilisearch.
    async fn index_bibliography_entry(&self, bibl_id: i64, entry: &Value) -> Result<()> {
        let doc = meili::build_bibliography_document(bibl_id, entry);
        let idx = self.meili.index(meili::BIBLIOGRAPHY_INDEX);

        idx.add_or_replace(&[doc], Some("id"))
            .await
            .map_err(|e| anyhow!("Failed to index bibliography in Meilisearch: {e}"))?;

        Ok(())
    }

    /// After we've synced an article, queue any related articles from sub_article or article_ref
    /// fields in the JSON.
    async fn queue_related_articles(
        &self,
        dict: UibDictionary,
        _article_id: i64,
        article: &Value,
    ) -> Result<()> {
        let mut related_ids = Vec::new();
        Self::find_related_article_ids(article, &mut related_ids);

        if related_ids.is_empty() {
            return Ok(());
        }

        let dict_str = dict.as_str();

        for rid in related_ids {
            let exists: Option<(i64,)> =
                sqlx::query_as("SELECT id FROM articles WHERE dictionary = $1 AND id = $2")
                    .bind(dict_str)
                    .bind(rid)
                    .fetch_optional(&self.db)
                    .await?;

            if exists.is_some() {
                continue;
            }

            if !self.try_mark_article_queued(dict_str, rid).await? {
                debug!(
                    "[{:?}] Related article {} already queued, skipping.",
                    dict, rid
                );
                continue;
            }

            let partial = ArticleMetadata {
                article_id: rid,
                primary_lemma: String::new(),
                revision: None,
                updated_at: String::new(),
            };
            self.enqueue_sync_article(dict, &partial).await?;
        }

        Ok(())
    }

    /// Recursively scan JSON for article_ref or sub_article entries.
    fn find_related_article_ids(value: &Value, ids: &mut Vec<i64>) {
        match value {
            Value::Object(map) => {
                let type_field = map.get("type_").and_then(|v| v.as_str());
                if let Some(t) = type_field
                    && (t == "article_ref" || t == "sub_article")
                    && let Some(aid) = map.get("article_id").and_then(|v| v.as_i64())
                    && !ids.contains(&aid)
                {
                    ids.push(aid);
                }
                for v in map.values() {
                    Self::find_related_article_ids(v, ids);
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    Self::find_related_article_ids(v, ids);
                }
            }
            _ => {}
        }
    }

    /// Extract all place IDs from an article.
    fn extract_place_ids(article: &Value) -> (HashSet<i64>, HashSet<i64>) {
        let dialect_ids = Self::extract_dialect_place_ids(article);
        let attestation_ids = Self::extract_attestation_place_ids(article);
        (dialect_ids, attestation_ids)
    }

    /// Extract place IDs from dialect sources.
    fn extract_dialect_place_ids(article: &Value) -> HashSet<i64> {
        article
            .pointer("/body/dialect")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .flat_map(|d| {
                d.get("subcats")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
            })
            .flat_map(|sc| {
                sc.get("forms")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
            })
            .flat_map(|f| {
                f.get("sources")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
            })
            .filter_map(|s| s.get("place_id").and_then(|v| v.as_i64()))
            .collect()
    }

    /// Extract place IDs from attestation place_refs.
    fn extract_attestation_place_ids(article: &Value) -> HashSet<i64> {
        article
            .pointer("/body/definitions")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .flat_map(|def| {
                def.get("elements")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
            })
            .flat_map(|elem| {
                elem.get("place_refs")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
            })
            .filter_map(|pr| pr.get("place")?.get("place_id")?.as_i64())
            .collect()
    }

    /// Enqueue place fetch jobs for any place IDs in an article that are not
    /// already present in the database.
    async fn enqueue_places_for_article(&self, dict: UibDictionary, article: &Value) -> Result<()> {
        if dict != UibDictionary::NorskOrdbok {
            return Ok(());
        }

        let (dialect_ids, attestation_ids) = Self::extract_place_ids(article);
        let place_ids: HashSet<i64> = dialect_ids.union(&attestation_ids).copied().collect();
        if place_ids.is_empty() {
            return Ok(());
        }
        self.enqueue_place_ids(&place_ids).await
    }

    /// Enqueue place fetch jobs for IDs not yet in the database.
    pub async fn enqueue_place_ids(&self, place_ids: &HashSet<i64>) -> Result<()> {
        if self.clarino_api_key.is_none() {
            debug!("No CLARINO_API_KEY set, skipping place enqueue.");
            return Ok(());
        }

        let ids_vec: Vec<i64> = place_ids.iter().copied().collect();
        let existing: Vec<(i64,)> = sqlx::query_as("SELECT id FROM places WHERE id = ANY($1)")
            .bind(&ids_vec)
            .fetch_all(&self.db)
            .await?;

        let existing_set: HashSet<i64> = existing.into_iter().map(|(id,)| id).collect();
        let missing: Vec<i64> = ids_vec
            .into_iter()
            .filter(|id| !existing_set.contains(id))
            .collect();

        if missing.is_empty() {
            return Ok(());
        }

        let mut storage = self.fetch_place_storage.clone();
        let mut enqueued = 0usize;
        for place_id in missing {
            if self.try_mark_place_queued(place_id).await? {
                storage.push(FetchPlaceJob { place_id }).await?;
                enqueued += 1;
            }
        }

        if enqueued > 0 {
            info!("Enqueued {enqueued} place fetch jobs");
        }

        Ok(())
    }

    /// Enqueue place jobs for all articles.
    pub async fn enqueue_initial_place_sync(&self) -> Result<()> {
        if self.clarino_api_key.is_none() {
            info!("No CLARINO_API_KEY set, skipping initial place sync.");
            return Ok(());
        }

        let rows: Vec<(i64,)> = sqlx::query_as("SELECT DISTINCT place_id FROM article_place")
            .fetch_all(&self.db)
            .await?;

        let all_place_ids: HashSet<i64> = rows.into_iter().map(|(id,)| id).collect();

        if all_place_ids.is_empty() {
            info!("No place IDs found in articles.");
            return Ok(());
        }

        info!(
            "Found {} unique place IDs across all articles, enqueueing missing entries…",
            all_place_ids.len()
        );

        self.enqueue_place_ids(&all_place_ids).await?;

        info!("Initial place sync jobs enqueued.");
        Ok(())
    }

    /// Handle a single place fetch job.
    pub async fn handle_sync_place(&self, place_id: i64) -> Result<()> {
        let api_key = self
            .clarino_api_key
            .as_deref()
            .ok_or_else(|| anyhow!("CLARINO_API_KEY not set"))?;

        let entry = self.fetch_place_entry(api_key, place_id).await?;
        self.store_place_entry(place_id, &entry).await?;
        self.index_place_entry(place_id, &entry).await?;

        let child_ids = Self::extract_child_place_ids(&entry);
        if !child_ids.is_empty() {
            self.enqueue_place_ids(&child_ids).await?;
        }

        let place_name = entry
            .get("place_name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !place_name.is_empty() {
            self.resolve_inline_place_by_name(place_id, place_name)
                .await?;
        }

        self.mark_articles_for_reindex(place_id).await?;
        self.schedule_drain_reindex().await?;

        info!("Place entry {place_id} synced");
        Ok(())
    }

    /// Extract children place IDs from the place API response.
    fn extract_child_place_ids(entry: &Value) -> HashSet<i64> {
        let mut ids = HashSet::new();
        if let Some(children) = entry.get("child_places").and_then(|v| v.as_array()) {
            for child in children {
                if let Some(pid) = child.get("place_id").and_then(|v| v.as_i64()) {
                    ids.insert(pid);
                }
            }
        }
        ids
    }

    /// Fetch a single place entry.
    async fn fetch_place_entry(&self, api_key: &str, place_id: i64) -> Result<Value> {
        let url = format!("https://clarino.uib.no/ordbank-api-prod/place/{place_id}");
        let resp = reqwest::Client::new()
            .get(&url)
            .header("x-api-key", api_key)
            .send()
            .await?
            .error_for_status()?;
        let data: Value = resp.json().await?;
        let entry = data
            .get(place_id.to_string())
            .cloned()
            .ok_or_else(|| anyhow!("Empty response for place {place_id}"))?;
        Ok(entry)
    }

    /// Index a place entry in Meilisearch.
    async fn index_place_entry(&self, place_id: i64, entry: &Value) -> Result<()> {
        let doc = meili::PlaceSearchDocument {
            id: place_id,
            place_name: entry
                .get("place_name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            place_name_full: entry
                .get("place_name_full")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            place_type: entry
                .get("place_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            parent_id: entry.get("parent_id").and_then(|v| v.as_i64()),
            municipality_nr: entry
                .get("municipality_nr")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        let idx = self.meili.index(meili::PLACE_INDEX);
        idx.add_or_replace(&[doc], Some("id"))
            .await
            .map_err(|e| anyhow!("Failed to index place in Meilisearch: {e}"))?;

        Ok(())
    }

    /// Store a place entry in PostgreSQL.
    async fn store_place_entry(&self, place_id: i64, entry: &Value) -> Result<()> {
        let place_name = entry
            .get("place_name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let place_name_full = entry
            .get("place_name_full")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let place_type = entry
            .get("place_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let parent_id = entry.get("parent_id").and_then(|v| v.as_i64());
        let place_order = entry
            .get("place_order")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;
        let municipality_nr = entry
            .get("municipality_nr")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let weight_threshold = entry
            .get("weight_threshold")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        sqlx::query(
            "INSERT INTO places (id, place_name, place_name_full, place_type, parent_id, place_order, municipality_nr, weight_threshold, fetched_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
             ON CONFLICT (id) DO UPDATE
             SET place_name = $2, place_name_full = $3, place_type = $4, parent_id = $5, place_order = $6, municipality_nr = $7, weight_threshold = $8, fetched_at = now()",
        )
        .bind(place_id)
        .bind(place_name)
        .bind(place_name_full)
        .bind(place_type)
        .bind(parent_id)
        .bind(place_order)
        .bind(municipality_nr)
        .bind(weight_threshold)
        .execute(&self.db)
        .await?;

        Ok(())
    }
}

/// A parsed inline bibliography reference found in example quote text.
#[derive(Debug, Clone, PartialEq)]
struct InlineRef {
    /// The full quote.content string containing the inline ref.
    quote_content: String,
    /// Byte offset where the opening `(` is in quote_content.
    offset_start: usize,
    /// Byte offset after the closing `)` in quote_content.
    offset_end: usize,
    /// The parsed bibliography code.
    code: String,
    /// The spec/page portion, if any.
    spec: Option<String>,
}

/// Matches inline bibliography references in text.
static INLINE_REF_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:\S| )\(([^)]+)\)").unwrap());

impl ArticleSyncService {
    /// Extract inline bibliography references from all example quotes in an article.
    fn extract_inline_refs(article: &Value) -> Vec<InlineRef> {
        let mut refs = Vec::new();
        let body = match article.get("body") {
            Some(b) => b,
            None => return refs,
        };
        Self::collect_inline_refs_recursive(body, &mut refs);
        refs
    }

    /// Extract inline bibliography refs from content in raw article data.
    fn collect_inline_refs_recursive(value: &Value, refs: &mut Vec<InlineRef>) {
        match value {
            Value::Object(map) => {
                let type_ = map.get("type_").and_then(|v| v.as_str());
                if type_ == Some("example") {
                    if let Some(content) = map
                        .get("quote")
                        .and_then(|q| q.get("content"))
                        .and_then(|c| c.as_str())
                    {
                        Self::extract_refs_from_quote(content, refs);
                    }
                } else if type_ == Some("explanation")
                    && let Some(content) = map.get("content").and_then(|c| c.as_str())
                {
                    Self::extract_refs_from_quote(content, refs);
                }
                for v in map.values() {
                    Self::collect_inline_refs_recursive(v, refs);
                }
            }
            Value::Array(arr) => {
                for v in arr {
                    Self::collect_inline_refs_recursive(v, refs);
                }
            }
            _ => {}
        }
    }

    /// Parse inline bibliography references from a single quote content string.
    fn extract_refs_from_quote(content: &str, refs: &mut Vec<InlineRef>) {
        for cap in INLINE_REF_REGEX.captures_iter(content) {
            let full_match = cap.get(0).unwrap();
            let inner = &cap[1];

            let paren_start = full_match.start() + full_match.as_str().find('(').unwrap();
            let paren_end = full_match.end();

            // Handle multiple refs separated by semicolons, e.g.
            // "(ordt, Meløy; StjørOrdt 21)".
            for segment in inner.split(';') {
                let segment = segment.trim();
                if segment.is_empty() {
                    continue;
                }

                let (code, spec) = match segment.find(' ') {
                    Some(pos) => {
                        let code = &segment[..pos];
                        let spec = segment[pos + 1..].trim();
                        (code, if spec.is_empty() { None } else { Some(spec) })
                    }
                    None => (segment, None),
                };

                let first_char = code.chars().next().unwrap_or(' ');
                if !first_char.is_uppercase() {
                    continue;
                }

                refs.push(InlineRef {
                    quote_content: content.to_string(),
                    offset_start: paren_start,
                    offset_end: paren_end,
                    code: code.to_string(),
                    spec: spec.map(|s| s.to_string()),
                });
            }
        }
    }

    /// Store extracted inline refs and resolve codes against the bibliography
    /// and places tables.
    async fn store_inline_refs(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        dict_str: &str,
        article_id: i64,
        refs: &[InlineRef],
    ) -> Result<(Vec<i64>, Vec<String>)> {
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
                .push_bind(r.offset_start as i32)
                .push_bind(r.offset_end as i32)
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

    /// After a bibliography entry is synced, resolve any pending inline refs
    /// that match the new entry's code.
    async fn resolve_inline_ref_as_bibl(&self, bibl_id: i64, code: &str) -> Result<()> {
        let result = sqlx::query(
            "UPDATE inline_ref_parse SET bibl_id = $1, ref_type = 'bibl' \
             WHERE code = $2 AND ref_type IS NULL",
        )
        .bind(bibl_id)
        .bind(code)
        .execute(&self.db)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(());
        }

        info!(
            "Resolved {} inline refs for code '{code}' to bibliography ID {bibl_id}",
            result.rows_affected()
        );

        sqlx::query(
            "INSERT INTO article_bibliography (dictionary, article_id, bibl_id) \
             SELECT DISTINCT dictionary, article_id, $1 \
             FROM inline_ref_parse \
             WHERE code = $2 AND bibl_id = $1 \
             ON CONFLICT DO NOTHING",
        )
        .bind(bibl_id)
        .bind(code)
        .execute(&self.db)
        .await?;

        let rows: Vec<(String, i64, Value)> = sqlx::query_as(
            "SELECT a.dictionary, a.id, a.data FROM articles a \
             INNER JOIN inline_ref_parse irp \
             ON a.dictionary = irp.dictionary AND a.id = irp.article_id \
             WHERE irp.code = $1 AND irp.bibl_id = $2",
        )
        .bind(code)
        .bind(bibl_id)
        .fetch_all(&self.db)
        .await?;

        for (dict_str, article_id, data) in &rows {
            let dict = Self::parse_dict(dict_str)?;
            if let Err(e) = self.index_article(dict, *article_id, data).await {
                warn!("Failed to re-index article {article_id} after inline bibl resolution: {e}");
            }
        }

        Ok(())
    }

    /// Resolve any pending inline refs that match the given place name.
    pub async fn resolve_inline_place_by_name(
        &self,
        place_id: i64,
        place_name: &str,
    ) -> Result<()> {
        // Match both exact name and name + M suffix.
        let code_with_m = format!("{place_name}M");
        let codes = vec![place_name.to_string(), code_with_m];

        let result = sqlx::query(
            "UPDATE inline_ref_parse SET place_id = $1, ref_type = 'place' \
             WHERE code = ANY($2) AND ref_type IS NULL",
        )
        .bind(place_id)
        .bind(&codes)
        .execute(&self.db)
        .await?;

        if result.rows_affected() == 0 {
            return Ok(());
        }

        info!(
            "Resolved {} inline refs for place '{place_name}' to place ID {place_id}",
            result.rows_affected()
        );

        let rows: Vec<(String, i64, Value)> = sqlx::query_as(
            "SELECT a.dictionary, a.id, a.data FROM articles a \
             INNER JOIN inline_ref_parse irp \
             ON a.dictionary = irp.dictionary AND a.id = irp.article_id \
             WHERE irp.place_id = $1",
        )
        .bind(place_id)
        .fetch_all(&self.db)
        .await?;

        for (dict_str, article_id, data) in &rows {
            let dict = Self::parse_dict(dict_str)?;
            if let Err(e) = self.index_article(dict, *article_id, data).await {
                warn!("Failed to re-index article {article_id} after inline place resolution: {e}");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_dictionary_all() {
        let all = UibDictionary::all();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&UibDictionary::Bokmål));
        assert!(all.contains(&UibDictionary::Nynorsk));
        assert!(all.contains(&UibDictionary::NorskOrdbok));
    }

    #[test]
    fn test_dictionary_as_str_roundtrip() {
        for dict in UibDictionary::all() {
            let s = dict.as_str();
            let parsed = UibDictionary::from_str(s, false).unwrap();
            assert_eq!(*dict, parsed);
        }
    }

    #[test]
    fn test_dictionary_as_str_values() {
        assert_eq!(UibDictionary::Bokmål.as_str(), "bm");
        assert_eq!(UibDictionary::Nynorsk.as_str(), "nn");
        assert_eq!(UibDictionary::NorskOrdbok.as_str(), "no");
    }

    #[test]
    fn test_dictionary_from_str_invalid() {
        assert!(UibDictionary::from_str("xx", false).is_err());
    }

    #[test]
    fn test_convert_raw_article_metadata() {
        let raw = json!([58083, "fjordsting", 2, "2026-04-30 14:55:59.171553"]);
        let meta = ArticleSyncService::convert_raw_article_metadata(raw).unwrap();
        assert_eq!(meta.article_id, 58083);
        assert_eq!(meta.primary_lemma, "fjordsting");
        assert_eq!(meta.revision, Some(2));
        assert_eq!(meta.updated_at, "2026-04-30 14:55:59.171553");
    }

    #[test]
    fn test_convert_raw_article_metadata_id_only() {
        let raw = json!([12345]);
        let meta = ArticleSyncService::convert_raw_article_metadata(raw).unwrap();
        assert_eq!(meta.article_id, 12345);
        assert_eq!(meta.primary_lemma, "");
        assert_eq!(meta.revision, Some(0));
        assert_eq!(meta.updated_at, "");
    }

    #[test]
    fn test_convert_raw_article_metadata_not_array() {
        let raw = json!({ "article_id": 1 });
        assert!(ArticleSyncService::convert_raw_article_metadata(raw).is_err());
    }

    #[test]
    fn test_convert_raw_article_metadata_empty_array() {
        let raw = json!([]);
        assert!(ArticleSyncService::convert_raw_article_metadata(raw).is_err());
    }

    #[test]
    fn test_find_first_lemma() {
        let data = json!({
            "lemmas": [
                { "hgno": 0, "id": 90001, "lemma": "strandskog" },
                { "hgno": 0, "id": 90002, "lemma": "strandskogen" }
            ]
        });
        assert_eq!(ArticleSyncService::find_first_lemma(&data), "strandskog");
    }

    #[test]
    fn test_find_first_lemma_missing() {
        assert_eq!(ArticleSyncService::find_first_lemma(&json!({})), "");
        assert_eq!(
            ArticleSyncService::find_first_lemma(&json!({ "lemmas": [] })),
            ""
        );
    }

    #[test]
    fn test_find_related_article_ids_in_definitions() {
        let data = json!({
            "body": {
                "definitions": [
                    {
                        "type_": "definition",
                        "elements": [
                            {
                                "type_": "explanation",
                                "content": "eit slag $",
                                "items": [
                                    {
                                        "type_": "article_ref",
                                        "article_id": 2002,
                                        "lemmas": [
                                            { "type_": "lemma", "hgno": 0, "id": 2529, "lemma": "skog" }
                                        ],
                                        "definition_id": null
                                    }
                                ]
                            }
                        ],
                        "id": 2
                    }
                ]
            }
        });
        let mut ids = Vec::new();
        ArticleSyncService::find_related_article_ids(&data, &mut ids);
        assert_eq!(ids, vec![2002]);
    }

    #[test]
    fn test_find_related_article_ids_sub_article() {
        let data = json!({
            "body": {
                "definitions": [
                    {
                        "type_": "definition",
                        "elements": [
                            {
                                "type_": "sub_article",
                                "article_id": 5001,
                                "lemmas": []
                            }
                        ],
                        "id": 3
                    }
                ]
            }
        });
        let mut ids = Vec::new();
        ArticleSyncService::find_related_article_ids(&data, &mut ids);
        assert_eq!(ids, vec![5001]);
    }

    #[test]
    fn test_find_related_article_ids_deduplicates() {
        let data = json!({
            "items": [
                { "type_": "article_ref", "article_id": 3000 },
                { "type_": "article_ref", "article_id": 3000 },
            ]
        });
        let mut ids = Vec::new();
        ArticleSyncService::find_related_article_ids(&data, &mut ids);
        assert_eq!(ids, vec![3000]);
    }

    #[test]
    fn test_find_related_article_ids_none() {
        let data = json!({
            "body": {
                "pronunciation": [],
                "definitions": [
                    {
                        "type_": "definition",
                        "elements": [
                            { "type_": "explanation", "content": "noko", "items": [] }
                        ],
                        "id": 1
                    }
                ]
            }
        });
        let mut ids = Vec::new();
        ArticleSyncService::find_related_article_ids(&data, &mut ids);
        assert!(ids.is_empty());
    }

    #[test]
    fn test_extract_refs_simple() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "dei dreiv med fjordfiske(Fj.Skr III,42)",
            &mut refs,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Fj.Skr");
        assert_eq!(refs[0].spec.as_deref(), Some("III,42"));
        assert_eq!(refs[0].offset_start, 24);
        assert_eq!(
            refs[0].offset_end,
            "dei dreiv med fjordfiske(Fj.Skr III,42)".len()
        );
    }

    #[test]
    fn test_extract_refs_no_spec() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "ho sette seg ned og kvilde(HaBrev)",
            &mut refs,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "HaBrev");
        assert_eq!(refs[0].spec, None);
    }

    #[test]
    fn test_extract_refs_with_trailing_text() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "han tok ljaaen sin(Fj.Skr II,87)og gjekk ut",
            &mut refs,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Fj.Skr");
        assert_eq!(refs[0].spec.as_deref(), Some("II,87"));
    }

    #[test]
    fn test_extract_refs_semicolon_separated() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "dei slo graset tidleg(ordt, Vik; DalOrdt 15)",
            &mut refs,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "DalOrdt");
        assert_eq!(refs[0].spec.as_deref(), Some("15"));
    }

    #[test]
    fn test_extract_refs_skips_editorial_parens_with_space() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "garden (den gamle) var stor, og dei (folket) trivdest godt der(Heim.S 1901)",
            &mut refs,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Heim.S");
        assert_eq!(refs[0].spec.as_deref(), Some("1901"));
    }

    #[test]
    fn test_extract_refs_no_refs() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "det var stilt i fjorden den kvelden",
            &mut refs,
        );
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_refs_skips_lowercase_code() {
        let mut refs = Vec::new();
        ArticleSyncService::extract_refs_from_quote(
            "dei budde langt inne i dalen(ordt, Vik)",
            &mut refs,
        );
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_inline_refs_from_article() {
        let article = json!({
            "body": {
                "definitions": [{
                    "type_": "definition",
                    "id": 1,
                    "elements": [{
                        "type_": "example",
                        "quote": { "content": "dei rodde ut kvar morgon(Fj.Skr 104)", "items": [] },
                        "attest": [],
                        "explanation": { "content": "", "items": [] }
                    }, {
                        "type_": "example",
                        "quote": { "content": "vanleg tekst utan kjelde", "items": [] },
                        "attest": [],
                        "explanation": { "content": "", "items": [] }
                    }]
                }]
            }
        });
        let refs = ArticleSyncService::extract_inline_refs(&article);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Fj.Skr");
        assert_eq!(refs[0].spec.as_deref(), Some("104"));
    }
}
