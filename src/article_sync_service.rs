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
use meilisearch_sdk::client::Client as MeiliClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
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
    #[cfg(feature = "matrix_notifs")]
    pub matrix_message_storage: RedisStorage<SendMatrixMessageJob>,
}

impl ArticleSyncService {
    const DEDUPE_TTL: u64 = 24 * 60 * 60;

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

        tx.commit().await?;

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
        let doc = meili::build_search_document(dict.as_str(), article_id, article_data, Some(&bib));
        let idx = self.meili.index(meili::index_name(dict.as_str()));

        idx.add_or_replace(&[doc], Some("id"))
            .await
            .map_err(|e| anyhow!("Failed to index article in Meilisearch: {e}"))?;

        Ok(())
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

    /// Handle a single bibliography fetch job.
    pub async fn handle_sync_bibliography(&self, bibl_id: i64) -> Result<()> {
        let api_key = self
            .clarino_api_key
            .as_deref()
            .ok_or_else(|| anyhow!("CLARINO_API_KEY not set"))?;

        let entry = self.fetch_bibliography_entry(api_key, bibl_id).await?;
        self.store_bibliography_entry(bibl_id, &entry).await?;
        self.index_bibliography_entry(bibl_id, &entry).await?;

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
}
