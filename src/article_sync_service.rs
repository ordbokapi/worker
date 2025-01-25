use anyhow::{anyhow, Result};
use clap::ValueEnum;
use log::{debug, info, warn};
use redis::{AsyncCommands, JsonAsyncCommands};
use serde_json::Value;
use std::collections::HashMap;

#[cfg(feature = "matrix_notifs")]
use crate::matrix_notify_service::MatrixNotifyService;
use crate::queue::{JobPayload, JobQueue, JobQueueService};

/// Dictionary variants for UiB
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

    /// Convert to string for usage in Redis keys or for the UiB API path, etc.
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

    fn to_possible_value<'a>(&self) -> Option<clap::builder::PossibleValue> {
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

/// Main service that handles fetch + store logic.
#[derive(Clone)]
pub struct ArticleSyncService {
    pub redis_client: redis::aio::ConnectionManager,
    pub job_queue: JobQueueService,
    #[cfg(feature = "matrix_notifs")]
    pub matrix_service: MatrixNotifyService,
}

/// Gets a dedup key for the article ID and dictionary.
fn dedup_key(dict: UibDictionary, article_id: i64) -> String {
    format!("lock:article:{}:{}", dict.as_str(), article_id)
}

#[cfg(feature = "matrix_notifs")]
struct ArticleListSyncResult {
    dict: UibDictionary,
    new_articles: u32,
    updated_articles: u32,
    missing_articles: u32,
}

impl ArticleSyncService {
    /// Fire off a job to do the full article-list sync for a dictionary.
    pub async fn enqueue_sync_articles(&mut self, dict: UibDictionary) -> Result<()> {
        let payload = JobPayload {
            data: serde_json::json!({ "dictionary": dict.as_str() }),
        };
        self.job_queue
            .enqueue(JobQueue::FetchArticleList, &payload)
            .await
    }

    /// Fire off a job to do the dictionary metadata sync.
    pub async fn enqueue_sync_dict_metadata(&mut self, dict: UibDictionary) -> Result<()> {
        let payload = JobPayload {
            data: serde_json::json!({ "dictionary": dict.as_str() }),
        };
        self.job_queue
            .enqueue(JobQueue::FetchDictionaryMetadata, &payload)
            .await
    }

    /// Fire off a job to fetch a single article.
    pub async fn enqueue_sync_article(
        &mut self,
        dict: UibDictionary,
        metadata: &ArticleMetadata,
    ) -> Result<()> {
        let payload = serde_json::json!({
            "dictionary": dict.as_str(),
            "metadata": {
                "articleId": metadata.article_id,
                "primaryLemma": metadata.primary_lemma,
                "revision": metadata.revision,
                "updatedAt": metadata.updated_at
            }
        });
        self.job_queue
            .enqueue_dedup(
                JobQueue::FetchArticle,
                &dedup_key(dict, metadata.article_id),
                &JobPayload { data: payload },
                6 * 60 * 60,
            )
            .await
            .map(|_| ())
    }

    /// Job handler for "FetchArticleList".
    pub async fn handle_sync_articles(&mut self, dict: UibDictionary) -> Result<()> {
        info!("[{:?}] Fetching article list from UiB API", dict);
        let article_list = self.fetch_article_list_from_uib(dict).await?;
        info!(
            "[{:?}] {} articles returned from UiB",
            dict,
            article_list.len()
        );

        let existing_metadata_map = self.get_all_article_metadata(dict).await?;
        info!(
            "[{:?}] We have {} existing articles in Redis",
            dict,
            existing_metadata_map.len()
        );

        // Prepare a vector to hold all the JSON payloads for articles we need to sync.
        let mut payloads_to_enqueue = Vec::new();

        #[cfg(feature = "matrix_notifs")]
        let mut result = ArticleListSyncResult {
            dict,
            new_articles: 0,
            updated_articles: 0,
            missing_articles: 0,
        };

        for raw_meta in article_list.iter() {
            let meta = self.convert_raw_article_metadata(raw_meta)?;

            // Check if existing metadata exists, and if so, matches the fetched metadata
            if let Some(existing) = existing_metadata_map.get(&meta.article_id) {
                if existing.revision == meta.revision {
                    // Same revision, check updatedAt
                    if existing.updated_at == meta.updated_at {
                        // No change
                        continue;
                    } else {
                        // Updated article
                        debug!(
                            "[{:?}] Article {} ({}) updated in UiB list {} -> {}",
                            dict,
                            meta.article_id,
                            meta.primary_lemma,
                            existing.updated_at,
                            meta.updated_at
                        );
                        #[cfg(feature = "matrix_notifs")]
                        {
                            result.updated_articles += 1;
                        }
                    }
                } else {
                    // New revision
                    debug!(
                        "[{:?}] Article {} ({}) has new revision in UiB list {:?} -> {:?}",
                        dict, meta.article_id, meta.primary_lemma, existing.revision, meta.revision
                    );
                    #[cfg(feature = "matrix_notifs")]
                    {
                        result.updated_articles += 1;
                    }
                }
            } else {
                // New article
                debug!(
                    "[{:?}] New article {} ({}) found in UiB list",
                    dict, meta.article_id, meta.primary_lemma
                );
                #[cfg(feature = "matrix_notifs")]
                {
                    result.new_articles += 1;
                }
            }

            // Build the JSON payload to be passed enqueue_sync_article further down
            let payload = serde_json::json!({
                "dictionary": dict.as_str(),
                "metadata": {
                    "articleId": meta.article_id,
                    "primaryLemma": meta.primary_lemma,
                    "revision": meta.revision,
                    "updatedAt": meta.updated_at
                }
            });
            payloads_to_enqueue.push(payload);
        }

        // Detect articles no longer in UiB's new list. We should manually check these
        // by fetching since they may possibly still exist if fetched directly.
        let new_id_set: std::collections::HashSet<i64> = article_list
            .iter()
            .filter_map(|raw| raw.get(0))
            .filter_map(|v| v.as_i64())
            .collect();
        let mut missing_count = 0;
        for (existing_id, ex_meta) in existing_metadata_map {
            if !new_id_set.contains(&existing_id) {
                debug!(
                    "[{:?}] Article {} ({}) no longer in UiB list",
                    dict, existing_id, ex_meta.primary_lemma
                );

                // Enqueue a fetch job for this article
                let payload = serde_json::json!({
                    "dictionary": dict.as_str(),
                    "metadata": {
                        "articleId": existing_id,
                        "primaryLemma": ex_meta.primary_lemma,
                        "revision": ex_meta.revision,
                        "updatedAt": ex_meta.updated_at
                    }
                });
                payloads_to_enqueue.push(payload);

                missing_count += 1;
            }
        }

        if missing_count > 0 {
            warn!(
                "[{:?}] {} articles no longer in UiB list, will re-fetch to check for updates",
                dict, missing_count
            );
            #[cfg(feature = "matrix_notifs")]
            {
                result.missing_articles = missing_count;
            }
        }

        let total = payloads_to_enqueue.len();

        // Perform a single pipelined batch enqueue
        if !payloads_to_enqueue.is_empty() {
            self.job_queue
                .enqueue_batch_dedup(
                    JobQueue::FetchArticle,
                    &payloads_to_enqueue
                        .into_iter()
                        .map(|p| {
                            let article_id = p
                                .get("metadata")
                                .and_then(|m| m.get("articleId"))
                                .and_then(|v| v.as_i64())
                                .unwrap_or(0);
                            (dedup_key(dict, article_id), p)
                        })
                        .collect::<Vec<_>>(),
                    6 * 60 * 60,
                )
                .await?;
        }

        info!("[{:?}] Enqueued {} article fetch jobs", dict, total);

        #[cfg(feature = "matrix_notifs")]
        {
            let msg = format!(
                "[{:?}] **Synkroniserer artiklar med UiB.**\n**Nye:** {}\n**Oppdaterte:** {}\n**Manglar i UiB-lista:** {}",
                result.dict, result.new_articles, result.updated_articles, result.missing_articles
            );
            self.matrix_service.queue_message(&msg).await;
        }

        Ok(())
    }

    /// Job handler for "FetchArticle".
    pub async fn handle_sync_article(
        &mut self,
        dict: UibDictionary,
        partial_meta: &Value,
    ) -> Result<()> {
        let article_id = partial_meta
            .get("articleId")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow!("Missing articleId in metadata"))?;

        let primary_lemma = partial_meta
            .get("primaryLemma")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        info!(
            "[{:?}] Fetching article {} ({}) from UiB",
            dict, article_id, primary_lemma
        );

        let article_json = self.fetch_article_from_uib(dict, article_id).await?;

        debug!(
            "[{:?}] Article {} ({}) fetched from UiB",
            dict, article_id, primary_lemma
        );

        let updated_article = self.add_ordbokapi_metadata(article_json)?;

        // If partial_meta is incomplete, reconstruct more precise metadata.
        let final_meta = serde_json::json!({
            "articleId": article_id,
            "primaryLemma": if let Some(v) = partial_meta.get("primaryLemma").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
                v.to_string()
            } else {
                self.find_first_lemma(&updated_article)
            },
            "revision": partial_meta.get("revision").and_then(|v| v.as_i64()).unwrap_or(0),
            "updatedAt": if let Some(v) = partial_meta.get("updatedAt").and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
                v
            } else {
                updated_article.get("updated").and_then(|v| v.as_str()).unwrap_or("1970-01-01T00:00:00Z")
            },
        });

        debug!(
            "[{:?}] Storing article {} ({}) in Redis",
            dict, article_id, primary_lemma
        );

        // Store in Redis
        self.store_article(dict, article_id, &updated_article, &final_meta)
            .await?;

        info!(
            "[{:?}] Article {} ({}) stored in Redis",
            dict, article_id, primary_lemma
        );

        // Then queue related articles
        self.queue_related_articles(dict, article_id, &updated_article)
            .await?;

        Ok(())
    }

    /// Job handler for "FetchDictionaryMetadata".
    pub async fn handle_sync_dictionary_metadata(&mut self, dict: UibDictionary) -> Result<()> {
        info!("[{:?}] Fetching dictionary metadata from UiB", dict);
        let concepts = self.fetch_concepts(dict).await?;
        let word_classes = self.fetch_word_classes(dict).await?;
        let word_subclasses = self.fetch_word_subclasses(dict).await?;
        info!("[{:?}] Fetched all metadata from UiB", dict);

        // Store
        let conn = &mut self.redis_client;
        let concept_key = format!("dictionary:{}:concepts", dict.as_str());
        let wc_key = format!("dictionary:{}:word_classes", dict.as_str());
        let wsc_key = format!("dictionary:{}:word_subclasses", dict.as_str());

        // Use a pipelined batch to store all dictionary metadata
        let _: () = redis::pipe()
            .cmd("JSON.SET")
            .arg(&concept_key)
            .arg("$")
            .arg(serde_json::to_string(&concepts)?)
            .cmd("JSON.SET")
            .arg(&wc_key)
            .arg("$")
            .arg(serde_json::to_string(&word_classes)?)
            .cmd("JSON.SET")
            .arg(&wsc_key)
            .arg("$")
            .arg(serde_json::to_string(&word_subclasses)?)
            .query_async(conn)
            .await?;

        info!("[{:?}] Dictionary metadata stored in Redis", dict);
        Ok(())
    }

    /// On startup, check if we need to do an initial sync for each dictionary.
    pub async fn initial_sync(&mut self) -> Result<()> {
        for dict in UibDictionary::all() {
            // Check if there are any articles for this dictionary
            let existing_metadata = self.get_all_article_metadata(*dict).await?;

            // Check if we've already queued the initial article fetch
            let marker_key = format!("dictionary:{}:initial_queue_done", dict.as_str());
            let already_queued: bool = self.redis_client.exists(&marker_key).await.unwrap_or(false);

            // If no articles, queue the initial fetch
            if existing_metadata.is_empty() && !already_queued {
                warn!(
                    "[{:?}] No articles in DB, queueing initial article fetch.",
                    dict
                );
                self.enqueue_sync_articles(*dict).await?;

                let conn = &mut self.redis_client;
                // Set a marker key to prevent re-queueing
                let _: () = redis::cmd("SET")
                    .arg(&marker_key)
                    .arg("1")
                    .query_async(conn)
                    .await?;
            }

            // Check if we have dictionary metadata
            let concept_key = format!("dictionary:{}:concepts", dict.as_str());
            let concept_val: Option<String> = self.redis_client.json_get(&concept_key, "$").await?;
            if concept_val.is_none() {
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
    fn convert_raw_article_metadata(&self, raw: &Value) -> Result<ArticleMetadata> {
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
        &mut self,
        dict: UibDictionary,
        article_id: i64,
        article_data: &Value,
        meta: &Value,
    ) -> Result<()> {
        let conn = &mut self.redis_client;
        let article_key = format!("article:{}:{}", dict.as_str(), article_id);
        let hash_key = format!("dictionary:{}:articles", dict.as_str());

        let article_json_str = serde_json::to_string(article_data)?;
        let meta_json_str = serde_json::to_string(meta)?;

        let _: () = redis::pipe()
            .cmd("JSON.SET")
            .arg(&article_key)
            .arg("$")
            .arg(&article_json_str)
            .ignore()
            .cmd("HSET")
            .arg(&hash_key)
            .arg(article_id.to_string())
            .arg(&meta_json_str)
            .ignore()
            .query_async(conn)
            .await?;

        Ok(())
    }

    /// Retrieve all known articles from "dictionary:<dict>:articles" as a map.
    async fn get_all_article_metadata(
        &mut self,
        dict: UibDictionary,
    ) -> Result<HashMap<i64, ArticleMetadata>> {
        let hash_key = format!("dictionary:{}:articles", dict.as_str());
        let conn = &mut self.redis_client;
        let vals: HashMap<String, String> = conn.hgetall(&hash_key).await?;
        let mut map = HashMap::new();
        for (article_id_str, meta_json_str) in vals {
            if let Ok(article_id) = article_id_str.parse::<i64>() {
                if let Ok(v) = serde_json::from_str::<Value>(&meta_json_str) {
                    let article_id = v
                        .get("articleId")
                        .and_then(|x| x.as_i64())
                        .unwrap_or(article_id);
                    let primary_lemma = v
                        .get("primaryLemma")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    let revision = Some(v.get("revision").and_then(|x| x.as_i64()).unwrap_or(0));
                    let updated_at = v
                        .get("updatedAt")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    map.insert(
                        article_id,
                        ArticleMetadata {
                            article_id,
                            primary_lemma,
                            revision,
                            updated_at,
                        },
                    );
                }
            }
        }
        Ok(map)
    }

    /// Add the `__ordbokapi__` block containing `hasSplitInf`. This is used for
    /// indexing purposes by Redisearch.
    fn add_ordbokapi_metadata(&self, mut article_json: Value) -> Result<Value> {
        // Parse out lemmas from the JSON, check if any have "split_inf == true"
        let has_split_inf = article_json
            .get("lemmas")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter().any(|lemma| {
                    lemma
                        .get("split_inf")
                        .and_then(|x| x.as_bool())
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);

        // Insert or overwrite __ordbokapi__
        if let Some(obj) = article_json.as_object_mut() {
            let mut ordbokapi_obj = serde_json::Map::new();
            ordbokapi_obj.insert("hasSplitInf".to_string(), Value::Bool(has_split_inf));
            obj.insert("__ordbokapi__".to_string(), Value::Object(ordbokapi_obj));
        }
        Ok(article_json)
    }

    /// Gets the first lemma from the article JSON. Defaults to an empty string if not found.
    fn find_first_lemma(&self, article_json: &Value) -> String {
        article_json
            .get("lemmas")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|lemma| lemma.get("lemma"))
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string()
    }

    /// After we've synced an article, queue any related articles from sub_article or article_ref
    /// fields in the JSON.
    async fn queue_related_articles(
        &mut self,
        dict: UibDictionary,
        article_id: i64,
        article: &Value,
    ) -> Result<()> {
        let mut related_ids = vec![];
        debug!("[{:?}] Checking related articles for {}", dict, article_id);
        ArticleSyncService::find_related_article_ids(article, &mut related_ids);
        if related_ids.is_empty() {
            debug!("[{:?}] No related articles found for {}", dict, article_id);
            return Ok(());
        }

        debug!(
            "[{:?}] Found {} related articles for {}",
            dict,
            related_ids.len(),
            article_id
        );

        // Check if these articles already exist in Redis, and enqueue if not
        let hash_key = format!("dictionary:{}:articles", dict.as_str());

        let mut enqueued_count = 0;

        for rid in related_ids {
            let exists: bool = {
                let conn = &mut self.redis_client;
                redis::cmd("HEXISTS")
                    .arg(&hash_key)
                    .arg(rid.to_string())
                    .query_async(conn)
                    .await
                    .unwrap_or(false)
            };

            if !exists {
                // We don't know what the metadata is in this context, so we provide
                // a stub with the article_id and an empty primary_lemma.
                let partial = ArticleMetadata {
                    article_id: rid,
                    primary_lemma: "".to_string(),
                    revision: None,
                    updated_at: "".to_string(),
                };
                self.enqueue_sync_article(dict, &partial).await?;
                enqueued_count += 1;
            }
        }

        if enqueued_count > 0 {
            debug!(
                "[{:?}] Queued {} related article fetch jobs for {}",
                dict, enqueued_count, article_id
            );
        }
        Ok(())
    }

    fn find_related_article_ids(v: &Value, acc: &mut Vec<i64>) {
        match v {
            Value::Object(map) => {
                // Check if type_ == "article_ref" or "sub_article" and article_id is numeric.
                if let Some(Value::String(t)) = map.get("type_") {
                    if (t == "article_ref" || t == "sub_article") && map.get("article_id").is_some()
                    {
                        if let Some(id_val) = map.get("article_id") {
                            if let Some(id_i64) = id_val.as_i64() {
                                acc.push(id_i64);
                            }
                        }
                    }
                }
                // Walk children.
                for (_k, child) in map {
                    ArticleSyncService::find_related_article_ids(child, acc);
                }
            }
            Value::Array(arr) => {
                for child in arr {
                    ArticleSyncService::find_related_article_ids(child, acc);
                }
            }
            _ => {}
        }
    }
}
