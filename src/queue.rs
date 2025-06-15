use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use log::{debug, error, info, trace, warn};
use redis::RedisResult;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::article_sync_service::UibDictionary;

/// Represents a named queue.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum JobQueue {
    FetchArticleList,
    FetchArticle,
    FetchDictionaryMetadata,
    #[cfg(feature = "matrix_notifs")]
    MatrixNotify,
}

impl JobQueue {
    pub fn to_stream_key(self) -> &'static str {
        match self {
            JobQueue::FetchArticleList => "stream:fetch-article-list",
            JobQueue::FetchArticle => "stream:fetch-article",
            JobQueue::FetchDictionaryMetadata => "stream:fetch-dict-metadata",
            #[cfg(feature = "matrix_notifs")]
            JobQueue::MatrixNotify => "stream:matrix-notify",
        }
    }

    pub fn to_group_name(self) -> &'static str {
        match self {
            JobQueue::FetchArticleList => "group:fetch-article-list",
            JobQueue::FetchArticle => "group:fetch-article",
            JobQueue::FetchDictionaryMetadata => "group:fetch-dict-metadata",
            #[cfg(feature = "matrix_notifs")]
            JobQueue::MatrixNotify => "group:matrix-notify",
        }
    }
}

/// Job payload structure as a JSON object.
#[derive(Debug, Clone)]
pub struct JobPayload {
    pub data: Value,
}

/// Gets the dictionary from the payload.
pub fn get_dict_id(job: &JobPayload) -> Result<UibDictionary> {
    let dict_str = job
        .data
        .get("dictionary")
        .and_then(|v| v.as_str())
        .map(|s| UibDictionary::from_str(s, false));
    match dict_str {
        Some(Ok(d)) => Ok(d),
        Some(Err(e)) => Err(anyhow!("Invalid dictionary value: {e}")),
        None => Err(anyhow!("Missing dictionary value in job payload.")),
    }
}

/// The result of XREADGROUP, a nested Vec.
type XReadGroupResultInner = Vec<(String, Vec<(String, HashMap<String, String>)>)>;

/// The result of XREADGROUP, a nested Vec.
type XReadGroupResult = RedisResult<Option<XReadGroupResultInner>>;

#[derive(Clone)]
pub struct JobQueueService {
    /// The underlying redis client (shared among tasks).
    pub redis_client: redis::Client,
    /// A connection to use for queuing new jobs.
    pub redis_connection_manager: redis::aio::ConnectionManager,
}

impl JobQueueService {
    /// Create the consumer group for each queue, if not already existing.
    ///
    /// This should be called once at startup. If the stream does not exist,
    /// MKSTREAM ensures the stream is created.
    pub async fn setup_consumer_groups(&mut self, recreate: bool) -> Result<()> {
        for queue in [
            JobQueue::FetchArticleList,
            JobQueue::FetchArticle,
            JobQueue::FetchDictionaryMetadata,
            #[cfg(feature = "matrix_notifs")]
            JobQueue::MatrixNotify,
        ] {
            let stream_key = queue.to_stream_key();
            let group_name = queue.to_group_name();

            let conn = &mut self.redis_connection_manager;

            // If recreate is true, delete the group first.
            if recreate {
                let _: RedisResult<String> = redis::cmd("XGROUP")
                    .arg("DESTROY")
                    .arg(stream_key)
                    .arg(group_name)
                    .query_async(conn)
                    .await;

                info!("Deleted group '{group_name}' on '{stream_key}'");
            }

            // XGROUP CREATE <key> <groupname> $ MKSTREAM
            // The `$` means "start delivering only new messages".
            // To re-process old messages, could use `0` instead.
            let result: RedisResult<String> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream_key)
                .arg(group_name)
                .arg("$")
                .arg("MKSTREAM")
                .query_async(conn)
                .await;

            match result {
                Ok(_) => info!("Created group '{group_name}' on '{stream_key}'"),
                Err(e) => {
                    let msg = format!("{e}");
                    if msg.contains("BUSYGROUP") {
                        // Group already exists.
                        info!("Consumer group '{group_name}' for '{stream_key}' already exists.");
                    } else {
                        return Err(anyhow!("Failed creating consumer group: {e}"));
                    }
                }
            }
        }

        if recreate {
            let _: RedisResult<i32> = redis::cmd("DEL")
                .arg("lock:*")
                .query_async(&mut self.redis_connection_manager)
                .await;
        }

        Ok(())
    }

    /// Push a job to the queue (XADD).
    pub async fn enqueue(&mut self, queue: JobQueue, payload: &JobPayload) -> Result<()> {
        let conn = &mut self.redis_connection_manager;
        let payload_str = serde_json::to_string(&payload.data)?;

        debug!("Enqueueing job: {payload_str}");

        // XADD <stream> * field value
        let _: String = redis::cmd("XADD")
            .arg(queue.to_stream_key())
            .arg("MAXLEN")
            .arg("~")
            .arg("1000000") // Trim to 1M messages.
            .arg("*") // Auto-generated ID.
            .arg("payload")
            .arg(&payload_str)
            .query_async(conn)
            .await
            .context("Failed to enqueue job")?;

        debug!("Enqueued job: {payload_str}");
        Ok(())
    }

    /// Enqueue a job only if a dedupKey is not already present in Redis.
    /// Returns Ok(true) if enqueued, Ok(false) if deduplicated (already in progress).
    pub async fn enqueue_dedup(
        &mut self,
        queue: JobQueue,
        dedup_key: &str,
        payload: &JobPayload,
        dedup_ttl_secs: usize,
    ) -> Result<bool> {
        let conn = &mut self.redis_connection_manager;

        // Attempt to set a small marker key in Redis with NX + EX.
        // If the key already exists, this returns None (or an error).
        // If it sets successfully, we proceed.
        let set_res: Option<String> = redis::cmd("SET")
            .arg(dedup_key)
            .arg(true) // The value can be anything; we just need the key
            .arg("NX") // Only set if not exists
            .arg("EX")
            .arg(dedup_ttl_secs)
            .query_async(conn)
            .await
            .context("Failed to SETNX dedup key")?;

        // If set_res is None, the key already exists, so we skip enqueuing.
        if set_res.is_none() {
            debug!("Dedup key '{}' already set; skipping enqueue.", dedup_key);
            return Ok(false);
        }

        // Enqueue job with XADD

        let payload_str = if let Value::Object(map) = &payload.data {
            // Add dedupKey to the XADD payload.
            let mut json_payload = map.clone();
            json_payload.insert("dedupKey".to_string(), Value::String(dedup_key.to_string()));
            serde_json::to_string(&json_payload)?
        } else {
            serde_json::to_string(&payload.data)?
        };

        let _: String = redis::cmd("XADD")
            .arg(queue.to_stream_key())
            .arg("MAXLEN")
            .arg("~")
            .arg("1000000") // Trim to 1M messages.
            .arg("*")
            .arg("payload")
            .arg(payload_str)
            .query_async(conn)
            .await?;

        debug!("Enqueued deduplicated job with key '{}'", dedup_key);

        Ok(true)
    }

    /// Push multiple jobs (XADD) with deduplication via SETNX + TTL.
    /// `payloads` is an array of `(dedup_key, payload_json)`.
    /// We SET each key with NX + EX = `dedup_ttl_secs` and only XADD if
    /// the SET was successful. If the SET was not done, we skip enqueue.
    pub async fn enqueue_batch_dedup(
        &mut self,
        queue: JobQueue,
        payloads: &[(String, Value)], // (dedup_key, payload_json)
        dedup_ttl_secs: usize,
    ) -> Result<()> {
        let mut conn = self.redis_connection_manager.clone();
        debug!("Enqueueing {} jobs with deduplication", payloads.len());

        if payloads.is_empty() {
            return Ok(());
        }

        // Attempt to SET all dedup keys in a single pipeline.
        let mut pipe_set = redis::pipe();
        for (dedup_key, _) in payloads {
            pipe_set
                .cmd("SET")
                .arg(dedup_key)
                .arg(true) // The value can be anything; we just need the key.
                .arg("NX")
                .arg("EX")
                .arg(dedup_ttl_secs);
        }

        // Each SET returns either:
        // - Some("OK") if it succeeded in setting (the key did not exist),
        // - None if the key already existed (dedup).
        // So the result will be a Vec<Option<String>> with length == payloads.len().
        let set_results: Vec<Option<String>> = pipe_set
            .query_async(&mut conn)
            .await
            .context("Failed executing pipeline for dedup SET")?;

        // Build a second pipeline of XADDs only for newly-set.
        let mut pipe_xadd = redis::pipe();
        let mut xadd_count = 0;

        for (i, (dedup_key, json_payload)) in payloads.iter().enumerate() {
            // If set_results[i] is Some("OK"), we can safely queue.
            if set_results[i].is_some() {
                let payload_str = if let Value::Object(map) = json_payload {
                    // Add dedupKey to the XADD payload.
                    let mut json_payload = map.clone();
                    json_payload.insert("dedupKey".to_string(), Value::String(dedup_key.clone()));
                    serde_json::to_string(&json_payload)
                        .context("Failed to serialize job payload to JSON string")?
                } else {
                    serde_json::to_string(json_payload)
                        .context("Failed to serialize job payload to JSON string")?
                };

                pipe_xadd
                    .cmd("XADD")
                    .arg(queue.to_stream_key())
                    .arg("MAXLEN")
                    .arg("~")
                    .arg("1000000") // Trim to 1M messages.
                    .arg("*")
                    .arg("payload")
                    .arg(payload_str);
                xadd_count += 1;
            }
        }

        // If xadd_count == 0, then everything was deduplicated, so we can skip.
        if xadd_count == 0 {
            debug!(
                "All {} jobs were deduplicated; no new enqueues.",
                payloads.len()
            );
            return Ok(());
        }

        // Execute the second pipeline (the XADD commands) only if needed.
        pipe_xadd
            .query_async(&mut conn)
            .await
            .context("Failed executing pipeline for XADD batch")
            .map(|_: ()| ())?;

        debug!(
            "Enqueued {} new jobs ({} were deduplicated)",
            xadd_count,
            payloads.len() - xadd_count
        );

        Ok(())
    }

    /// Logs error information using Sentry if the `sentry_integration` feature is enabled.
    /// When Sentry is not enabled, this is a no-op.
    #[cfg(feature = "sentry_integration")]
    pub async fn log_error(
        &mut self,
        error_info: &str,
        queue: Option<&str>,
        job_payload: Option<&serde_json::Value>,
    ) -> Result<()> {
        let mut extra = HashMap::new();
        if let Some(q) = queue {
            extra.insert("queue", serde_json::Value::String(q.to_string()));
        }
        if let Some(payload) = job_payload {
            extra.insert("job_payload", payload.clone());
        }
        sentry::with_scope(
            |scope| {
                scope.set_extra("extra", serde_json::to_value(&extra).unwrap_or_default());
            },
            || sentry::capture_message(error_info, sentry::Level::Error),
        );
        Ok(())
    }

    /// Logs error information using Sentry if the `sentry_integration` feature is enabled.
    /// When Sentry is not enabled, this is a no-op.
    #[cfg(not(feature = "sentry_integration"))]
    pub async fn log_error(
        &mut self,
        _error_info: &str,
        _queue: Option<&str>,
        _job_payload: Option<&serde_json::Value>,
    ) -> Result<()> {
        Ok(())
    }

    /// Start multiple consumer tasks for the given queue, each with concurrency=worker_count.
    ///
    /// For example, if `worker_count = 4`, we spawn 4 tasks (each with a unique consumer name
    /// within the same group).
    pub async fn start_worker_group<F, Fut>(
        &self,
        queue: JobQueue,
        worker_count: usize,
        max_messages_per_read: usize,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(JobPayload) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let group_name = queue.to_group_name().to_string();
        let stream_key = queue.to_stream_key().to_string();

        // This semaphore limits concurrency to allow each worker to handle only one
        // job at a time.
        let concurrency_sem = std::sync::Arc::new(Semaphore::new(worker_count));

        for i in 0..worker_count {
            let mut redis_client = self.redis_client.get_connection_manager().await?;
            let handler_clone = handler.clone();
            let group_name_clone = group_name.clone();
            let stream_key_clone = stream_key.clone();
            let concurrency_sem_clone = concurrency_sem.clone();

            let consumer_name = format!("consumer-{}-{}", stream_key_clone, i);

            // Spawn each worker in the background.
            tokio::spawn(async move {
                let conn = &mut redis_client;

                // Loop forever, reading from XREADGROUP.
                loop {
                    // Acquire the concurrency permit for this worker.
                    let _permit = concurrency_sem_clone.acquire().await.unwrap();

                    trace!("[{consumer_name}] Waiting for job…");

                    match xreadgroup_for_jobs(
                        conn,
                        &group_name_clone,
                        &consumer_name,
                        &stream_key_clone,
                        max_messages_per_read,
                    )
                    .await
                    {
                        Ok(Some(streams)) => {
                            let mut params = WorkerParams {
                                conn,
                                handler: &handler_clone,
                                stream_key: &stream_key_clone,
                                group_name: &group_name_clone,
                                consumer_name: &consumer_name,
                            };
                            if let Err(e) = process_entries(&mut params, streams).await {
                                error!("Error processing entries: {:?}", e);
                            }
                        }
                        Ok(None) => {
                            trace!("[{consumer_name}] No new jobs");
                        }
                        Err(e) => {
                            error!("Error from XREADGROUP on {stream_key_clone}: {e}");
                            sleep(Duration::from_secs(2)).await;
                        }
                    }
                }
            });
        }

        Ok(())
    }
}

struct WorkerParams<'a, F> {
    conn: &'a mut redis::aio::ConnectionManager,
    handler: &'a F,
    stream_key: &'a str,
    group_name: &'a str,
    consumer_name: &'a str,
}

async fn xreadgroup_for_jobs(
    conn: &mut redis::aio::ConnectionManager,
    group_name: &str,
    consumer_name: &str,
    stream_key: &str,
    max_messages: usize,
) -> XReadGroupResult {
    redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group_name)
        .arg(consumer_name)
        .arg("BLOCK")
        .arg("2000")
        .arg("COUNT")
        .arg(max_messages.to_string())
        .arg("STREAMS")
        .arg(stream_key)
        .arg(">")
        .query_async(conn)
        .await
}

async fn process_entries<F, Fut>(
    params: &mut WorkerParams<'_, F>,
    streams: XReadGroupResultInner,
) -> Result<()>
where
    F: Fn(JobPayload) -> Fut + Send + Sync,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    for (_stream, entries) in streams {
        for (entry_id, field_map) in entries {
            // Parse the payload.

            if let Some(payload_str) = field_map.get("payload") {
                match serde_json::from_str::<serde_json::Value>(payload_str) {
                    Ok(json_val) => {
                        let job_payload = JobPayload {
                            data: json_val.clone(),
                        };

                        // Call job handler.
                        if let Err(e) = (params.handler)(job_payload).await {
                            error!("Job failed: {e:?}");
                            let error_msg =
                                format!("Job processing error for entry {}: {:?}", entry_id, e);
                            let _ = log_error(
                                &mut *params.conn,
                                &error_msg,
                                Some(params.stream_key),
                                Some(&json_val),
                            )
                            .await;

                            // May consider pushing to a dead-letter queue, etc., later?
                            // For now, just XACK.

                            xack_and_cleanup(params, &entry_id, None).await;
                        } else {
                            // If success, XACK
                            xack_and_cleanup(params, &entry_id, None).await;
                        }
                    }
                    Err(e) => {
                        error!("Invalid JSON in payload: {e}");
                        let error_msg =
                            format!("Invalid JSON payload for entry {}: {:?}", entry_id, e);
                        let _ =
                            log_error(&mut *params.conn, &error_msg, Some(params.stream_key), None)
                                .await;
                        xack_and_cleanup(params, &entry_id, None).await;
                    }
                }
            } else {
                warn!("No payload field in entry {entry_id}");
                xack_and_cleanup(params, &entry_id, None).await;
            }
        }
    }
    Ok(())
}

async fn xack_and_cleanup<F>(
    params: &mut WorkerParams<'_, F>,
    entry_id: &str,
    dedup_key: Option<&str>,
) {
    let ack_res: redis::RedisResult<i64> = redis::cmd("XACK")
        .arg(params.stream_key)
        .arg(params.group_name)
        .arg(entry_id)
        .query_async(&mut *params.conn)
        .await;
    trace!("[{}] XACK {entry_id}: {:?}", params.consumer_name, ack_res);

    if let Err(e) = ack_res {
        error!("Failed XACK {entry_id}: {e}");
    } else if let Some(dedup_key) = dedup_key {
        let _: redis::RedisResult<i32> = redis::cmd("DEL")
            .arg(dedup_key)
            .query_async(&mut *params.conn)
            .await;
    }
}

/// Logs error information using Sentry if the `sentry_integration` feature is enabled.
/// When Sentry is not enabled, this is a no-op.
#[cfg(feature = "sentry_integration")]
async fn log_error(
    _conn: &mut redis::aio::ConnectionManager,
    error_info: &str,
    queue: Option<&str>,
    job_payload: Option<&serde_json::Value>,
) -> Result<()> {
    let mut extra = HashMap::new();
    if let Some(q) = queue {
        extra.insert("queue", serde_json::Value::String(q.to_string()));
    }
    if let Some(payload) = job_payload {
        extra.insert("job_payload", payload.clone());
    }
    sentry::with_scope(
        |scope| {
            scope.set_extra("extra", serde_json::to_value(&extra).unwrap_or_default());
        },
        || sentry::capture_message(error_info, sentry::Level::Error),
    );
    Ok(())
}

/// Logs error information using Sentry if the `sentry_integration` feature is enabled.
/// When Sentry is not enabled, this is a no-op.
#[cfg(not(feature = "sentry_integration"))]
async fn log_error(
    _conn: &mut redis::aio::ConnectionManager,
    _error_info: &str,
    _queue: Option<&str>,
    _job_payload: Option<&serde_json::Value>,
) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::article_sync_service::UibDictionary;
    use serde_json::json;

    #[test]
    fn test_job_queue_stream_key() {
        assert_eq!(
            JobQueue::FetchArticleList.to_stream_key(),
            "stream:fetch-article-list"
        );
        assert_eq!(
            JobQueue::FetchArticle.to_stream_key(),
            "stream:fetch-article"
        );
        assert_eq!(
            JobQueue::FetchDictionaryMetadata.to_stream_key(),
            "stream:fetch-dict-metadata"
        );
    }

    #[test]
    fn test_job_queue_group_name() {
        assert_eq!(
            JobQueue::FetchArticleList.to_group_name(),
            "group:fetch-article-list"
        );
        assert_eq!(
            JobQueue::FetchArticle.to_group_name(),
            "group:fetch-article"
        );
        assert_eq!(
            JobQueue::FetchDictionaryMetadata.to_group_name(),
            "group:fetch-dict-metadata"
        );
    }

    #[test]
    fn test_get_dict_id_success() {
        let payload = JobPayload {
            data: json!({"dictionary": "bm"}),
        };
        let dict = get_dict_id(&payload).unwrap();
        assert_eq!(dict, UibDictionary::Bokmål);
    }

    #[test]
    fn test_get_dict_id_invalid() {
        let payload = JobPayload {
            data: json!({"dictionary": "xx"}),
        };
        assert!(get_dict_id(&payload).is_err());
    }

    #[test]
    fn test_get_dict_id_missing() {
        let payload = JobPayload { data: json!({}) };
        assert!(get_dict_id(&payload).is_err());
    }
}
