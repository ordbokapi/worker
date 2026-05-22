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
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tracing::{info, warn};

use crate::state::UibDictionary;

/// Number of consecutive transient failures before the circuit opens.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 10;
/// How long the circuit stays open before allowing a probe request, in seconds.
const CIRCUIT_BREAKER_COOLDOWN: u64 = 60;

/// Client for UiB APIs.
#[derive(Clone)]
pub struct UibClient {
    client: Client,
    clarino_api_key: Option<String>,
    max_retries: u32,
    circuit: Arc<CircuitBreaker>,
}

/// Tracks consecutive transient failures.
struct CircuitBreaker {
    consecutive_failures: AtomicU32,
    last_failure_epoch: AtomicU64,
}

impl CircuitBreaker {
    const fn new() -> Self {
        Self {
            consecutive_failures: AtomicU32::new(0),
            last_failure_epoch: AtomicU64::new(0),
        }
    }

    fn record_success(&self) {
        let prev = self.consecutive_failures.swap(0, Ordering::Relaxed);
        if prev >= CIRCUIT_BREAKER_THRESHOLD {
            info!("Circuit breaker closed. Upstream recovered after {prev} consecutive failures.");
        }
    }

    fn record_transient_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_failure_epoch.store(now, Ordering::Relaxed);
    }

    fn is_open(&self) -> bool {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);
        if failures < CIRCUIT_BREAKER_THRESHOLD {
            return false;
        }
        let last = self.last_failure_epoch.load(Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(last) < CIRCUIT_BREAKER_COOLDOWN
    }
}

impl UibClient {
    #[must_use]
    pub fn new(clarino_api_key: Option<String>) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_mins(1))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(4)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            clarino_api_key,
            max_retries: 4,
            circuit: Arc::new(CircuitBreaker::new()),
        }
    }

    #[must_use]
    pub const fn has_clarino_key(&self) -> bool {
        self.clarino_api_key.is_some()
    }

    fn clarino_key(&self) -> Result<&str> {
        self.clarino_api_key
            .as_deref()
            .ok_or_else(|| anyhow!("CLARINO_API_KEY not set."))
    }

    /// Retry wrapper. Does exponential backoff.
    async fn with_retry<F, Fut, T>(&self, desc: &str, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if self.circuit.is_open() {
            return Err(anyhow!("{desc}: circuit breaker open, rejecting request."));
        }

        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match f().await {
                Ok(v) => {
                    self.circuit.record_success();
                    return Ok(v);
                }
                Err(e) if !is_transient_error(&e) => {
                    return Err(e);
                }
                Err(e) if attempt > self.max_retries => {
                    self.circuit.record_transient_failure();
                    return Err(e.context(format!("{desc} failed after {attempt} attempts.")));
                }
                Err(e) => {
                    let delay = Duration::from_millis(500 * 2u64.pow(attempt - 1));
                    warn!(
                        "{desc}: attempt {attempt}/{} failed ({e:#}), retrying in {delay:?}.",
                        self.max_retries + 1
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Fetch the article list for a dictionary.
    pub async fn fetch_article_list(&self, dict: UibDictionary) -> Result<Vec<Value>> {
        let url = format!("https://ord.uib.no/{}/fil/article.json", dict.as_str());
        self.with_retry(&format!("fetch_article_list({dict})"), || {
            let url = url.clone();
            let client = self.client.clone();
            async move {
                let resp = client.get(&url).send().await?.error_for_status()?;
                let data: Value = resp.json().await?;
                let arr = data
                    .as_array()
                    .ok_or_else(|| anyhow!("Expected JSON array."))?;
                Ok(arr.clone())
            }
        })
        .await
    }

    /// Fetch a single article.
    pub async fn fetch_article(&self, dict: UibDictionary, article_id: i64) -> Result<Value> {
        let url = format!(
            "https://ord.uib.no/{}/article/{}.json",
            dict.as_str(),
            article_id
        );
        self.with_retry(&format!("fetch_article({dict}, {article_id})"), || {
            let url = url.clone();
            let client = self.client.clone();
            async move {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json().await?)
            }
        })
        .await
    }

    /// Fetch dictionary concepts.
    pub async fn fetch_concepts(&self, dict: UibDictionary) -> Result<Value> {
        let url = format!("https://ord.uib.no/{}/concepts.json", dict.as_str());
        self.with_retry(&format!("fetch_concepts({dict})"), || {
            let url = url.clone();
            let client = self.client.clone();
            async move {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json().await?)
            }
        })
        .await
    }

    /// Fetch dictionary word classes.
    pub async fn fetch_word_classes(&self, dict: UibDictionary) -> Result<Value> {
        let url = format!("https://ord.uib.no/{}/fil/word_class.json", dict.as_str());
        self.with_retry(&format!("fetch_word_classes({dict})"), || {
            let url = url.clone();
            let client = self.client.clone();
            async move {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json().await?)
            }
        })
        .await
    }

    /// Fetch dictionary word subclasses.
    pub async fn fetch_word_subclasses(&self, dict: UibDictionary) -> Result<Value> {
        let url = format!(
            "https://ord.uib.no/{}/fil/sub_word_class.json",
            dict.as_str()
        );
        self.with_retry(&format!("fetch_word_subclasses({dict})"), || {
            let url = url.clone();
            let client = self.client.clone();
            async move {
                let resp = client.get(&url).send().await?.error_for_status()?;
                Ok(resp.json().await?)
            }
        })
        .await
    }

    /// Fetch a single bibliography entry by ID.
    pub async fn fetch_bibliography(&self, bibl_id: i64) -> Result<Value> {
        let api_key = self.clarino_key()?;
        let url = format!("https://clarino.uib.no/ordbank-api-prod/bibl/{bibl_id}");
        let key = api_key.to_string();
        self.with_retry(&format!("fetch_bibliography({bibl_id})"), || {
            let url = url.clone();
            let client = self.client.clone();
            let key = key.clone();
            async move {
                let resp = client
                    .get(&url)
                    .header("x-api-key", &key)
                    .send()
                    .await?
                    .error_for_status()?;
                let data: Value = resp.json().await?;
                data.as_array()
                    .and_then(|arr| arr.first())
                    .cloned()
                    .ok_or_else(|| anyhow!("Empty response for bibliography {bibl_id}"))
            }
        })
        .await
    }

    /// Fetch bibliography entries by code.
    pub async fn fetch_bibliography_by_code(&self, code: &str) -> Result<Vec<Value>> {
        let api_key = self.clarino_key()?;
        let encoded = urlencoding::encode(code);
        let url = format!("https://clarino.uib.no/ordbank-api-prod/bibl?code={encoded}");
        let key = api_key.to_string();
        self.with_retry(&format!("fetch_bibl_by_code({code})"), || {
            let url = url.clone();
            let client = self.client.clone();
            let key = key.clone();
            async move {
                let resp = client
                    .get(&url)
                    .header("x-api-key", &key)
                    .send()
                    .await?
                    .error_for_status()?;
                Ok(resp.json().await?)
            }
        })
        .await
    }

    /// Fetch a single place entry by ID.
    pub async fn fetch_place(&self, place_id: i64) -> Result<Value> {
        let api_key = self.clarino_key()?;
        let url = format!("https://clarino.uib.no/ordbank-api-prod/place/{place_id}");
        let key = api_key.to_string();
        self.with_retry(&format!("fetch_place({place_id})"), || {
            let url = url.clone();
            let client = self.client.clone();
            let key = key.clone();
            async move {
                let resp = client
                    .get(&url)
                    .header("x-api-key", &key)
                    .send()
                    .await?
                    .error_for_status()?;
                let data: Value = resp.json().await?;
                data.get(place_id.to_string())
                    .cloned()
                    .ok_or_else(|| anyhow!("Empty response for place {place_id}"))
            }
        })
        .await
    }

    /// Fetch a place by name.
    pub async fn fetch_place_by_name(&self, name: &str) -> Result<Option<(i64, Value)>> {
        let api_key = self.clarino_key()?;
        let encoded = urlencoding::encode(name);
        let url = format!("https://clarino.uib.no/ordbank-api-prod/place?place_name={encoded}");
        let key = api_key.to_string();

        let result: Result<Option<(i64, Value)>> = self
            .with_retry(&format!("fetch_place_by_name({name})"), || {
                let url = url.clone();
                let client = self.client.clone();
                let key = key.clone();
                async move {
                    let resp = client.get(&url).header("x-api-key", &key).send().await?;

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
            })
            .await;

        // 404 during retry shouldn't be retried.
        result
    }
}

impl std::fmt::Debug for UibClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UibClient")
            .field("has_clarino_key", &self.clarino_api_key.is_some())
            .field("max_retries", &self.max_retries)
            .finish_non_exhaustive()
    }
}

/// Determine if an error is transient.
#[must_use]
pub fn is_transient_error(err: &anyhow::Error) -> bool {
    if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>() {
        if reqwest_err.is_timeout() || reqwest_err.is_connect() || reqwest_err.is_request() {
            return true;
        }
        if let Some(status) = reqwest_err.status() {
            return status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS;
        }
        return true;
    }
    false
}
