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

mod article_sync_service;
#[cfg(feature = "matrix_notifs")]
mod matrix_notify_service;
mod meili;

use std::sync::Arc;

use anyhow::{Error, Result};
use apalis::prelude::*;
use apalis_board::axum::{
    framework::{ApiBuilder, RegisterRoute},
    ui::ServeUI,
};
use apalis_cron::CronStream;
use apalis_redis::{ConnectionManager, RedisConfig, RedisStorage};
use axum::Router;
use clap::value_parser;
use cron::Schedule;
use meilisearch_sdk::client::Client as MeiliClient;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info, warn};

use crate::article_sync_service::{
    ArticleSyncService, FetchArticleJob, FetchArticleListJob, FetchBibliographyJob,
    FetchDictionaryMetadataJob, UibDictionary,
};

#[cfg(feature = "matrix_notifs")]
use crate::article_sync_service::SendMatrixMessageJob;

#[derive(Default, Clone)]
struct JobTracker {
    inner: Arc<std::sync::Mutex<Vec<String>>>,
}

impl JobTracker {
    fn track(&self, description: String) -> JobGuard {
        self.inner.lock().unwrap().push(description.clone());
        JobGuard {
            tracker: self.clone(),
            description,
        }
    }

    fn active_jobs(&self) -> Vec<String> {
        self.inner.lock().unwrap().clone()
    }
}

struct JobGuard {
    tracker: JobTracker,
    description: String,
}

impl Drop for JobGuard {
    fn drop(&mut self) {
        let mut jobs = self.tracker.inner.lock().unwrap();
        if let Some(pos) = jobs.iter().position(|j| j == &self.description) {
            jobs.swap_remove(pos);
        }
    }
}

#[cfg(feature = "sentry_integration")]
use sentry::integrations::anyhow::capture_anyhow;

#[cfg(feature = "sentry_integration")]
fn init_sentry() -> Option<sentry::ClientInitGuard> {
    match std::env::var("SENTRY_ENDPOINT") {
        Ok(dsn) => {
            let guard = sentry::init((
                dsn,
                sentry::ClientOptions {
                    environment: std::env::var("ENVIRONMENT").ok().map(Into::into),
                    release: sentry::release_name!(),
                    ..Default::default()
                },
            ));
            Some(guard)
        }
        Err(_) => {
            warn!("SENTRY_ENDPOINT not set; skipping sentry initialization.");
            None
        }
    }
}

/// Create a RedisStorage with the given namespace.
fn redis_storage<T>(conn: &ConnectionManager, namespace: &str) -> RedisStorage<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
    RedisStorage::new_with_config(
        conn.clone(),
        RedisConfig::default().set_namespace(namespace),
    )
}

/// Register one or more workers with the monitor.
macro_rules! register_workers {
    ($monitor:expr, $svc:expr, $tracker:expr, $($name:literal => $backend:expr, $concurrency:expr, $handler:expr);+ $(;)?) => {{
        let monitor = $monitor;
        $(
            let s = $svc.clone();
            let t = $tracker.clone();
            let b = $backend;
            let monitor = monitor.register(move |_| {
                WorkerBuilder::new($name)
                    .backend(b.clone())
                    .concurrency($concurrency)
                    .data(s.clone())
                    .data(t.clone())
                    .build($handler)
            });
        )+
        monitor
    }};
}

fn redact_url_credentials(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after_scheme = &url[scheme_end + 3..];
        if let Some(at_pos) = after_scheme.find('@') {
            let host_onwards = &after_scheme[at_pos..];
            return format!("{}://***{}", &url[..scheme_end], host_onwards);
        }
    }
    url.to_string()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    #[cfg(feature = "use_dotenv")]
    dotenvy::dotenv().ok();

    #[cfg(feature = "sentry_integration")]
    let _sentry_guard = init_sentry();

    // Used to notify Matrix when events occur.
    #[cfg(feature = "matrix_notifs")]
    let mut matrix_service_opt: Option<matrix_notify_service::MatrixNotifyService> = None;

    // Inner run function which is wrapped to be able to report errors to Matrix.
    async fn run(
        #[cfg(feature = "matrix_notifs")] matrix_service_opt: &mut Option<
            matrix_notify_service::MatrixNotifyService,
        >,
    ) -> Result<()> {
        let matches = clap::Command::new("Ordbok API Worker")
            .version(env!("CARGO_PKG_VERSION"))
            .author("Adaline Simonian <adalinesimonian@gmail.com>")
            .about("Ordbok API worker service")
            .max_term_width(100)
            .arg(
                clap::Arg::new("log-level")
                    .long("log-level")
                    .short('l')
                    .value_name("LEVEL")
                    .help("Set the log level. If the log level is set by the RUST_LOG environment variable, this flag will be ignored.")
                    .global(true)
                    .default_value("info"),
            )
            .subcommand(
                clap::Command::new("resync")
                    .about("Resync all dictionaries")
                    .arg(
                        clap::Arg::new("dictionary")
                            .long("dictionary")
                            .short('d')
                            .value_name("DICTIONARY")
                            .help("Dictionary to resync. If omitted, all dictionaries will be resynced. Can be specified multiple times.")
                            .value_parser(value_parser!(UibDictionary))
                            .action(clap::ArgAction::Append),
                    ),
            )
            .get_matches();

        let log_level = matches
            .get_one::<String>("log-level")
            .map(String::as_str)
            .unwrap_or("info");

        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
            )
            .init();

        let resync_dictionaries = if let Some(subcmd) = matches.subcommand_matches("resync") {
            let dicts = subcmd
                .get_many::<UibDictionary>("dictionary")
                .unwrap_or_default()
                .cloned()
                .collect::<Vec<_>>();

            if dicts.is_empty() {
                info!("Resyncing all dictionaries.");
                UibDictionary::all().to_vec()
            } else {
                info!("Resyncing dictionaries: {dicts:?}");
                dicts
            }
        } else {
            vec![]
        };

        let num_workers = num_cpus::get();
        info!("Launching Ordbok API sync worker with up to {num_workers} threads.");

        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost:5432/ordbokapi".to_string());
        info!("Connecting to PostgreSQL…");

        let db = PgPoolOptions::new()
            .max_connections(20)
            .connect(&database_url)
            .await?;

        info!("Connected to PostgreSQL.");

        info!("Running database migrations…");
        sqlx::migrate!("./migrations").run(&db).await?;
        info!("Migrations complete.");

        let meili_url =
            std::env::var("MEILI_URL").unwrap_or_else(|_| "http://127.0.0.1:7700".to_string());
        let meili_key = std::env::var("MEILI_API_KEY").ok();
        info!(
            "Connecting to Meilisearch at {}…",
            redact_url_credentials(&meili_url)
        );

        let meili = MeiliClient::new(&meili_url, meili_key.as_deref())?;

        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        info!(
            "Connecting to Redis at {}… for job queues…",
            redact_url_credentials(&redis_url)
        );

        let redis_conn = apalis_redis::connect(redis_url).await?;

        let fetch_article_list_storage =
            redis_storage::<FetchArticleListJob>(&redis_conn, "apalis:article-list");
        let fetch_article_storage = redis_storage::<FetchArticleJob>(&redis_conn, "apalis:article");
        let fetch_dict_metadata_storage =
            redis_storage::<FetchDictionaryMetadataJob>(&redis_conn, "apalis:dict-metadata");
        let fetch_bibliography_storage =
            redis_storage::<FetchBibliographyJob>(&redis_conn, "apalis:bibliography");

        #[cfg(feature = "matrix_notifs")]
        let matrix_message_storage =
            redis_storage::<SendMatrixMessageJob>(&redis_conn, "apalis:matrix-notify");

        info!("Connected to Redis.");

        #[cfg(feature = "matrix_notifs")]
        {
            let ms = matrix_notify_service::MatrixNotifyService::new().await;
            *matrix_service_opt = Some(ms);
        }

        let health_db = db.clone();
        let health_redis = redis_conn.clone();
        let health_meili = meili.clone();

        let api = ApiBuilder::new(Router::new())
            .register(fetch_article_list_storage.clone())
            .register(fetch_article_storage.clone())
            .register(fetch_dict_metadata_storage.clone())
            .register(fetch_bibliography_storage.clone());

        #[cfg(feature = "matrix_notifs")]
        let api = api.register(matrix_message_storage.clone());

        let router = Router::new()
            .route(
                "/health",
                axum::routing::get(move || {
                    let db = health_db.clone();
                    let mut redis = health_redis.clone();
                    let meili = health_meili.clone();
                    async move {
                        let db_ok = sqlx::query("SELECT 1").execute(&db).await.is_ok();
                        let redis_ok = redis::cmd("PING")
                            .query_async::<String>(&mut redis)
                            .await
                            .is_ok();
                        let meili_ok = meili.health().await.is_ok();

                        if db_ok && redis_ok && meili_ok {
                            (
                                axum::http::StatusCode::OK,
                                axum::Json(serde_json::json!({"status": "ok"})),
                            )
                        } else {
                            (
                                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                                axum::Json(serde_json::json!({
                                    "status": "unhealthy",
                                    "db": db_ok,
                                    "redis": redis_ok,
                                    "meili": meili_ok,
                                })),
                            )
                        }
                    }
                }),
            )
            .nest("/api/v1", api.build())
            .fallback_service(ServeUI::new());

        let http_port = std::env::var("PORT").unwrap_or_else(|_| "3001".to_string());
        tokio::spawn(async move {
            let addr = format!("0.0.0.0:{http_port}");
            let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
            info!("HTTP server listening on port {http_port}");
            axum::serve(listener, router).await.unwrap();
        });

        meili::setup_indexes(&meili).await?;
        info!("Meilisearch indexes configured.");

        meili::reindex_if_needed(&meili, &db).await?;

        let sync_service = ArticleSyncService {
            db: db.clone(),
            meili: meili.clone(),
            redis_conn: redis_conn.clone(),
            clarino_api_key: std::env::var("CLARINO_API_KEY").ok(),
            fetch_article_list_storage: fetch_article_list_storage.clone(),
            fetch_article_storage: fetch_article_storage.clone(),
            fetch_dict_metadata_storage: fetch_dict_metadata_storage.clone(),
            fetch_bibliography_storage: fetch_bibliography_storage.clone(),
            #[cfg(feature = "matrix_notifs")]
            matrix_message_storage: matrix_message_storage.clone(),
        };

        if resync_dictionaries.is_empty() {
            info!("Checking if initial sync is required…");
            sync_service.initial_sync().await?;
        } else {
            // Flush all pending jobs before enqueueing fresh resync tasks.
            info!("Flushing existing job queues before resync…");
            redis::cmd("FLUSHDB")
                .query_async::<()>(&mut redis_conn.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to flush Redis: {e}"))?;
            info!("Job queues flushed.");

            for d in resync_dictionaries {
                info!("Enqueuing resync tasks for dictionary {:?}…", d);
                if let Err(e) = sync_service.enqueue_sync_articles(d).await {
                    error!("enqueue_sync_articles error: {e}");
                }
                if let Err(e) = sync_service.enqueue_sync_dict_metadata(d).await {
                    error!("enqueue_sync_dict_metadata error: {e}");
                }
            }
        }

        if let Err(e) = sync_service.enqueue_initial_bibliography_sync().await {
            error!("Initial bibliography sync enqueue failed: {e}");
        }

        let svc = sync_service.clone();
        let job_tracker = JobTracker::default();
        let article_concurrency = num_workers.clamp(4, 16);

        info!("Worker is now running. Press Ctrl+C to exit.");

        let monitor = register_workers!(Monitor::new(), svc, job_tracker,
            "fetch-article-list" => fetch_article_list_storage, 1, handle_fetch_article_list;
            "fetch-article" => fetch_article_storage, article_concurrency, handle_fetch_article;
            "fetch-dict-metadata" => fetch_dict_metadata_storage, 3, handle_fetch_dict_metadata;
            "fetch-bibliography" => fetch_bibliography_storage, 4, handle_fetch_bibliography;
        );

        // Set up cron for daily sync at 2 AM.
        let schedule: Schedule = "0 0 2 * * *".parse()?;
        let s = svc.clone();
        let t = job_tracker.clone();
        let monitor = monitor.register(move |_| {
            WorkerBuilder::new("daily-sync")
                .backend(CronStream::new(schedule.clone()))
                .data(s.clone())
                .data(t.clone())
                .build(handle_daily_sync)
        });

        #[cfg(feature = "matrix_notifs")]
        let monitor = {
            let ms = matrix_service_opt
                .clone()
                .unwrap_or_else(matrix_notify_service::MatrixNotifyService::empty);
            let t = job_tracker.clone();
            let b = matrix_message_storage;
            monitor.register(move |_| {
                WorkerBuilder::new("matrix-notify")
                    .backend(b.clone())
                    .concurrency(1)
                    .data(ms.clone())
                    .data(t.clone())
                    .build(handle_matrix_message)
            })
        };

        monitor
            .on_event(|_ctx, e| {
                let event_str = format!("{e:?}");
                if event_str.contains("Error") || event_str.contains("Failed") {
                    tracing::warn!("Worker event: {event_str}");
                }
            })
            .run_with_signal(graceful_shutdown(job_tracker))
            .await?;

        Ok(())
    }

    // Run the main logic and handle errors
    if let Err(err) = run(
        #[cfg(feature = "matrix_notifs")]
        &mut matrix_service_opt,
    )
    .await
    {
        error!("Application error: {err}");

        #[cfg(feature = "sentry_integration")]
        {
            capture_anyhow(&err);
        }

        // If MatrixNotifyService is available, send an alert
        #[cfg(feature = "matrix_notifs")]
        {
            if let Some(matrix_service) = matrix_service_opt.as_mut() {
                matrix_service
                    .send_message(
                        format!("@room 🚨 **Arbeidarprosessen kræsja**\n\n```{err}```").as_str(),
                    )
                    .await;
            }
        }

        return Err(err);
    }
    Ok(())
}

async fn handle_fetch_article_list(
    job: FetchArticleListJob,
    svc: Data<ArticleSyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Syncing article list for {}", job.dictionary));
    svc.handle_sync_articles(job).await
}

async fn handle_fetch_article(
    job: FetchArticleJob,
    svc: Data<ArticleSyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!(
        "Fetching article {} «{}» ({})",
        job.article_id, job.primary_lemma, job.dictionary
    ));
    svc.handle_sync_article(job).await
}

async fn handle_fetch_dict_metadata(
    job: FetchDictionaryMetadataJob,
    svc: Data<ArticleSyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Fetching metadata for {}", job.dictionary));
    svc.handle_sync_dictionary_metadata(job).await
}

async fn handle_fetch_bibliography(
    job: FetchBibliographyJob,
    svc: Data<ArticleSyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Fetching bibliography {}", job.bibl_id));
    svc.handle_sync_bibliography(job.bibl_id).await
}

async fn handle_daily_sync(
    _event: apalis_cron::Tick,
    svc: Data<ArticleSyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track("Daily sync".to_string());
    info!("2 AM daily sync tasks fired.");

    #[cfg(feature = "matrix_notifs")]
    svc.queue_matrix_message("Planlegg daglege synkroniseringsoppgåver.")
        .await;

    for d in UibDictionary::all() {
        if let Err(e) = svc.enqueue_sync_articles(*d).await {
            error!("enqueue_sync_articles error: {e}");
        }
        if let Err(e) = svc.enqueue_sync_dict_metadata(*d).await {
            error!("enqueue_sync_dict_metadata error: {e}");
        }
    }
    Ok(())
}

#[cfg(feature = "matrix_notifs")]
async fn handle_matrix_message(
    job: SendMatrixMessageJob,
    matrix_service: Data<matrix_notify_service::MatrixNotifyService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track("Sending Matrix message".to_string());
    matrix_service.send_message(&job.message).await;
    Ok(())
}

/// Shut down gracefully on Ctrl+C.
async fn graceful_shutdown(tracker: JobTracker) -> std::io::Result<()> {
    tokio::signal::ctrl_c().await?;

    info!("Received Ctrl+C, shutting down gracefully...");

    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let active = tracker.active_jobs();
        if active.is_empty() {
            warn!("Still shutting down... Press Ctrl+C again to force exit.");
        } else {
            warn!(
                "Still shutting down, waiting for {} in-flight job(s):\n{}\n\
                 Press Ctrl+C again to force exit.",
                active.len(),
                active
                    .iter()
                    .map(|j| format!("  - {j}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
        error!("Second Ctrl+C received, forcing exit.");
        std::process::exit(1);
    });

    Ok(())
}
