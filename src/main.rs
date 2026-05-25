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

mod extraction;
mod indexing;
mod jobs;
#[cfg(feature = "matrix_notifs")]
mod matrix_notify_service;
mod meili;
mod outbox;
mod state;
mod storage;
mod sync_service;
mod uib_client;
mod web;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use anyhow::{Error, Result};
use apalis::prelude::*;
use apalis_cron::CronStream;
use apalis_redis::{ConnectionManager, RedisConfig, RedisStorage};
use axum::Router;
use clap::value_parser;
use cron::Schedule;
use meilisearch_sdk::client::Client as MeiliClient;
use rand::RngExt;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info, warn};

use crate::jobs::{
    BackfillInlineRefsJob, BatchIndexJob, FetchArticleJob, FetchArticleListJob,
    FetchBibliographyJob, FetchDictionaryMetadataJob, FetchPlaceJob, IndexArticleJob,
    ResolveInlineCodeJob, SweepJob,
};
use crate::state::UibDictionary;
use crate::sync_service::SyncService;
use crate::uib_client::UibClient;

#[cfg(feature = "matrix_notifs")]
use crate::jobs::SendMatrixMessageJob;

#[derive(Default, Clone)]
pub(crate) struct JobTracker {
    inner: Arc<std::sync::Mutex<Vec<String>>>,
}

impl JobTracker {
    pub(crate) fn track(&self, description: String) -> JobGuard {
        self.inner.lock().unwrap().push(description.clone());
        JobGuard {
            tracker: self.clone(),
            description,
        }
    }

    pub(crate) fn active_jobs(&self) -> Vec<String> {
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

/// Check if error is a transient error, e.g. a temporary network issue, that
/// warrants a retry.
#[allow(clippy::borrowed_box)] // Required due to Apalis's retry_if API signature.
fn is_transient_job_error(err: &Box<dyn std::error::Error + Send + Sync + 'static>) -> bool {
    let mut source: Option<&dyn std::error::Error> = Some(err.as_ref());
    while let Some(e) = source {
        if let Some(reqwest_err) = e.downcast_ref::<reqwest::Error>() {
            if reqwest_err.is_timeout() || reqwest_err.is_connect() || reqwest_err.is_request() {
                return true;
            }
            return reqwest_err.status().is_none_or(|s| {
                s.is_server_error() || s == reqwest::StatusCode::TOO_MANY_REQUESTS
            });
        }
        source = e.source();
    }
    // Assume the error is transient if we can't get more details.
    true
}

#[cfg(feature = "sentry_integration")]
use sentry::integrations::anyhow::capture_anyhow;

#[cfg(feature = "sentry_integration")]
fn init_sentry() -> Option<sentry::ClientInitGuard> {
    std::env::var("SENTRY_ENDPOINT")
        .ok()
        .map(|dsn| {
            sentry::init((
                dsn,
                sentry::ClientOptions {
                    environment: std::env::var("ENVIRONMENT").ok().map(Into::into),
                    release: sentry::release_name!(),
                    ..Default::default()
                },
            ))
        })
        .or_else(|| {
            warn!("SENTRY_ENDPOINT not set; skipping sentry initialization.");
            None
        })
}

fn redis_storage<T>(conn: &ConnectionManager, namespace: &str) -> RedisStorage<T>
where
    T: Serialize + DeserializeOwned + Send + Sync + Unpin + 'static,
{
    RedisStorage::new_with_config(
        conn.clone(),
        RedisConfig::default().set_namespace(namespace),
    )
}

macro_rules! register_workers {
    ($monitor:expr, $svc:expr, $tracker:expr, $retry:expr, $instance_id:expr, $($name:literal => $backend:expr, $concurrency:expr, $handler:expr);+ $(;)?) => {{
        let monitor = $monitor;
        $(
            let s = $svc.clone();
            let t = $tracker.clone();
            let b = $backend;
            let r = $retry.clone();
            let worker_name = format!("{}-{}", $name, $instance_id);
            let monitor = monitor.register(move |_| {
                WorkerBuilder::new(&worker_name)
                    .backend(b.clone())
                    .retry(r.clone())
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

    #[cfg(feature = "matrix_notifs")]
    let mut matrix_service_opt: Option<matrix_notify_service::MatrixNotifyService> = None;

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

#[allow(clippy::too_many_lines)]
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
        .map_or("info", String::as_str);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    let resync_dictionaries =
        matches
            .subcommand_matches("resync")
            .map_or_else(Vec::new, |subcmd| {
                let dicts = subcmd
                    .get_many::<UibDictionary>("dictionary")
                    .unwrap_or_default()
                    .copied()
                    .collect::<Vec<_>>();

                if dicts.is_empty() {
                    info!("Resyncing all dictionaries.");
                    UibDictionary::all().to_vec()
                } else {
                    info!("Resyncing dictionaries: {dicts:?}");
                    dicts
                }
            });

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
        "Connecting to Redis at {}…",
        redact_url_credentials(&redis_url)
    );

    let redis_conn = apalis_redis::connect(redis_url).await?;

    let fetch_article_list_storage =
        redis_storage::<FetchArticleListJob>(&redis_conn, FetchArticleListJob::NAMESPACE);
    let fetch_article_storage =
        redis_storage::<FetchArticleJob>(&redis_conn, FetchArticleJob::NAMESPACE);
    let index_article_storage =
        redis_storage::<IndexArticleJob>(&redis_conn, IndexArticleJob::NAMESPACE);
    let batch_index_storage = redis_storage::<BatchIndexJob>(&redis_conn, BatchIndexJob::NAMESPACE);
    let fetch_dict_metadata_storage = redis_storage::<FetchDictionaryMetadataJob>(
        &redis_conn,
        FetchDictionaryMetadataJob::NAMESPACE,
    );
    let fetch_bibliography_storage =
        redis_storage::<FetchBibliographyJob>(&redis_conn, FetchBibliographyJob::NAMESPACE);
    let fetch_place_storage = redis_storage::<FetchPlaceJob>(&redis_conn, FetchPlaceJob::NAMESPACE);
    let backfill_inline_refs_storage =
        redis_storage::<BackfillInlineRefsJob>(&redis_conn, BackfillInlineRefsJob::NAMESPACE);
    let resolve_inline_code_storage =
        redis_storage::<ResolveInlineCodeJob>(&redis_conn, ResolveInlineCodeJob::NAMESPACE);
    let _sweep_storage = redis_storage::<SweepJob>(&redis_conn, SweepJob::NAMESPACE);

    #[cfg(feature = "matrix_notifs")]
    let matrix_message_storage =
        redis_storage::<SendMatrixMessageJob>(&redis_conn, SendMatrixMessageJob::NAMESPACE);

    info!("Connected to Redis.");

    #[cfg(feature = "matrix_notifs")]
    {
        let ms = matrix_notify_service::MatrixNotifyService::new().await;
        *matrix_service_opt = Some(ms);
    }

    let job_tracker = JobTracker::default();

    let health_db = db.clone();
    let health_redis = redis_conn.clone();
    let health_meili = meili.clone();

    let web_state = web::WebState {
        db: db.clone(),
        redis_conn: redis_conn.clone(),
        job_tracker: job_tracker.clone(),
        fetch_article_list_storage: fetch_article_list_storage.clone(),
        fetch_dict_metadata_storage: fetch_dict_metadata_storage.clone(),
        secret_hash: std::env::var("MGMT_UI_SECRET_HASH").ok(),
    };

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
        .merge(web::routes(web_state));

    let http_port = std::env::var("PORT").unwrap_or_else(|_| "3001".to_string());
    tokio::spawn(async move {
        let addr = format!("0.0.0.0:{http_port}");
        let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
        info!("HTTP server listening on port {http_port}");
        axum::serve(listener, router).await.unwrap();
    });

    info!("Running database migrations…");
    sqlx::migrate!("./migrations").run(&db).await?;
    info!("Migrations complete.");

    meili::setup_indexes(&meili).await?;
    info!("Meilisearch indexes configured.");

    meili::reindex_if_needed(&meili, &db).await?;

    let http_client = UibClient::new(std::env::var("CLARINO_API_KEY").ok());

    let sync_service = SyncService {
        db: db.clone(),
        meili: meili.clone(),
        http: http_client,
        redis_conn: redis_conn.clone(),
        inline_refs_backfill_count: Arc::new(AtomicU64::new(0)),
        #[cfg(feature = "matrix_notifs")]
        matrix_message_storage: matrix_message_storage.clone(),
    };

    let outbox_db = db.clone();
    let outbox_storages = outbox::OutboxStorages {
        fetch_article: fetch_article_storage.clone(),
        index_article: index_article_storage.clone(),
        batch_index: batch_index_storage.clone(),
        fetch_bibliography: fetch_bibliography_storage.clone(),
        fetch_place: fetch_place_storage.clone(),
        resolve_inline_code: resolve_inline_code_storage.clone(),
    };
    tokio::spawn(async move {
        outbox::run_outbox_poller(outbox_db, outbox_storages).await;
    });

    if resync_dictionaries.is_empty() {
        info!("Checking if initial sync is required…");
        sync_service
            .initial_sync(&fetch_article_list_storage, &fetch_dict_metadata_storage)
            .await?;
    } else {
        sync_service
            .enqueue_resync(
                &resync_dictionaries,
                &fetch_article_list_storage,
                &fetch_dict_metadata_storage,
            )
            .await?;
    }

    if let Err(e) = sync_service.enqueue_initial_bibliography_sync().await {
        error!("Initial bibliography sync enqueue failed: {e}");
    }

    if let Err(e) = sync_service
        .enqueue_inline_refs_backfill(&backfill_inline_refs_storage)
        .await
    {
        error!("Inline reference backfill enqueue failed: {e}");
    }

    if let Err(e) = sync_service.enqueue_initial_place_sync().await {
        error!("Initial place sync enqueue failed: {e}");
    }

    let svc = sync_service.clone();
    let article_concurrency = num_workers.clamp(4, 16);

    let daily_fal = fetch_article_list_storage.clone();
    let daily_fdm = fetch_dict_metadata_storage.clone();

    let instance_id: String = {
        let bytes: [u8; 4] = rand::rng().random();
        format!("{:08x}", u32::from_ne_bytes(bytes))
    };

    info!("Worker instance {instance_id} is now running. Press Ctrl+C to exit.");

    let retry_policy =
        apalis::layers::retry::RetryPolicy::retries(3).retry_if(is_transient_job_error);

    let monitor = register_workers!(Monitor::new(), svc, job_tracker, retry_policy, &instance_id,
        "fetch-article-list" => fetch_article_list_storage, 1, handle_fetch_article_list;
        "fetch-article" => fetch_article_storage, article_concurrency, handle_fetch_article;
        "index-article" => index_article_storage, article_concurrency, handle_index_article;
        "batch-index" => batch_index_storage, 2, handle_batch_index;
        "fetch-dict-metadata" => fetch_dict_metadata_storage, 3, handle_fetch_dict_metadata;
        "fetch-bibliography" => fetch_bibliography_storage, 4, handle_fetch_bibliography;
        "fetch-place" => fetch_place_storage, 4, handle_fetch_place;
        "backfill-inline-refs" => backfill_inline_refs_storage, 4, handle_backfill_inline_refs;
        "resolve-inline-code" => resolve_inline_code_storage, 2, handle_resolve_inline_code;
    );

    // Sweep every 5 minutes.
    let sweep_schedule: Schedule = "0 */5 * * * *".parse()?;
    let s = svc.clone();
    let t = job_tracker.clone();
    let sweep_name = format!("sweep-{instance_id}");
    let monitor = monitor.register(move |_| {
        WorkerBuilder::new(&sweep_name)
            .backend(CronStream::new(sweep_schedule.clone()))
            .data(s.clone())
            .data(t.clone())
            .build(handle_sweep)
    });

    // Daily sync at midnight. Can be overriden with the SYNC_SCHEDULE env var.
    let daily_schedule: Schedule = std::env::var("SYNC_SCHEDULE")
        .unwrap_or_else(|_| "0 0 0 * * *".to_string())
        .parse()?;
    let s = svc.clone();
    let t = job_tracker.clone();
    let daily_name = format!("daily-sync-{instance_id}");
    let monitor = monitor.register(move |_| {
        WorkerBuilder::new(&daily_name)
            .backend(CronStream::new(daily_schedule.clone()))
            .data(s.clone())
            .data(t.clone())
            .data(daily_fal.clone())
            .data(daily_fdm.clone())
            .build(handle_daily_sync)
    });

    #[cfg(feature = "matrix_notifs")]
    let monitor = {
        let ms = matrix_service_opt
            .clone()
            .unwrap_or_else(matrix_notify_service::MatrixNotifyService::empty);
        let t = job_tracker.clone();
        let b = matrix_message_storage;
        let matrix_name = format!("matrix-notify-{instance_id}");
        monitor.register(move |_| {
            WorkerBuilder::new(&matrix_name)
                .backend(b.clone())
                .concurrency(1)
                .data(ms.clone())
                .data(t.clone())
                .build(handle_matrix_message)
        })
    };

    monitor
        .should_restart(|ctx, error, attempt| {
            if matches!(error, WorkerError::GracefulExit) {
                return false;
            }
            let name = ctx.name();
            if attempt < 5 {
                tracing::warn!("Worker {name} failed (attempt {attempt}): {error:?}. Restarting…");
                true
            } else {
                tracing::error!(
                    "Worker {name} failed after {attempt} restart attempts. Giving up."
                );
                false
            }
        })
        .on_event(|ctx, e| match e {
            Event::Error(_) => {
                let event_str = format!("{e:?}");
                tracing::warn!("Worker event: {event_str}");
            }
            Event::Stop => {
                let name = ctx.name();
                if !name.starts_with("sweep-")
                    && !name.starts_with("daily-sync-")
                    && !name.starts_with("matrix-notify-")
                    && !SHUTTING_DOWN.load(Ordering::Relaxed)
                {
                    tracing::error!(
                        "Queue worker {name} stopped unexpectedly. Exiting process for platform restart."
                    );
                    std::process::exit(1);
                }
            }
            _ => {}
        })
        .run_with_signal(graceful_shutdown(job_tracker))
        .await?;

    Ok(())
}

async fn handle_fetch_article_list(
    job: FetchArticleListJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Syncing article list for {}", job.dictionary));
    svc.handle_fetch_article_list(job).await
}

async fn handle_fetch_article(
    job: FetchArticleJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!(
        "Fetching article {}:{}",
        job.dictionary, job.article_id
    ));
    svc.handle_fetch_article(job).await
}

async fn handle_index_article(
    job: IndexArticleJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!(
        "Indexing article {}:{}",
        job.dictionary, job.article_id
    ));
    svc.handle_index_article(job).await
}

async fn handle_batch_index(
    job: BatchIndexJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!(
        "Batch indexing {} articles",
        job.article_keys.len()
    ));
    svc.handle_batch_index(job).await
}

async fn handle_fetch_dict_metadata(
    job: FetchDictionaryMetadataJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Fetching metadata for {}", job.dictionary));
    svc.handle_fetch_dict_metadata(job).await
}

async fn handle_fetch_bibliography(
    job: FetchBibliographyJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Fetching bibliography {}", job.bibl_id));
    svc.handle_fetch_bibliography(job).await
}

async fn handle_fetch_place(
    job: FetchPlaceJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Fetching place {}", job.place_id));
    svc.handle_fetch_place(job).await
}

async fn handle_backfill_inline_refs(
    job: BackfillInlineRefsJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!(
        "Backfill inline refs for {} articles",
        job.article_ids.len()
    ));
    svc.handle_backfill_inline_refs(job).await
}

async fn handle_resolve_inline_code(
    job: ResolveInlineCodeJob,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track(format!("Resolve inline code '{}'", job.code));
    svc.handle_resolve_inline_code(job).await
}

async fn handle_sweep(
    _event: apalis_cron::Tick,
    svc: Data<SyncService>,
    tracker: Data<JobTracker>,
) -> Result<(), Error> {
    let _guard = tracker.track("Sweep stuck items".to_string());
    svc.handle_sweep().await
}

async fn handle_daily_sync(
    _event: apalis_cron::Tick,
    #[cfg_attr(not(feature = "matrix_notifs"), allow(unused_variables))] svc: Data<SyncService>,
    tracker: Data<JobTracker>,
    fetch_article_list_storage: Data<RedisStorage<FetchArticleListJob>>,
    fetch_dict_metadata_storage: Data<RedisStorage<FetchDictionaryMetadataJob>>,
) -> Result<(), Error> {
    let _guard = tracker.track("Daily sync".to_string());
    info!("Daily sync tasks fired.");

    #[cfg(feature = "matrix_notifs")]
    svc.queue_matrix_message("Planlegg daglege synkroniseringsoppgåver.")
        .await;

    for d in UibDictionary::all() {
        let mut fal = (*fetch_article_list_storage).clone();
        if let Err(e) = fal
            .push(FetchArticleListJob {
                dictionary: d.as_str().to_string(),
            })
            .await
        {
            error!("enqueue article list error: {e}");
        }

        let mut fdm = (*fetch_dict_metadata_storage).clone();
        if let Err(e) = fdm
            .push(FetchDictionaryMetadataJob {
                dictionary: d.as_str().to_string(),
            })
            .await
        {
            error!("enqueue dict metadata error: {e}");
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

static SHUTTING_DOWN: AtomicBool = AtomicBool::new(false);

async fn graceful_shutdown(tracker: JobTracker) -> std::io::Result<()> {
    tokio::signal::ctrl_c().await?;

    SHUTTING_DOWN.store(true, Ordering::Relaxed);

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
