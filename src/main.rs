mod article_sync_service;
#[cfg(feature = "matrix_notifs")]
mod matrix_notify_service;
mod migration;
mod queue;

use std::collections::HashMap;

use anyhow::Result;
use clap::value_parser;
use log::{error, info};
use queue::get_dict_id;
#[cfg(feature = "sentry_integration")]
use sentry::integrations::anyhow::capture_anyhow;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::article_sync_service::{ArticleSyncService, UibDictionary};
#[cfg(feature = "matrix_notifs")]
use crate::matrix_notify_service::MatrixNotifyService;
use crate::migration::MigrationService;
use crate::queue::{JobPayload, JobQueue, JobQueueService};

#[cfg(feature = "sentry_integration")]
fn init_sentry() -> sentry::ClientInitGuard {
    let dsn = std::env::var("SENTRY_ENDPOINT")
        .expect("SENTRY_ENDPOINT must be set when using the sentry_integration feature.");
    sentry::init((
        dsn,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            ..Default::default()
        },
    ))
}

#[derive(serde::Serialize)]
struct ConsumerStatus {
    consumer_name: String,
    pending: i64,
}

#[derive(serde::Serialize)]
struct PendingDetail {
    message_id: String,
    consumer: String,
    idle_time_ms: i64,
    delivery_count: i64,
    job_payload: String,
}

#[derive(serde::Serialize)]
struct QueueStatus {
    queue: String,
    stream_key: String,
    total_messages: i64,
    pending_total: i64,
    min_id: String,
    max_id: String,
    consumer_stats: Vec<ConsumerStatus>,
    pending_details: Vec<PendingDetail>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    #[cfg(feature = "dotenv")]
    dotenv::dotenv().ok();

    #[cfg(feature = "sentry_integration")]
    let _sentry_guard = init_sentry();

    // Used to notify Matrix when events occur.
    #[cfg(feature = "matrix_notifs")]
    let mut matrix_service_opt = None;

    // Inner run function which is wrapped to be able to report errors to Matrix.
    async fn run(
        #[cfg(feature = "matrix_notifs")] matrix_service_opt: &mut Option<MatrixNotifyService>,
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
            .subcommand(
                clap::Command::new("queues")
                    .about("Queue operations")
                    .subcommand(
                        clap::Command::new("flush")
                            .long_about("Flush all queues, removing all pending jobs.")
                            .after_long_help("This is a destructive operation and cannot be undone. If you have flushed all queues and the dictionaries are not in sync, you will need to either resync the dictionaries manually or wait for the next scheduled sync.")
                            .visible_alias("flushqueues")
                            .arg(
                                clap::Arg::new("yes")
                                    .long("yes")
                                    .short('y')
                                    .help("Skip the confirmation prompt and flush all queues immediately.")
                                    .action(clap::ArgAction::SetTrue),
                            ),
                    )
                    .subcommand(
                        clap::Command::new("status")
                            .about("Display the status of the job queues")
                            .arg(
                                clap::Arg::new("output")
                                    .long("output")
                                    .short('o')
                                    .value_name("FORMAT")
                                    .help("Output format: text or json")
                                    .default_value("text")
                                    .value_parser(["text", "json"]),
                            ),
                    )
                    .subcommand(
                        clap::Command::new("resync")
                            .about("Queue a resync of all dictionaries without running them immediately")
                            .after_long_help("This command queues resync tasks for dictionaries and then exits.")
                            .arg(
                                clap::Arg::new("dictionary")
                                    .long("dictionary")
                                    .short('d')
                                    .value_name("DICTIONARY")
                                    .help("Dictionary to resync. If omitted, all dictionaries will be queued for resync. Can be specified multiple times.")
                                    .value_parser(value_parser!(UibDictionary))
                                    .action(clap::ArgAction::Append),
                            ),
                    )
            )
            .get_matches();

        let log_level = matches
            .get_one::<String>("log-level")
            .map(String::as_str)
            .unwrap_or("info");

        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
            .init();

        // Handle the queues subcommand branch.
        if let Some(("queues", queues_sub)) = matches.subcommand() {
            match queues_sub.subcommand() {
                Some(("flush", subcmd)) => {
                    if !subcmd.get_flag("yes") {
                        eprintln!("Flushing all queues is a destructive operation and cannot be undone. Are you sure you want to continue? (y/N)");
                        let mut input = String::new();
                        std::io::stdin().read_line(&mut input)?;
                        if input.trim().to_lowercase() != "y" {
                            eprintln!("Received input other than 'y', exiting.");
                            return Ok(());
                        }
                    } else {
                        eprintln!("--yes/-y passed to flush, skipping confirmation prompt.");
                    };

                    eprintln!("Flushing all queues…");
                    // Connect to Redis.
                    let redis_url = std::env::var("REDIS_URL")
                        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
                    let redis_client = redis::Client::open(redis_url.clone())?;
                    let conn_info = redis_client.get_connection_info();

                    eprintln!("Connecting to Redis at {}…", conn_info.addr);

                    let mut job_queue = JobQueueService {
                        redis_client: redis_client.clone(),
                        redis_connection_manager: redis_client.get_connection_manager().await?,
                    };
                    job_queue.setup_consumer_groups(true).await?;
                    eprintln!("All queues have been flushed.");
                    return Ok(());
                }
                Some(("status", subcmd)) => {
                    let output_format = subcmd
                        .get_one::<String>("output")
                        .map(String::as_str)
                        .unwrap_or("text");
                    let redis_url = std::env::var("REDIS_URL")
                        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
                    let redis_client = redis::Client::open(redis_url.clone())?;
                    let conn_info = redis_client.get_connection_info();

                    eprintln!("Connecting to Redis at {}…", conn_info.addr);

                    handle_queue_status(&redis_client, output_format).await?;
                    return Ok(());
                }
                Some(("resync", subcmd)) => {
                    let redis_url = std::env::var("REDIS_URL")
                        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
                    let redis_client = redis::Client::open(redis_url.clone())?;
                    let conn_info = redis_client.get_connection_info();
                    eprintln!("Connecting to Redis at {}…", conn_info.addr);

                    // Create separate connection managers for queues and data.
                    let redis_connection_manager_queues =
                        redis_client.get_connection_manager().await?;
                    let redis_connection_manager_data =
                        redis_client.get_connection_manager().await?;
                    let mut job_queue = JobQueueService {
                        redis_client: redis_client.clone(),
                        redis_connection_manager: redis_connection_manager_queues.clone(),
                    };
                    job_queue.setup_consumer_groups(false).await?;

                    #[cfg(feature = "matrix_notifs")]
                    let matrix_service = MatrixNotifyService::new(job_queue.clone()).await;

                    let mut sync_service = ArticleSyncService {
                        redis_client: redis_connection_manager_data.clone(),
                        job_queue: job_queue.clone(),
                        #[cfg(feature = "matrix_notifs")]
                        matrix_service,
                    };

                    let dicts = subcmd
                        .get_many::<UibDictionary>("dictionary")
                        .unwrap_or_default()
                        .cloned()
                        .collect::<Vec<_>>();

                    let dicts = if dicts.is_empty() {
                        eprintln!("Queueing resync for all dictionaries.");
                        UibDictionary::all().to_vec()
                    } else {
                        eprintln!("Queueing resync for dictionaries: {:?}", dicts);
                        dicts
                    };

                    for d in dicts {
                        eprintln!("Queueing resync tasks for dictionary {:?}…", d);
                        if let Err(e) = sync_service.enqueue_sync_articles(d).await {
                            eprintln!("enqueue_sync_articles error: {e}");
                        }
                        if let Err(e) = sync_service.enqueue_sync_dict_metadata(d).await {
                            eprintln!("enqueue_sync_dict_metadata error: {e}");
                        }
                    }
                    eprintln!("Resync tasks have been queued. Exiting.");
                    return Ok(());
                }
                _ => {}
            }
        }

        let resync_dictionaries = if let Some("resync") = matches.subcommand_name() {
            let subcmd = matches.subcommand_matches("resync").unwrap();
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

        // Connect to Redis
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let redis_client = redis::Client::open(redis_url.clone())?;
        let conn_info = redis_client.get_connection_info();

        info!("Connecting to Redis at {}…", conn_info.addr);

        let redis_connection_manager_queues = redis_client.get_connection_manager().await?;
        let redis_connection_manager_data = redis_client.get_connection_manager().await?;

        info!("Connected to Redis.");

        // Run migrations.
        let mut migrator = MigrationService {
            redis_client: redis_connection_manager_data.clone(),
        };
        if let Err(e) = migrator.migrate().await {
            error!("Failed to run Redis migrations: {e:?}");
            return Err(e);
        }

        // Setup consumer groups.
        let mut job_queue = JobQueueService {
            redis_client: redis_client.clone(),
            redis_connection_manager: redis_connection_manager_queues.clone(),
        };
        job_queue.setup_consumer_groups(false).await?;

        // Initialize MatrixNotifyService
        #[cfg(feature = "matrix_notifs")]
        {
            let matrix_service = MatrixNotifyService::new(job_queue.clone()).await;
            *matrix_service_opt = Some(matrix_service);
        }

        // Create the ArticleSyncService.
        let mut sync_service = ArticleSyncService {
            redis_client: redis_connection_manager_data.clone(),
            job_queue: job_queue.clone(),
            #[cfg(feature = "matrix_notifs")]
            matrix_service: matrix_service_opt
                .as_ref()
                .expect("MatrixNotifyService not initialized.")
                .clone(),
        };

        // Perform initial sync checks.
        if resync_dictionaries.is_empty() {
            info!("Checking if initial sync is required…");
            sync_service.initial_sync().await?;
        } else {
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

        // Set up cron for daily tasks at 2 AM.
        info!("Setting up daily sync tasks at 2 AM…");
        let sched = JobScheduler::new().await?;
        {
            let svc = sync_service.clone();
            #[cfg(feature = "matrix_notifs")]
            let matrix_svc = matrix_service_opt.clone();
            sched
                .add(Job::new_async("0 0 2 * * *", move |_uuid, _l| {
                    let mut svc2 = svc.clone();
                    #[cfg(feature = "matrix_notifs")]
                    let matrix_svc2 = matrix_svc.clone();
                    Box::pin(async move {
                        info!("2 AM daily sync tasks fired.");
                        #[cfg(feature = "matrix_notifs")]
                        {
                            matrix_svc2
                                .expect("MatrixNotifyService not initialized.")
                                .queue_message("Planlegg daglege synkroniseringsoppgåver.")
                                .await;
                        }
                        for d in UibDictionary::all() {
                            if let Err(e) = svc2.enqueue_sync_articles(*d).await {
                                error!("enqueue_sync_articles error: {e}");
                            }
                            if let Err(e) = svc2.enqueue_sync_dict_metadata(*d).await {
                                error!("enqueue_sync_dict_metadata error: {e}");
                            }
                        }
                    })
                })?)
                .await?;
        }
        sched.start().await?;

        // Start worker tasks for each queue.
        {
            info!("Starting worker group for FetchDictionaryMetadata…");
            let svc = sync_service.clone();
            job_queue
                .start_worker_group(
                    JobQueue::FetchDictionaryMetadata,
                    3,
                    1,
                    move |job: JobPayload| {
                        let mut svc2 = svc.clone();
                        async move {
                            let dict = match get_dict_id(&job) {
                                Ok(d) => d,
                                Err(e) => {
                                    error!("{e}");
                                    return Ok(());
                                }
                            };
                            svc2.handle_sync_dictionary_metadata(dict).await
                        }
                    },
                )
                .await?;
        }
        {
            info!("Starting worker group for FetchArticleList…");
            let svc = sync_service.clone();
            job_queue
                .start_worker_group(JobQueue::FetchArticleList, 3, 1, move |job: JobPayload| {
                    let mut svc2 = svc.clone();
                    async move {
                        let dict = match get_dict_id(&job) {
                            Ok(d) => d,
                            Err(e) => {
                                error!("{e}");
                                return Ok(());
                            }
                        };
                        svc2.handle_sync_articles(dict).await
                    }
                })
                .await?;
        }
        {
            info!("Starting worker group for FetchArticle…");
            let svc = sync_service.clone();
            job_queue
                // CPU thread count = worker count, except minimum 4 workers, maximum 16 workers.
                .start_worker_group(
                    JobQueue::FetchArticle,
                    // Calculate worker count.
                    num_workers.clamp(4, 16),
                    1,
                    move |job: JobPayload| {
                        let mut svc2 = svc.clone();
                        async move {
                            let dict = match get_dict_id(&job) {
                                Ok(d) => d,
                                Err(e) => {
                                    error!("{e}");
                                    return Ok(());
                                }
                            };
                            let meta_val =
                                job.data.get("metadata").unwrap_or(&serde_json::Value::Null);
                            svc2.handle_sync_article(dict, meta_val).await
                        }
                    },
                )
                .await?;
        }
        #[cfg(feature = "matrix_notifs")]
        {
            info!("Starting worker group for MatrixNotify…");
            let svc = matrix_service_opt.clone();
            job_queue
                .start_worker_group(JobQueue::MatrixNotify, 1, 1, move |job: JobPayload| {
                    let svc2 = svc.clone();
                    async move {
                        let msg = job.data.get("msg").unwrap_or(&serde_json::Value::Null);
                        svc2.expect("MatrixNotifyService not initialized.")
                            .send_message(msg.as_str().unwrap_or_default())
                            .await;
                        Ok(())
                    }
                })
                .await?;
        }

        // Idle forever.
        info!("Worker is now running. Press Ctrl+C to exit.");
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
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

async fn handle_queue_status(redis_client: &redis::Client, output_format: &str) -> Result<()> {
    let conn = redis_client.get_connection_manager().await?;

    // List of queues. MatrixNotify is added only if the feature is enabled.
    let queues = vec![
        crate::queue::JobQueue::FetchArticleList,
        crate::queue::JobQueue::FetchArticle,
        crate::queue::JobQueue::FetchDictionaryMetadata,
        #[cfg(feature = "matrix_notifs")]
        crate::queue::JobQueue::MatrixNotify,
    ];

    const EXTENDED_LIMIT: usize = 10;

    // Helper function to retrieve and pretty-print a job's payload.
    async fn get_job_payload(
        conn: &mut redis::aio::ConnectionManager,
        stream_key: &str,
        message_id: &str,
    ) -> String {
        let range: Vec<(String, HashMap<String, String>)> = redis::cmd("XRANGE")
            .arg(stream_key)
            .arg(message_id)
            .arg(message_id)
            .query_async(conn)
            .await
            .unwrap_or_default();

        range
            .first()
            .and_then(|(_id, fields)| fields.get("payload"))
            .and_then(|payload_str| serde_json::from_str::<serde_json::Value>(payload_str).ok())
            .map(|json_val| {
                serde_json::to_string_pretty(&json_val)
                    .unwrap_or_else(|_| "Could not parse job payload.".to_string())
            })
            .unwrap_or_else(|| "Could not parse job payload.".to_string())
    }

    // Helper function to pretty-print a duration in milliseconds, from a number of
    // milliseconds into a string like "6d 01:49:39.13".
    fn format_duration(ms: i64) -> String {
        let total_seconds = ms as f64 / 1000.0;
        let days = (total_seconds / 86400.0).floor() as i64;
        let rem_after_days = total_seconds - (days as f64 * 86400.0);
        let hours = (rem_after_days / 3600.0).floor() as i64;
        let rem_after_hours = rem_after_days - (hours as f64 * 3600.0);
        let minutes = (rem_after_hours / 60.0).floor() as i64;
        let seconds = rem_after_hours - (minutes as f64 * 60.0);
        if days > 0 {
            format!("{}d {:02}:{:02}:{:05.2}", days, hours, minutes, seconds)
        } else {
            format!("{:02}:{:02}:{:05.2}", hours, minutes, seconds)
        }
    }

    // Process all queues concurrently.
    let statuses = futures::future::join_all(queues.into_iter().map(|q| {
        // Clone the connection manager for each concurrent task.
        let mut conn = conn.clone();
        async move {
            let stream_key = q.to_stream_key();
            let group_name = q.to_group_name();

            // Retrieve XPENDING summary.
            let xp_summary: (i64, String, String, Vec<(String, i64)>) = redis::cmd("XPENDING")
                .arg(stream_key)
                .arg(group_name)
                .query_async(&mut conn)
                .await
                .unwrap_or((0, "-".to_string(), "-".to_string(), vec![]));
            let (pending_total, min_id, max_id, consumer_stats_raw) = xp_summary;
            let consumer_stats = consumer_stats_raw
                .into_iter()
                .map(|(name, pending)| ConsumerStatus {
                    consumer_name: name,
                    pending,
                })
                .collect::<Vec<_>>();

            // Retrieve extended pending details.
            let extended_details_raw: Vec<(String, String, i64, i64)> = redis::cmd("XPENDING")
                .arg(stream_key)
                .arg(group_name)
                .arg("-")
                .arg("+")
                .arg(EXTENDED_LIMIT)
                .query_async(&mut conn)
                .await
                .unwrap_or_default();

            // For each pending message, fetch its job payload.
            let mut pending_details = Vec::with_capacity(extended_details_raw.len());
            for (message_id, consumer, idle_time_ms, delivery_count) in extended_details_raw {
                let job_payload = get_job_payload(&mut conn, stream_key, &message_id).await;
                pending_details.push(PendingDetail {
                    message_id,
                    consumer,
                    idle_time_ms,
                    delivery_count,
                    job_payload,
                });
            }

            // Retrieve total messages in the stream.
            let total_messages: i64 = redis::cmd("XLEN")
                .arg(stream_key)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);

            QueueStatus {
                queue: format!("{:?}", q),
                stream_key: stream_key.to_owned(),
                total_messages,
                pending_total,
                min_id,
                max_id,
                consumer_stats,
                pending_details,
            }
        }
    }))
    .await;

    // Output the result.

    if output_format == "json" {
        println!("{}", serde_json::to_string_pretty(&statuses)?);
        return Ok(());
    }

    for status in statuses {
        println!("Queue: {}", status.queue);
        println!("  Stream key: {}", status.stream_key);
        println!("  Total messages: {}", status.total_messages);
        println!(
            "  Pending messages: {} (min: {}, max: {})",
            status.pending_total, status.min_id, status.max_id
        );
        println!("  Consumer stats:");
        if status.consumer_stats.is_empty() {
            println!("    (none)");
        } else {
            for consumer in &status.consumer_stats {
                println!(
                    "    {}: {} pending",
                    consumer.consumer_name, consumer.pending
                );
            }
        }
        println!("  Pending job details (up to {}):", EXTENDED_LIMIT);
        if status.pending_details.is_empty() {
            println!("    (none)");
        } else {
            for detail in &status.pending_details {
                let formatted_idle = format_duration(detail.idle_time_ms);
                println!(
                    "    Message {}: consumer '{}', idle {}, delivered {} times",
                    detail.message_id, detail.consumer, formatted_idle, detail.delivery_count
                );
                println!("      Payload:");

                // Remove outer braces and indent inner lines.
                if detail.job_payload.starts_with('{') && detail.job_payload.ends_with('}') {
                    let lines: Vec<&str> = detail.job_payload.lines().collect();
                    if lines.len() > 2 {
                        for line in &lines[1..lines.len() - 1] {
                            println!("        {}", line.trim());
                        }

                        continue;
                    }
                }

                println!("        {}", detail.job_payload);
            }
        }
        println!();
    }

    Ok(())
}
