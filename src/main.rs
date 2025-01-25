// src/main.rs

mod article_sync_service;
#[cfg(feature = "matrix_notifs")]
mod matrix_notify_service;
mod migration;
mod queue;

use anyhow::Result;
use clap::value_parser;
use log::{error, info};
use queue::get_dict_id;
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::article_sync_service::{ArticleSyncService, UibDictionary};
#[cfg(feature = "matrix_notifs")]
use crate::matrix_notify_service::MatrixNotifyService;
use crate::migration::MigrationService;
use crate::queue::{JobPayload, JobQueue, JobQueueService};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Used to notify Matrix when events occur.
    #[cfg(feature = "matrix_notifs")]
    let mut matrix_service_opt = None;

    // Inner run function which is wrapped to be able to report errors to Matrix.
    async fn run(
        #[cfg(feature = "matrix_notifs")] matrix_service_opt: &mut Option<MatrixNotifyService>,
    ) -> Result<()> {
        #[cfg(feature = "dotenv")]
        dotenv::dotenv().ok();

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
                clap::Command::new("flush-queues")
                    .about("Flush all queues")
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
            .get_matches();

        let log_level = matches
            .get_one::<String>("log-level")
            .map(String::as_str)
            .unwrap_or("info");

        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
            .init();

        let flush_queues = if let Some("flush-queues") = matches.subcommand_name() {
            let subcmd = matches.subcommand_matches("flush-queues").unwrap();
            if !subcmd.get_flag("yes") {
                eprintln!("Flushing all queues is a destructive operation and cannot be undone. Are you sure you want to continue? (y/N)");
                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                if input.trim().to_lowercase() == "y" {
                    true
                } else {
                    eprintln!("Received input other than 'y', exiting.");
                    return Ok(());
                }
            } else {
                eprintln!("--yes/-y passed to flush-queues, skipping confirmation prompt.");
                true
            }
        } else {
            false
        };

        if flush_queues {
            info!("Flushing all queues…");
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
        job_queue.setup_consumer_groups(flush_queues).await?;

        if flush_queues {
            info!("All queues have been flushed.");
            return Ok(());
        }

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
