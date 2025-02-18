use log::{error, info, warn};
use matrix_sdk::ruma::RoomId;
use matrix_sdk::{
    config::SyncSettings, room::Room, ruma::events::room::message::RoomMessageEventContent, Client,
};
use std::time::{Duration, Instant};

use crate::queue::{JobPayload, JobQueue, JobQueueService};

#[derive(Clone)]
pub struct MatrixNotifyService {
    last_send: Instant,
    room: Option<Room>,
    job_queue: JobQueueService,
}

fn get_empty_service(queue_service: JobQueueService) -> MatrixNotifyService {
    MatrixNotifyService {
        last_send: Instant::now() - Duration::from_secs(3600),
        room: None,
        job_queue: queue_service,
    }
}

impl MatrixNotifyService {
    pub async fn new(queue_service: JobQueueService) -> Self {
        // Read env vars for homeserver, user, pass, room, etc.
        let homeserver = std::env::var("MATRIX_HOMESERVER").unwrap_or_default();
        let username = std::env::var("MATRIX_USERNAME").unwrap_or_default();
        let password = std::env::var("MATRIX_PASSWORD").unwrap_or_default();
        let room = std::env::var("MATRIX_ROOM_ID").unwrap_or_default();

        if homeserver.is_empty() || username.is_empty() || password.is_empty() || room.is_empty() {
            warn!("Missing env vars; not starting.");
            return get_empty_service(queue_service);
        }

        let room_id = match RoomId::parse(&room) {
            Ok(room) => room,
            Err(err) => {
                error!("Invalid room ID: '{room}'. Will not notify using Matrix. {err}");
                return get_empty_service(queue_service);
            }
        };

        info!("Trying to connect to Matrix homeserver: '{homeserver}' as '{username}' in room '{room_id}'");

        let client = match Client::builder().homeserver_url(homeserver).build().await {
            Ok(client) => client,
            Err(err) => {
                error!("Error creating client. Will not notify using Matrix. {err}");
                return get_empty_service(queue_service);
            }
        };

        if let Err(err) = client
            .matrix_auth()
            .login_username(username, &password)
            .send()
            .await
        {
            error!("Error logging in. Will not notify using Matrix. {err}");
            return get_empty_service(queue_service);
        }

        if let Err(err) = client.sync_once(SyncSettings::default()).await {
            error!("Error syncing. Will not notify using Matrix. {err}");
            return get_empty_service(queue_service);
        }

        let room = match client.join_room_by_id(&room_id).await {
            Ok(room) => room,
            Err(err) => {
                error!("Error getting room. Will not notify using Matrix. {err}");
                return get_empty_service(queue_service);
            }
        };

        info!("Connected to Matrix.");

        Self {
            last_send: Instant::now() - Duration::from_secs(3600),
            room: Some(room),
            job_queue: queue_service,
        }
    }

    /// Sends a message.
    pub async fn send_message(&mut self, msg: &str) {
        if let Some(room) = &self.room {
            let content = RoomMessageEventContent::text_markdown(msg);
            if room.send(content).await.is_err() {
                error!("Error sending message: '{msg}'");
                let _ = self
                    .job_queue
                    .log_error(
                        &format!("Error sending Matrix message: {}", msg),
                        Some(JobQueue::MatrixNotify.to_stream_key()),
                        Some(&serde_json::json!({ "msg": msg })),
                    )
                    .await;
            } else {
                self.last_send = Instant::now();
            }
        }
    }

    /// Queues a message to be sent.
    pub async fn queue_message(&mut self, msg: &str) {
        let payload = serde_json::json!({ "msg": msg });
        if self
            .job_queue
            .enqueue(
                JobQueue::MatrixNotify,
                &JobPayload {
                    data: payload.clone(),
                },
            )
            .await
            .is_err()
        {
            error!("Error queueing message: '{msg}'");
            let _ = self
                .job_queue
                .log_error(
                    &format!("Error queueing Matrix message: {}", msg),
                    Some(JobQueue::MatrixNotify.to_stream_key()),
                    Some(&payload),
                )
                .await;
        }
    }
}
