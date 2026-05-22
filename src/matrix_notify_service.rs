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

use matrix_sdk::ruma::RoomId;
use matrix_sdk::{
    Client, config::SyncSettings, room::Room, ruma::events::room::message::RoomMessageEventContent,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct MatrixNotifyService {
    last_send: Arc<Mutex<Instant>>,
    room: Option<Room>,
}

fn get_empty_service() -> MatrixNotifyService {
    MatrixNotifyService {
        last_send: Arc::new(Mutex::new(
            Instant::now().checked_sub(Duration::from_hours(1)).unwrap(),
        )),
        room: None,
    }
}

impl MatrixNotifyService {
    #[must_use]
    pub fn empty() -> Self {
        get_empty_service()
    }

    pub async fn new() -> Self {
        // Read env vars for homeserver, user, pass, room, etc.
        let homeserver = std::env::var("MATRIX_HOMESERVER").unwrap_or_default();
        let username = std::env::var("MATRIX_USERNAME").unwrap_or_default();
        let password = std::env::var("MATRIX_PASSWORD").unwrap_or_default();
        let room = std::env::var("MATRIX_ROOM_ID").unwrap_or_default();

        if homeserver.is_empty() || username.is_empty() || password.is_empty() || room.is_empty() {
            warn!("Missing env vars; not starting.");
            return get_empty_service();
        }

        let room_id = match RoomId::parse(&room) {
            Ok(room) => room,
            Err(err) => {
                error!("Invalid room ID: '{room}'. Will not notify using Matrix. {err}");
                return get_empty_service();
            }
        };

        info!(
            "Trying to connect to Matrix homeserver: '{homeserver}' as '{username}' in room '{room_id}'"
        );

        let client = match Client::builder().homeserver_url(homeserver).build().await {
            Ok(client) => client,
            Err(err) => {
                error!("Error creating client. Will not notify using Matrix. {err}");
                return get_empty_service();
            }
        };

        if let Err(err) = client
            .matrix_auth()
            .login_username(username, &password)
            .send()
            .await
        {
            error!("Error logging in. Will not notify using Matrix. {err}");
            return get_empty_service();
        }

        if let Err(err) = client.sync_once(SyncSettings::default()).await {
            error!("Error syncing. Will not notify using Matrix. {err}");
            return get_empty_service();
        }

        let room = match client.join_room_by_id(&room_id).await {
            Ok(room) => room,
            Err(err) => {
                error!("Error getting room. Will not notify using Matrix. {err}");
                return get_empty_service();
            }
        };

        info!("Connected to Matrix.");

        Self {
            last_send: Arc::new(Mutex::new(
                Instant::now().checked_sub(Duration::from_hours(1)).unwrap(),
            )),
            room: Some(room),
        }
    }

    /// Sends a message.
    pub async fn send_message(&self, msg: &str) {
        if let Some(room) = &self.room {
            let content = RoomMessageEventContent::text_markdown(msg);
            if room.send(content).await.is_err() {
                error!("Error sending message: '{msg}'");
            } else {
                *self.last_send.lock().unwrap() = Instant::now();
            }
        }
    }
}
