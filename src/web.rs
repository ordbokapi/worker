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

use apalis::prelude::*;
use axum::{
    Json, Router,
    extract::Request,
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::info;

use crate::jobs::{FetchArticleListJob, FetchDictionaryMetadataJob, QUEUE_NAMESPACES};
use crate::state::UibDictionary;

pub use apalis_redis::{ConnectionManager, RedisStorage};

#[derive(Clone)]
pub struct WebState {
    pub db: PgPool,
    pub redis_conn: ConnectionManager,
    pub job_tracker: super::JobTracker,
    pub fetch_article_list_storage: RedisStorage<FetchArticleListJob>,
    pub fetch_dict_metadata_storage: RedisStorage<FetchDictionaryMetadataJob>,
    pub secret_hash: Option<String>,
}

pub fn routes(state: WebState) -> Router {
    let authed = Router::new()
        .route("/api/stats", get(get_stats))
        .route("/api/clear-queues", post(clear_queues))
        .route("/api/clear-outbox", post(clear_outbox))
        .route("/api/clear-all", post(clear_all))
        .route("/api/sync", post(trigger_sync))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/", get(serve_ui))
        .route("/api/auth-status", get(auth_status))
        .merge(authed)
        .with_state(state)
}

async fn require_auth(
    axum::extract::State(state): axum::extract::State<WebState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected_hash) = &state.secret_hash else {
        return next.run(req).await;
    };

    let authorized = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|secret| {
            let hash = base16ct::lower::encode_string(&Sha256::digest(secret.as_bytes()));
            hash == *expected_hash
        });

    if authorized {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "Unauthorized").into_response()
    }
}

#[derive(Serialize)]
struct AuthStatus {
    auth_required: bool,
}

async fn auth_status(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Json<AuthStatus> {
    Json(AuthStatus {
        auth_required: state.secret_hash.is_some(),
    })
}

async fn serve_ui() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(include_str!("../static/management.html")),
    )
}

#[derive(Serialize)]
struct QueueStats {
    namespace: String,
    pending: i64,
    failed: i64,
    dead: i64,
    done: i64,
    scheduled: i64,
}

#[derive(Serialize)]
struct OutboxStats {
    pending: i64,
    processed_last_hour: i64,
    total: i64,
}

#[derive(Serialize)]
struct Stats {
    queues: Vec<QueueStats>,
    outbox: OutboxStats,
    active_jobs: Vec<String>,
}

async fn get_stats(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Result<Json<Stats>, StatusCode> {
    let mut conn = state.redis_conn.clone();

    let mut queues = Vec::with_capacity(QUEUE_NAMESPACES.len());
    for &ns in QUEUE_NAMESPACES {
        let pending: i64 = redis::cmd("LLEN")
            .arg(format!("{ns}:active"))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        let failed: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:failed"))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        let dead: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:dead"))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        let done: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:done"))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        let scheduled: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:scheduled"))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);

        queues.push(QueueStats {
            namespace: ns.to_string(),
            pending,
            failed,
            dead,
            done,
            scheduled,
        });
    }

    let outbox = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT \
            COUNT(*) FILTER (WHERE processed_at IS NULL), \
            COUNT(*) FILTER (WHERE processed_at > NOW() - INTERVAL '1 hour'), \
            COUNT(*) \
         FROM job_outbox",
    )
    .fetch_one(&state.db)
    .await
    .map_or(
        OutboxStats {
            pending: 0,
            processed_last_hour: 0,
            total: 0,
        },
        |(pending, processed_last_hour, total)| OutboxStats {
            pending,
            processed_last_hour,
            total,
        },
    );

    let active_jobs = state.job_tracker.active_jobs();

    Ok(Json(Stats {
        queues,
        outbox,
        active_jobs,
    }))
}

#[derive(Serialize)]
struct ActionResult {
    ok: bool,
    message: String,
}

async fn clear_queues(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Result<Json<ActionResult>, StatusCode> {
    let mut conn = state.redis_conn.clone();

    for &ns in QUEUE_NAMESPACES {
        let _: () = redis::cmd("DEL")
            .arg(format!("{ns}:active"))
            .arg(format!("{ns}:scheduled"))
            .arg(format!("{ns}:failed"))
            .arg(format!("{ns}:dead"))
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }

    Ok(Json(ActionResult {
        ok: true,
        message: "All queues cleared.".into(),
    }))
}

async fn clear_outbox(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Result<Json<ActionResult>, StatusCode> {
    let result = sqlx::query("DELETE FROM job_outbox WHERE processed_at IS NULL")
        .execute(&state.db)
        .await;

    match result {
        Ok(r) => Ok(Json(ActionResult {
            ok: true,
            message: format!("Deleted {} pending outbox entries.", r.rows_affected()),
        })),
        Err(e) => Ok(Json(ActionResult {
            ok: false,
            message: format!("Failed to clear outbox: {e}"),
        })),
    }
}

async fn clear_all(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Result<Json<ActionResult>, StatusCode> {
    let mut conn = state.redis_conn.clone();

    for &ns in QUEUE_NAMESPACES {
        let _: () = redis::cmd("DEL")
            .arg(format!("{ns}:active"))
            .arg(format!("{ns}:scheduled"))
            .arg(format!("{ns}:failed"))
            .arg(format!("{ns}:dead"))
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }

    let outbox_result = sqlx::query("DELETE FROM job_outbox WHERE processed_at IS NULL")
        .execute(&state.db)
        .await;

    let outbox_msg = match outbox_result {
        Ok(r) => format!("Deleted {} pending outbox entries.", r.rows_affected()),
        Err(e) => format!("Outbox clear failed: {e}"),
    };

    Ok(Json(ActionResult {
        ok: true,
        message: format!("Queues cleared. {outbox_msg}"),
    }))
}

#[derive(Deserialize)]
struct SyncRequest {
    #[serde(default)]
    dictionaries: Vec<String>,
}

async fn trigger_sync(
    axum::extract::State(state): axum::extract::State<WebState>,
    Json(body): Json<SyncRequest>,
) -> Result<Json<ActionResult>, StatusCode> {
    let dicts: Vec<UibDictionary> = if body.dictionaries.is_empty() {
        UibDictionary::all().to_vec()
    } else {
        let mut parsed = Vec::new();
        for s in &body.dictionaries {
            match UibDictionary::parse(s) {
                Some(d) => parsed.push(d),
                None => {
                    return Ok(Json(ActionResult {
                        ok: false,
                        message: format!("Unknown dictionary: {s}. Valid: bm, nn, no"),
                    }));
                }
            }
        }
        parsed
    };

    let dict_names: Vec<&str> = dicts.iter().map(|d| d.as_str()).collect();
    info!("Triggering sync for: {dict_names:?}");

    for dict in &dicts {
        if let Err(e) = sqlx::query(
            "UPDATE articles SET sync_status = 'pending_fetch', status_changed_at = now()
             WHERE dictionary = $1",
        )
        .bind(dict.as_str())
        .execute(&state.db)
        .await
        {
            return Ok(Json(ActionResult {
                ok: false,
                message: format!("DB error resetting {}: {e}", dict.as_str()),
            }));
        }

        let mut fal = state.fetch_article_list_storage.clone();
        if let Err(e) = fal
            .push(FetchArticleListJob {
                dictionary: dict.as_str().to_string(),
            })
            .await
        {
            return Ok(Json(ActionResult {
                ok: false,
                message: format!("Failed to enqueue article-list for {}: {e}", dict.as_str()),
            }));
        }

        let mut fdm = state.fetch_dict_metadata_storage.clone();
        if let Err(e) = fdm
            .push(FetchDictionaryMetadataJob {
                dictionary: dict.as_str().to_string(),
            })
            .await
        {
            return Ok(Json(ActionResult {
                ok: false,
                message: format!("Failed to enqueue dict-metadata for {}: {e}", dict.as_str()),
            }));
        }
    }

    Ok(Json(ActionResult {
        ok: true,
        message: format!("Sync triggered for: {dict_names:?}"),
    }))
}
