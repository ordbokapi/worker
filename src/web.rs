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
    extract::{DefaultBodyLimit, Request},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use axum_governor::{GovernorConfigBuilder, GovernorLayer, Quota, extractor::SmartIp, nz};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use tracing::{info, warn};

use crate::jobs::{FetchArticleListJob, FetchDictionaryMetadataJob, QUEUE_NAMESPACES};
use crate::state::UibDictionary;

pub use apalis_redis::{ConnectionManager, RedisStorage};

type HmacSha256 = Hmac<Sha256>;

const SESSION_MAX_AGE: u64 = 60 * 60;

const PRIVATE_CIDRS: &[&str] = &[
    "10.0.0.0/8",
    "172.16.0.0/12",
    "192.168.0.0/16",
    "127.0.0.0/8",
    "::1/128",
    "fc00::/7",
    "fe80::/10",
];

#[derive(Clone)]
pub struct WebState {
    pub db: PgPool,
    pub redis_conn: ConnectionManager,
    pub job_tracker: super::JobTracker,
    pub fetch_article_list_storage: RedisStorage<FetchArticleListJob>,
    pub fetch_dict_metadata_storage: RedisStorage<FetchDictionaryMetadataJob>,
    pub secret_hash: Option<String>,
    pub session_key: [u8; 32],
}

pub async fn routes(state: WebState) -> Router {
    let mut trusted_nets: Vec<ipnet::IpNet> =
        PRIVATE_CIDRS.iter().map(|s| s.parse().unwrap()).collect();

    if let Ok(cidrs) = std::env::var("TRUSTED_PROXIES") {
        trusted_nets.extend(
            cidrs
                .split(',')
                .filter_map(|s| s.trim().parse::<ipnet::IpNet>().ok()),
        );
    }

    trusted_nets.extend(fetch_proxy_list_urls().await);
    info!("Trusted proxy CIDRs: {}", trusted_nets.len());

    let smart_ip = SmartIp::new().with_trusted_proxies(trusted_nets);

    let rate_limit = GovernorConfigBuilder::default()
        .with_extractor(smart_ip)
        .expect_connect_info()
        .quota_default(Quota::requests_per_minute(nz!(10u32)))
        .finish()
        .unwrap();

    let login_route = Router::new()
        .route("/api/login", post(login))
        .layer(GovernorLayer::new(rate_limit))
        .with_state(state.clone());

    let authed = Router::new()
        .route("/api/stats", get(get_stats))
        .route("/api/clear-queues", post(clear_queues))
        .route("/api/clear-outbox", post(clear_outbox))
        .route("/api/clear-all", post(clear_all))
        .route("/api/sync", post(trigger_sync))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/", get(serve_ui))
        .route("/login", get(serve_login))
        .route("/api/auth-status", get(auth_status))
        .route("/api/logout", post(logout))
        .merge(login_route)
        .merge(authed)
        .layer(DefaultBodyLimit::max(4096))
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

async fn fetch_proxy_list_urls() -> Vec<ipnet::IpNet> {
    let urls = match std::env::var("TRUSTED_PROXY_LIST_URLS") {
        Ok(v) if !v.is_empty() => v,
        _ => return vec![],
    };

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("Failed to build HTTP client for proxy list fetch: {e}");
            return vec![];
        }
    };

    let mut nets = Vec::new();

    for url in urls.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        match fetch_single_proxy_list(&client, url).await {
            Ok(parsed) => {
                info!("Fetched {} CIDRs from {url}", parsed.len());
                nets.extend(parsed);
            }
            Err(e) => {
                warn!("Failed to fetch proxy list from {url}: {e}");
            }
        }
    }

    nets
}

async fn fetch_single_proxy_list(
    client: &reqwest::Client,
    url: &str,
) -> Result<Vec<ipnet::IpNet>, String> {
    let resp = client.get(url).send().await.map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    let content_length = resp.content_length().unwrap_or(0);

    if content_length > 1_048_576 {
        return Err(format!("Response too large: {content_length} bytes"));
    }

    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();

    let bytes = resp.bytes().await.map_err(|e| e.to_string())?;

    if bytes.len() > 1_048_576 {
        return Err(format!("Response too large: {} bytes", bytes.len()));
    }

    let body = String::from_utf8_lossy(&bytes);

    if content_type.contains("application/json") {
        let ips: Vec<String> = serde_json::from_str(&body).map_err(|e| e.to_string())?;

        Ok(ips
            .iter()
            .filter_map(|s| parse_ip_or_cidr(s.trim()))
            .collect())
    } else {
        Ok(body
            .lines()
            .filter_map(|line| parse_ip_or_cidr(line.trim()))
            .collect())
    }
}

fn parse_ip_or_cidr(s: &str) -> Option<ipnet::IpNet> {
    if s.is_empty() {
        return None;
    }

    s.parse::<ipnet::IpNet>().ok().or_else(|| {
        s.parse::<std::net::IpAddr>().ok().map(|ip| match ip {
            std::net::IpAddr::V4(v4) => ipnet::Ipv4Net::new(v4, 32).unwrap().into(),
            std::net::IpAddr::V6(v6) => ipnet::Ipv6Net::new(v6, 128).unwrap().into(),
        })
    })
}

async fn security_headers(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    let headers = res.headers_mut();

    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    res
}

fn create_session_token(key: &[u8]) -> String {
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + SESSION_MAX_AGE;
    let exp_str = exp.to_string();
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");

    mac.update(exp_str.as_bytes());

    let sig = base16ct::lower::encode_string(&mac.finalize().into_bytes());

    format!("{exp_str}.{sig}")
}

fn verify_session_token(token: &str, key: &[u8]) -> bool {
    let Some((exp_str, sig_hex)) = token.split_once('.') else {
        return false;
    };

    let Ok(exp) = exp_str.parse::<u64>() else {
        return false;
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    if now > exp {
        return false;
    }

    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");

    mac.update(exp_str.as_bytes());

    let Ok(sig_bytes) = base16ct::lower::decode_vec(sig_hex) else {
        return false;
    };

    mac.verify_slice(&sig_bytes).is_ok()
}

fn extract_cookie<'a>(headers: &'a axum::http::HeaderMap, name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(|cookies| {
            cookies.split(';').find_map(|c| {
                let c = c.trim();
                c.strip_prefix(name)?.strip_prefix('=')
            })
        })
}

async fn require_auth(
    axum::extract::State(state): axum::extract::State<WebState>,
    req: Request,
    next: Next,
) -> Response {
    if state.secret_hash.is_none() {
        return next.run(req).await;
    }

    let authorized = extract_cookie(req.headers(), "mgmt_session")
        .is_some_and(|token| verify_session_token(token, &state.session_key));

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

#[derive(Deserialize)]
struct LoginRequest {
    secret: String,
}

async fn login(
    axum::extract::State(state): axum::extract::State<WebState>,
    axum::extract::Form(body): axum::extract::Form<LoginRequest>,
) -> Response {
    let Some(expected_hash) = &state.secret_hash else {
        warn!("Login attempt but MGMT_UI_SECRET_HASH is not set");
        return Redirect::to("/login").into_response();
    };

    let hash = base16ct::lower::encode_string(&Sha256::digest(body.secret.as_bytes()));

    if hash != *expected_hash {
        return Redirect::to("/login?error=1").into_response();
    }

    let token = create_session_token(&state.session_key);
    let secure = std::env::var("SECURE_COOKIES").map_or(true, |v| v != "0" && v != "false");
    let secure_flag = if secure { "; Secure" } else { "" };
    let cookie = format!(
        "mgmt_session={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={SESSION_MAX_AGE}{secure_flag}"
    );

    ([(header::SET_COOKIE, cookie)], Redirect::to("/")).into_response()
}

async fn serve_ui(
    axum::extract::State(state): axum::extract::State<WebState>,
    req: Request,
) -> Response {
    if state.secret_hash.is_some() {
        let authorized = extract_cookie(req.headers(), "mgmt_session")
            .is_some_and(|token| verify_session_token(token, &state.session_key));

        if !authorized {
            return Redirect::to("/login").into_response();
        }
    }

    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(include_str!("../static/management.html")),
    )
        .into_response()
}

async fn serve_login(
    axum::extract::State(state): axum::extract::State<WebState>,
    req: Request,
) -> Response {
    if state.secret_hash.is_some() {
        let authorized = extract_cookie(req.headers(), "mgmt_session")
            .is_some_and(|token| verify_session_token(token, &state.session_key));

        if authorized {
            return Redirect::to("/").into_response();
        }
    }

    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(include_str!("../static/login.html")),
    )
        .into_response()
}

async fn logout(
    axum::extract::State(state): axum::extract::State<WebState>,
    req: Request,
) -> Response {
    if state.secret_hash.is_some() {
        let authorized = extract_cookie(req.headers(), "mgmt_session")
            .is_some_and(|token| verify_session_token(token, &state.session_key));

        if !authorized {
            return Redirect::to("/login").into_response();
        }
    }

    let cookie = "mgmt_session=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0";
    ([(header::SET_COOKIE, cookie)], Redirect::to("/login")).into_response()
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
