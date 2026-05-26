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
    extract::{DefaultBodyLimit, Request, WebSocketUpgrade, ws},
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
    pub sweep_storage: RedisStorage<super::jobs::SweepJob>,
    pub fetch_article_list_storage: RedisStorage<FetchArticleListJob>,
    pub fetch_dict_metadata_storage: RedisStorage<FetchDictionaryMetadataJob>,
    pub secret_hash: Option<String>,
    pub session_key: [u8; 32],
    #[cfg(feature = "dev_ui")]
    pub reload_tx: tokio::sync::broadcast::Sender<()>,
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
        .route("/api/stats/ws", get(stats_ws))
        .route("/api/clear-queues", post(clear_queues))
        .route("/api/clear-outbox", post(clear_outbox))
        .route("/api/clear-all", post(clear_all))
        .route("/api/sync", post(trigger_sync))
        .route("/api/sweep", post(trigger_sweep))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));

    #[allow(unused_mut)]
    let mut app = Router::new()
        .route("/", get(serve_ui))
        .route("/login", get(serve_login))
        .route("/api/auth-status", get(auth_status))
        .route("/api/logout", post(logout))
        .merge(login_route)
        .merge(authed);

    #[cfg(feature = "dev_ui")]
    {
        app = app.route("/api/dev-reload/ws", get(dev_reload_ws));
    }

    #[allow(unused_mut)]
    let mut app = app
        .layer(DefaultBodyLimit::max(4096))
        .layer(middleware::from_fn(security_headers))
        .with_state(state);

    #[cfg(feature = "dev_ui")]
    {
        app = app.route("/static/{*path}", get(serve_static_file));
    }

    app
}

#[cfg(feature = "dev_ui")]
async fn serve_static_file(axum::extract::Path(path): axum::extract::Path<String>) -> Response {
    let file_path = std::path::Path::new("static").join(&path);
    std::fs::read_to_string(&file_path).map_or_else(
        |_| StatusCode::NOT_FOUND.into_response(),
        |content| {
            let ext = file_path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let content_type = match ext {
                "js" => "application/javascript",
                _ => "text/plain",
            };
            ([(header::CONTENT_TYPE, content_type)], content).into_response()
        },
    )
}

#[cfg(feature = "dev_ui")]
async fn dev_reload_ws(
    wsu: WebSocketUpgrade,
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Response {
    wsu.on_upgrade(|mut socket| async move {
        let mut rx = state.reload_tx.subscribe();
        while rx.recv().await.is_ok() {
            if socket
                .send(ws::Message::Text("reload".into()))
                .await
                .is_err()
            {
                break;
            }
        }
    })
}

#[cfg(feature = "dev_ui")]
pub fn spawn_static_watcher(tx: tokio::sync::broadcast::Sender<()>) {
    use notify::{RecursiveMode, Watcher, event::ModifyKind};

    std::thread::spawn(move || {
        let (ntx, nrx) = std::sync::mpsc::channel();
        let mut watcher = notify::recommended_watcher(ntx).expect("failed to create file watcher");
        watcher
            .watch(std::path::Path::new("static"), RecursiveMode::Recursive)
            .expect("failed to watch static/");

        for event in nrx.into_iter().flatten() {
            let dominated_by_modify = matches!(
                event.kind,
                notify::EventKind::Modify(ModifyKind::Data(_))
                    | notify::EventKind::Create(_)
                    | notify::EventKind::Remove(_)
            );
            if dominated_by_modify {
                let _ = tx.send(());
            }
        }
    });
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

    #[cfg(feature = "dev_ui")]
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'none'; script-src 'self' 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    #[cfg(not(feature = "dev_ui"))]
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

#[cfg(feature = "dev_ui")]
fn serve_static(filename: &str) -> String {
    let path = std::path::Path::new("static").join(filename);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => return format!("<pre>Failed to read {}: {e}</pre>", path.display()),
    };
    content.replace(
        "</body>",
        "<script type=\"module\" src=\"/static/dev-reload.js\"></script></body>",
    )
}

#[cfg(not(feature = "dev_ui"))]
fn serve_static(filename: &str) -> &'static str {
    match filename {
        "management.html" => include_str!("../static/management.html"),
        "login.html" => include_str!("../static/login.html"),
        _ => "<pre>Unknown file</pre>",
    }
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

    let dictionaries: Vec<&str> = UibDictionary::all().iter().map(|d| d.as_str()).collect();
    let queues: Vec<&str> = QUEUE_NAMESPACES
        .iter()
        .map(|ns| ns.strip_prefix("apalis:").unwrap_or(ns))
        .collect();
    let config_json = serde_json::json!({
        "dictionaries": dictionaries,
        "queueNamespaces": queues,
    });
    let config_script = format!("<script>window.__CONFIG__={config_json};</script>");
    let html =
        serve_static("management.html").replace("</head>", &format!("{config_script}</head>"));

    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(html),
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
        Html(serve_static("login.html")),
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
    running: i64,
    failed: i64,
    dead: i64,
    done: i64,
    scheduled: i64,
}

#[derive(Serialize)]
struct OutboxJobTypeStats {
    job_type: String,
    pending: i64,
    processed_last_hour: i64,
    processed_last_day: i64,
}

#[derive(Serialize)]
struct OutboxStats {
    by_type: Vec<OutboxJobTypeStats>,
    pending: i64,
    processed_last_hour: i64,
    processed_last_day: i64,
}

#[derive(Serialize)]
struct DictionarySyncStats {
    dictionary: String,
    idle: i64,
    pending_fetch: i64,
    pending_index: i64,
}

#[derive(Serialize)]
#[serde(tag = "type", content = "data")]
enum StatsMessage {
    #[serde(rename = "queues")]
    Queues(Vec<QueueStats>),
    #[serde(rename = "active_jobs")]
    ActiveJobs(Vec<String>),
    #[serde(rename = "outbox")]
    Outbox(OutboxStats),
    #[serde(rename = "articles")]
    Articles(Vec<DictionarySyncStats>),
}

async fn fetch_queue_stats(conn: &mut ConnectionManager) -> Vec<QueueStats> {
    let mut queues = Vec::with_capacity(QUEUE_NAMESPACES.len());
    for &ns in QUEUE_NAMESPACES {
        let pending: i64 = redis::cmd("LLEN")
            .arg(format!("{ns}:active"))
            .query_async(conn)
            .await
            .unwrap_or(0);
        let workers: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(format!("{ns}:workers"))
            .arg("0")
            .arg("+inf")
            .query_async(conn)
            .await
            .unwrap_or_default();
        let mut running: i64 = 0;
        for worker_set in &workers {
            let count: i64 = redis::cmd("SCARD")
                .arg(worker_set)
                .query_async(conn)
                .await
                .unwrap_or(0);
            running += count;
        }
        let failed: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:failed"))
            .query_async(conn)
            .await
            .unwrap_or(0);
        let dead: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:dead"))
            .query_async(conn)
            .await
            .unwrap_or(0);
        let done: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:done"))
            .query_async(conn)
            .await
            .unwrap_or(0);
        let scheduled: i64 = redis::cmd("ZCARD")
            .arg(format!("{ns}:scheduled"))
            .query_async(conn)
            .await
            .unwrap_or(0);

        queues.push(QueueStats {
            namespace: ns.strip_prefix("apalis:").unwrap_or(ns).to_string(),
            pending,
            running,
            failed,
            dead,
            done,
            scheduled,
        });
    }
    queues
}

async fn fetch_outbox_stats(db: &PgPool) -> OutboxStats {
    let rows: Vec<(String, i64, i64, i64)> = sqlx::query_as(
        "SELECT job_type, \
            COUNT(*) FILTER (WHERE processed_at IS NULL), \
            COUNT(*) FILTER (WHERE processed_at > NOW() - INTERVAL '1 hour'), \
            COUNT(*) FILTER (WHERE processed_at > NOW() - INTERVAL '1 day') \
         FROM job_outbox GROUP BY job_type ORDER BY job_type",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let pending: i64 = rows.iter().map(|(_, p, _, _)| p).sum();
    let processed_last_hour: i64 = rows.iter().map(|(_, _, p, _)| p).sum();
    let processed_last_day: i64 = rows.iter().map(|(_, _, _, p)| p).sum();

    let by_type = rows
        .into_iter()
        .map(|(job_type, p, plh, pld)| OutboxJobTypeStats {
            job_type,
            pending: p,
            processed_last_hour: plh,
            processed_last_day: pld,
        })
        .collect();

    OutboxStats {
        by_type,
        pending,
        processed_last_hour,
        processed_last_day,
    }
}

async fn fetch_article_stats(db: &PgPool) -> Vec<DictionarySyncStats> {
    sqlx::query_as::<_, (String, i64, i64, i64)>(
        "SELECT dictionary, \
            COUNT(*) FILTER (WHERE sync_status = 'idle'), \
            COUNT(*) FILTER (WHERE sync_status = 'pending_fetch'), \
            COUNT(*) FILTER (WHERE sync_status = 'pending_index') \
         FROM articles GROUP BY dictionary ORDER BY dictionary",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default()
    .into_iter()
    .map(
        |(dictionary, idle, pending_fetch, pending_index)| DictionarySyncStats {
            dictionary,
            idle,
            pending_fetch,
            pending_index,
        },
    )
    .collect()
}

async fn stats_ws(
    axum::extract::State(state): axum::extract::State<WebState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade.on_upgrade(move |socket| stats_ws_handler(socket, state))
}

async fn stats_ws_handler(mut socket: ws::WebSocket, state: WebState) {
    use tokio::time::{Duration, interval};

    macro_rules! send {
        ($socket:expr, $msg:expr) => {
            if let Ok(json) = serde_json::to_string(&$msg) {
                if $socket.send(ws::Message::text(json)).await.is_err() {
                    return;
                }
            }
        };
    }

    let mut fast_tick = interval(Duration::from_millis(500));
    let mut slow_tick = interval(Duration::from_secs(3));

    let mut conn = state.redis_conn.clone();
    send!(
        socket,
        StatsMessage::Queues(fetch_queue_stats(&mut conn).await)
    );
    send!(
        socket,
        StatsMessage::ActiveJobs(state.job_tracker.active_jobs())
    );
    send!(
        socket,
        StatsMessage::Outbox(fetch_outbox_stats(&state.db).await)
    );
    send!(
        socket,
        StatsMessage::Articles(fetch_article_stats(&state.db).await)
    );

    fast_tick.reset();
    slow_tick.reset();

    loop {
        tokio::select! {
            _ = fast_tick.tick() => {
                send!(socket, StatsMessage::Queues(fetch_queue_stats(&mut conn).await));
                send!(socket, StatsMessage::ActiveJobs(state.job_tracker.active_jobs()));
            }
            _ = slow_tick.tick() => {
                send!(socket, StatsMessage::Outbox(fetch_outbox_stats(&state.db).await));
                send!(socket, StatsMessage::Articles(fetch_article_stats(&state.db).await));
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(ws::Message::Close(_))) | None => return,
                    _ => {}
                }
            }
        }
    }
}

#[derive(Serialize)]
struct ActionResult {
    ok: bool,
    message: String,
}

async fn reset_orphaned_articles(db: &sqlx::PgPool) -> String {
    match sqlx::query(
        "UPDATE articles SET sync_status = 'idle', status_changed_at = now() \
         WHERE sync_status != 'idle'",
    )
    .execute(db)
    .await
    {
        Ok(r) if r.rows_affected() > 0 => {
            format!("Nullstilte {} foreldrelause artiklar.", r.rows_affected())
        }
        Ok(_) => String::new(),
        Err(e) => format!("Nullstilling feila: {e}"),
    }
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

    let reset = reset_orphaned_articles(&state.db).await;

    Ok(Json(ActionResult {
        ok: true,
        message: format!("Alle køar tømde. {reset}"),
    }))
}

async fn clear_outbox(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Result<Json<ActionResult>, StatusCode> {
    let result = sqlx::query("DELETE FROM job_outbox WHERE processed_at IS NULL")
        .execute(&state.db)
        .await;

    let reset = reset_orphaned_articles(&state.db).await;

    match result {
        Ok(r) => Ok(Json(ActionResult {
            ok: true,
            message: format!(
                "Sletta {} ventande utboksoppføringar. {reset}",
                r.rows_affected()
            ),
        })),
        Err(e) => Ok(Json(ActionResult {
            ok: false,
            message: format!("Kunne ikkje tømme utboks: {e}"),
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
        Ok(r) => format!("Sletta {} ventande utboksoppføringar.", r.rows_affected()),
        Err(e) => format!("Utbokstømming feila: {e}"),
    };

    let reset = reset_orphaned_articles(&state.db).await;

    Ok(Json(ActionResult {
        ok: true,
        message: format!("Køar tømde. {outbox_msg} {reset}"),
    }))
}

#[derive(Deserialize)]
struct SyncRequest {
    #[serde(default)]
    dictionaries: Vec<String>,
    #[serde(default)]
    force: bool,
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
                        message: format!("Ukjend ordbok: {s}. Gyldige: bm, nn, no"),
                    }));
                }
            }
        }
        parsed
    };

    let dict_names: Vec<&str> = dicts.iter().map(|d| d.as_str()).collect();
    info!("Triggering sync for: {dict_names:?}");

    for dict in &dicts {
        let mut fal = state.fetch_article_list_storage.clone();
        if let Err(e) = fal
            .push(FetchArticleListJob {
                dictionary: dict.as_str().to_string(),
                force: body.force,
            })
            .await
        {
            return Ok(Json(ActionResult {
                ok: false,
                message: format!(
                    "Kunne ikkje leggje til artikkelliste for {}: {e}",
                    dict.as_str()
                ),
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
                message: format!(
                    "Kunne ikkje leggje til ordbokmetadata for {}: {e}",
                    dict.as_str()
                ),
            }));
        }
    }

    Ok(Json(ActionResult {
        ok: true,
        message: format!("Synkronisering starta for: {dict_names:?}"),
    }))
}

async fn trigger_sweep(
    axum::extract::State(state): axum::extract::State<WebState>,
) -> Result<Json<ActionResult>, StatusCode> {
    let mut storage = state.sweep_storage.clone();
    match storage.push(super::jobs::SweepJob).await {
        Ok(()) => Ok(Json(ActionResult {
            ok: true,
            message: "Opprydding lagd i kø.".to_string(),
        })),
        Err(e) => Ok(Json(ActionResult {
            ok: false,
            message: format!("Kunne ikkje starte opprydding: {e}"),
        })),
    }
}
