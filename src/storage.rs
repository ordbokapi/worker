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

use anyhow::Result;
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::{HashMap, HashSet};

use crate::extraction::{ArticleAnalysis, ConceptMap, InlineRef};
use crate::state::{SyncStatus, UibDictionary};

/// Maps articles to location data.
pub type ArticlePlaceMap = HashMap<String, HashMap<i64, (Vec<i64>, Vec<i64>)>>;

/// Metadata stored in the DB for an article.
#[derive(Debug)]
pub struct StoredArticleMetadata {
    pub primary_lemma: String,
    pub revision: Option<i64>,
    pub updated_at: String,
    pub sync_status: SyncStatus,
}

/// Ensure an article row exists with `pending_fetch` status.
pub async fn ensure_article_pending_fetch(
    db: &PgPool,
    dict: &str,
    article_id: i64,
) -> Result<bool> {
    let result = sqlx::query(
        "INSERT INTO articles (dictionary, id, data, sync_status, status_changed_at)
         VALUES ($1, $2, '{}'::jsonb, 'pending_fetch', now())
         ON CONFLICT (dictionary, id) DO UPDATE
         SET sync_status = 'pending_fetch', status_changed_at = now()
         WHERE articles.sync_status = 'idle'",
    )
    .bind(dict)
    .bind(article_id)
    .execute(db)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Reset an article to idle.
pub async fn reset_article_to_idle(
    db: &PgPool,
    dict: UibDictionary,
    article_id: i64,
) -> Result<()> {
    sqlx::query(
        "UPDATE articles SET sync_status = 'idle', status_changed_at = now()
         WHERE dictionary = $1 AND id = $2 AND sync_status != 'idle'",
    )
    .bind(dict.as_str())
    .bind(article_id)
    .execute(db)
    .await?;
    Ok(())
}

/// Ensure a bibliography or place row exists with `pending_fetch` status.
async fn ensure_entity_pending_fetch(db: &PgPool, table: &str, entity_id: i64) -> Result<bool> {
    let query = format!(
        "INSERT INTO {table} (id, sync_status, status_changed_at)
         VALUES ($1, 'pending_fetch', now())
         ON CONFLICT (id) DO UPDATE
         SET sync_status = 'pending_fetch', status_changed_at = now()
         WHERE {table}.sync_status = 'idle'
            OR ({table}.sync_status = 'not_found'
                AND {table}.status_changed_at < now() - interval '24 hours')"
    );
    let result = sqlx::query(&query).bind(entity_id).execute(db).await?;
    Ok(result.rows_affected() > 0)
}

/// Ensure a bibliography row exists with `pending_fetch` status.
pub async fn ensure_bibl_pending_fetch(db: &PgPool, bibl_id: i64) -> Result<bool> {
    ensure_entity_pending_fetch(db, "bibliography", bibl_id).await
}

/// Ensure a place row exists with `pending_fetch` status.
pub async fn ensure_place_pending_fetch(db: &PgPool, place_id: i64) -> Result<bool> {
    ensure_entity_pending_fetch(db, "places", place_id).await
}

/// Mark an entity (bibliography or place) as not found.
pub async fn mark_entity_not_found(db: &PgPool, table: &str, id: i64) -> Result<()> {
    let query = format!(
        "UPDATE {table} SET sync_status = 'not_found', status_changed_at = now() WHERE id = $1"
    );
    sqlx::query(&query).bind(id).execute(db).await?;
    Ok(())
}

/// Store a fetched article and its relationships.
#[allow(clippy::too_many_lines)]
pub async fn store_article(
    db: &PgPool,
    dict: UibDictionary,
    article_id: i64,
    article_data: &Value,
    analysis: &ArticleAnalysis,
    revision: i64,
    updated_at: &str,
) -> Result<StoreArticleResult> {
    let dict_str = dict.as_str();
    let bibl_ids: Vec<i64> = analysis.bibl_ids.iter().copied().collect();
    let dialect_ids_vec: Vec<i64> = analysis.dialect_place_ids.iter().copied().collect();
    let attestation_ids_vec: Vec<i64> = analysis.attestation_place_ids.iter().copied().collect();

    let mut tx = db.begin().await?;

    sqlx::query(
        "INSERT INTO articles (dictionary, id, data, primary_lemma, revision, updated_at, modified_at, sync_status, status_changed_at)
         VALUES ($1, $2, $3, $4, $5, $6, now(), 'pending_index', now())
         ON CONFLICT (dictionary, id) DO UPDATE
         SET data = $3, primary_lemma = $4, revision = $5, updated_at = $6, modified_at = now(),
             sync_status = 'pending_index', status_changed_at = now()",
    )
    .bind(dict_str)
    .bind(article_id)
    .bind(article_data)
    .bind(&analysis.primary_lemma)
    .bind(revision)
    .bind(updated_at)
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM article_bibliography WHERE dictionary = $1 AND article_id = $2")
        .bind(dict_str)
        .bind(article_id)
        .execute(&mut *tx)
        .await?;

    if !bibl_ids.is_empty() {
        sqlx::query(
            "INSERT INTO article_bibliography (dictionary, article_id, bibl_id)
             SELECT $1, $2, unnest($3::bigint[])
             ON CONFLICT DO NOTHING",
        )
        .bind(dict_str)
        .bind(article_id)
        .bind(&bibl_ids)
        .execute(&mut *tx)
        .await?;
    }

    let (inline_resolved_bibl_ids, unresolved_codes) =
        store_inline_refs(&mut tx, dict_str, article_id, &analysis.inline_refs).await?;

    if !inline_resolved_bibl_ids.is_empty() {
        sqlx::query(
            "INSERT INTO article_bibliography (dictionary, article_id, bibl_id)
             SELECT $1, $2, unnest($3::bigint[])
             ON CONFLICT DO NOTHING",
        )
        .bind(dict_str)
        .bind(article_id)
        .bind(&inline_resolved_bibl_ids)
        .execute(&mut *tx)
        .await?;
    }

    replace_article_place_links(
        &mut tx,
        dict_str,
        article_id,
        &dialect_ids_vec,
        &attestation_ids_vec,
    )
    .await?;

    write_outbox_index_article(&mut tx, dict_str, article_id).await?;

    let all_bibl_ids: Vec<i64> = analysis
        .bibl_ids
        .iter()
        .chain(inline_resolved_bibl_ids.iter())
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let missing_bibl = filter_missing_ids(&mut tx, "bibliography", &all_bibl_ids).await?;
    for bibl_id in &missing_bibl {
        write_outbox_fetch_bibl(&mut tx, *bibl_id).await?;
    }

    let all_place_ids: Vec<i64> = if dict == UibDictionary::NorskOrdbok {
        analysis
            .dialect_place_ids
            .iter()
            .chain(analysis.attestation_place_ids.iter())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    } else {
        Vec::new()
    };

    let missing_places = filter_missing_ids(&mut tx, "places", &all_place_ids).await?;
    for place_id in &missing_places {
        write_outbox_fetch_place(&mut tx, *place_id).await?;
    }

    let missing_articles =
        filter_missing_article_ids(&mut tx, dict_str, &analysis.related_article_ids).await?;
    for related_id in &missing_articles {
        write_outbox_fetch_article(&mut tx, dict_str, *related_id).await?;
    }

    for code in &unresolved_codes {
        write_outbox_resolve_code(&mut tx, code).await?;
    }

    tx.commit().await?;

    Ok(StoreArticleResult {
        bibl_fetched: missing_bibl.len(),
        places_fetched: missing_places.len(),
        related_fetched: missing_articles.len(),
    })
}

/// Replace article_place links for an article with new ones from extraction.
async fn replace_article_place_links(
    tx: &mut Transaction<'_, Postgres>,
    dict_str: &str,
    article_id: i64,
    dialect_ids: &[i64],
    attestation_ids: &[i64],
) -> Result<()> {
    sqlx::query("DELETE FROM article_place WHERE dictionary = $1 AND article_id = $2")
        .bind(dict_str)
        .bind(article_id)
        .execute(&mut **tx)
        .await?;

    if !dialect_ids.is_empty() {
        sqlx::query(
            "INSERT INTO article_place (dictionary, article_id, place_id, context)
             SELECT $1, $2, unnest($3::bigint[]), 'dialect'
             ON CONFLICT DO NOTHING",
        )
        .bind(dict_str)
        .bind(article_id)
        .bind(dialect_ids)
        .execute(&mut **tx)
        .await?;
    }

    if !attestation_ids.is_empty() {
        sqlx::query(
            "INSERT INTO article_place (dictionary, article_id, place_id, context)
             SELECT $1, $2, unnest($3::bigint[]), 'attestation'
             ON CONFLICT DO NOTHING",
        )
        .bind(dict_str)
        .bind(article_id)
        .bind(attestation_ids)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

/// Result from storing an article.
#[allow(clippy::struct_field_names)]
pub struct StoreArticleResult {
    pub bibl_fetched: usize,
    pub places_fetched: usize,
    pub related_fetched: usize,
}

/// Store a fetched bibliography entry.
pub async fn store_bibliography(db: &PgPool, bibl_id: i64, entry: &Value) -> Result<()> {
    let code = entry.get("code").and_then(|v| v.as_str()).unwrap_or("");
    let author = entry.get("author").and_then(|v| v.as_str()).unwrap_or("");
    let title = entry.get("title").and_then(|v| v.as_str()).unwrap_or("");
    let year = entry.get("year").and_then(|v| v.as_str()).unwrap_or("");
    let empty_arr = Value::Array(vec![]);
    let fields = entry.get("fields").unwrap_or(&empty_arr);

    sqlx::query(
        "INSERT INTO bibliography (id, code, author, title, year, fields, fetched_at, sync_status, status_changed_at)
         VALUES ($1, $2, $3, $4, $5, $6, now(), 'idle', now())
         ON CONFLICT (id) DO UPDATE
         SET code = $2, author = $3, title = $4, year = $5, fields = $6, fetched_at = now(),
             sync_status = 'idle', status_changed_at = now()",
    )
    .bind(bibl_id)
    .bind(code)
    .bind(author)
    .bind(title)
    .bind(year)
    .bind(fields)
    .execute(db)
    .await?;

    Ok(())
}

/// Store a fetched place entry.
pub async fn store_place(db: &PgPool, place_id: i64, entry: &Value) -> Result<()> {
    let place_name = entry
        .get("place_name")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let place_name_full = entry
        .get("place_name_full")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let place_type = entry
        .get("place_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let parent_id = entry.get("parent_id").and_then(serde_json::Value::as_i64);
    let place_order = entry
        .get("place_order")
        .and_then(serde_json::Value::as_i64)
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0);
    let municipality_nr = entry
        .get("municipality_nr")
        .and_then(|v| v.as_str())
        .map(std::string::ToString::to_string);
    let weight_threshold = entry
        .get("weight_threshold")
        .and_then(serde_json::Value::as_i64)
        .and_then(|v| i32::try_from(v).ok())
        .unwrap_or(0);

    sqlx::query(
        "INSERT INTO places (id, place_name, place_name_full, place_type, parent_id, place_order, municipality_nr, weight_threshold, fetched_at, sync_status, status_changed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), 'idle', now())
         ON CONFLICT (id) DO UPDATE
         SET place_name = $2, place_name_full = $3, place_type = $4, parent_id = $5, place_order = $6, municipality_nr = $7, weight_threshold = $8, fetched_at = now(),
             sync_status = 'idle', status_changed_at = now()",
    )
    .bind(place_id)
    .bind(place_name)
    .bind(place_name_full)
    .bind(place_type)
    .bind(parent_id)
    .bind(place_order)
    .bind(municipality_nr)
    .bind(weight_threshold)
    .execute(db)
    .await?;

    Ok(())
}

/// Mark articles that depend on an entity for re-indexing.
async fn mark_articles_for_reindex(
    db: &PgPool,
    join_table: &str,
    entity_column: &str,
    entity_id: i64,
) -> Result<Vec<(String, i64)>> {
    let query = format!(
        "UPDATE articles a
         SET sync_status = 'pending_index', status_changed_at = now()
         FROM {join_table} j
         WHERE a.dictionary = j.dictionary AND a.id = j.article_id
           AND j.{entity_column} = $1 AND a.sync_status = 'idle'
         RETURNING a.dictionary, a.id"
    );
    let rows: Vec<(String, i64)> = sqlx::query_as(&query).bind(entity_id).fetch_all(db).await?;
    Ok(rows)
}

/// Mark articles that depend on a bibliography entry for re-indexing.
pub async fn mark_articles_for_reindex_by_bibl(
    db: &PgPool,
    bibl_id: i64,
) -> Result<Vec<(String, i64)>> {
    mark_articles_for_reindex(db, "article_bibliography", "bibl_id", bibl_id).await
}

/// Mark articles that depend on a place entry for re-indexing.
pub async fn mark_articles_for_reindex_by_place(
    db: &PgPool,
    place_id: i64,
) -> Result<Vec<(String, i64)>> {
    mark_articles_for_reindex(db, "article_place", "place_id", place_id).await
}

/// Get all article metadata for a dictionary.
pub async fn get_all_article_metadata(
    db: &PgPool,
    dict: UibDictionary,
) -> Result<HashMap<i64, StoredArticleMetadata>> {
    let rows: Vec<(i64, String, i64, String, String)> = sqlx::query_as(
        "SELECT id, primary_lemma, revision, updated_at, sync_status
         FROM articles WHERE dictionary = $1",
    )
    .bind(dict.as_str())
    .fetch_all(db)
    .await?;

    let mut map = HashMap::new();
    for (id, primary_lemma, revision, updated_at, status_str) in rows {
        map.insert(
            id,
            StoredArticleMetadata {
                primary_lemma,
                revision: Some(revision),
                updated_at,
                sync_status: status_str.parse().unwrap_or(SyncStatus::Idle),
            },
        );
    }
    Ok(map)
}

/// Store inline refs and resolve codes against bibliography and place data.
#[allow(clippy::too_many_lines)]
pub async fn store_inline_refs(
    tx: &mut Transaction<'_, Postgres>,
    dict_str: &str,
    article_id: i64,
    refs: &[InlineRef],
) -> Result<(Vec<i64>, Vec<String>)> {
    sqlx::query("DELETE FROM inline_ref_parse WHERE dictionary = $1 AND article_id = $2")
        .bind(dict_str)
        .bind(article_id)
        .execute(&mut **tx)
        .await?;

    if refs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let codes: Vec<&str> = refs
        .iter()
        .map(|r| r.code.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let resolved_bibl: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, code FROM bibliography WHERE code = ANY($1)")
            .bind(&codes)
            .fetch_all(&mut **tx)
            .await?;

    let mut code_to_bibl: HashMap<&str, i64> = HashMap::new();
    for (id, code) in &resolved_bibl {
        code_to_bibl.entry(code.as_str()).or_insert(*id);
    }

    let unresolved_codes: Vec<&str> = codes
        .iter()
        .filter(|c| !code_to_bibl.contains_key(*c))
        .copied()
        .collect();

    let mut code_to_place: HashMap<&str, i64> = HashMap::new();
    if !unresolved_codes.is_empty() {
        let place_names: Vec<String> = unresolved_codes
            .iter()
            .flat_map(|c| {
                let base = c.to_string();
                let stripped = c.strip_suffix('M').map(str::to_string);
                std::iter::once(base).chain(stripped)
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let resolved_places: Vec<(i64, String)> =
            sqlx::query_as("SELECT id, place_name FROM places WHERE place_name = ANY($1)")
                .bind(&place_names)
                .fetch_all(&mut **tx)
                .await?;

        let place_name_to_id: HashMap<&str, i64> = resolved_places
            .iter()
            .map(|(id, name)| (name.as_str(), *id))
            .collect();

        for code in &unresolved_codes {
            if let Some(&pid) = place_name_to_id.get(*code) {
                code_to_place.insert(code, pid);
            } else if let Some(stripped) = code.strip_suffix('M')
                && let Some(&pid) = place_name_to_id.get(stripped)
            {
                code_to_place.insert(code, pid);
            }
        }
    }

    let mut resolved_bibl_ids = Vec::new();

    let mut qb = sqlx::QueryBuilder::new(
        "INSERT INTO inline_ref_parse \
         (dictionary, article_id, quote_content, offset_start, offset_end, code, spec, ref_type, bibl_id, place_id) ",
    );
    qb.push_values(refs, |mut b, r| {
        let bibl_id = code_to_bibl.get(r.code.as_str()).copied();
        let place_id = code_to_place.get(r.code.as_str()).copied();
        let ref_type = if bibl_id.is_some() {
            Some("bibl")
        } else if place_id.is_some() {
            Some("place")
        } else {
            None
        };

        if let Some(id) = bibl_id {
            resolved_bibl_ids.push(id);
        }

        b.push_bind(dict_str.to_owned())
            .push_bind(article_id)
            .push_bind(r.quote_content.to_string())
            .push_bind(i32::try_from(r.offset_start).unwrap_or(i32::MAX))
            .push_bind(i32::try_from(r.offset_end).unwrap_or(i32::MAX))
            .push_bind(r.code.clone())
            .push_bind(r.spec.clone())
            .push_bind(ref_type)
            .push_bind(bibl_id)
            .push_bind(place_id);
    });
    qb.build().execute(&mut **tx).await?;

    let still_unresolved: Vec<String> = refs
        .iter()
        .filter(|r| {
            !code_to_bibl.contains_key(r.code.as_str())
                && !code_to_place.contains_key(r.code.as_str())
        })
        .map(|r| r.code.as_str())
        .collect::<HashSet<_>>()
        .into_iter()
        .map(String::from)
        .collect();

    Ok((resolved_bibl_ids, still_unresolved))
}

/// Resolve pending refs as bibliography after a bibliography entry is synced.
pub async fn resolve_inline_ref_as_bibl(db: &PgPool, bibl_id: i64, code: &str) -> Result<u64> {
    let result = sqlx::query(
        "UPDATE inline_ref_parse SET bibl_id = $1, ref_type = 'bibl'
         WHERE code = $2 AND ref_type IS NULL",
    )
    .bind(bibl_id)
    .bind(code)
    .execute(db)
    .await?;

    if result.rows_affected() > 0 {
        sqlx::query(
            "INSERT INTO article_bibliography (dictionary, article_id, bibl_id)
             SELECT DISTINCT dictionary, article_id, $1
             FROM inline_ref_parse
             WHERE code = $2 AND bibl_id = $1
             ON CONFLICT DO NOTHING",
        )
        .bind(bibl_id)
        .bind(code)
        .execute(db)
        .await?;
    }

    Ok(result.rows_affected())
}

/// Resolve pending inline refs as place after a place entry is synced.
pub async fn resolve_inline_place_by_name(
    db: &PgPool,
    place_id: i64,
    place_name: &str,
) -> Result<u64> {
    let code_with_m = format!("{place_name}M");
    let codes = vec![place_name.to_string(), code_with_m];

    let result = sqlx::query(
        "UPDATE inline_ref_parse SET place_id = $1, ref_type = 'place'
         WHERE code = ANY($2) AND ref_type IS NULL",
    )
    .bind(place_id)
    .bind(&codes)
    .execute(db)
    .await?;

    Ok(result.rows_affected())
}

/// Return IDs from `ids` that don't already exist in the given table.
async fn filter_missing_ids(
    tx: &mut Transaction<'_, Postgres>,
    table: &str,
    ids: &[i64],
) -> Result<Vec<i64>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let query = match table {
        "bibliography" => "SELECT id FROM bibliography WHERE id = ANY($1)",
        "places" => "SELECT id FROM places WHERE id = ANY($1)",
        _ => return Ok(ids.to_vec()),
    };

    let existing: Vec<(i64,)> = sqlx::query_as(query).bind(ids).fetch_all(&mut **tx).await?;
    let existing_set: HashSet<i64> = existing.into_iter().map(|(id,)| id).collect();

    Ok(ids
        .iter()
        .copied()
        .filter(|id| !existing_set.contains(id))
        .collect())
}

/// Return article IDs from `ids` that don't already exist for the given dictionary.
async fn filter_missing_article_ids(
    tx: &mut Transaction<'_, Postgres>,
    dict: &str,
    ids: &[i64],
) -> Result<Vec<i64>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let existing: Vec<(i64,)> =
        sqlx::query_as("SELECT id FROM articles WHERE dictionary = $1 AND id = ANY($2)")
            .bind(dict)
            .bind(ids)
            .fetch_all(&mut **tx)
            .await?;
    let existing_set: HashSet<i64> = existing.into_iter().map(|(id,)| id).collect();
    Ok(ids
        .iter()
        .copied()
        .filter(|id| !existing_set.contains(id))
        .collect())
}

/// Write an outbox entry.
async fn write_outbox(
    tx: &mut Transaction<'_, Postgres>,
    job_type: &str,
    key: &str,
    payload: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO job_outbox (job_type, job_key, payload)
         SELECT $1, $2, $3
         WHERE NOT EXISTS (
             SELECT 1 FROM job_outbox
             WHERE job_type = $1 AND job_key = $2 AND processed_at IS NULL
         )",
    )
    .bind(job_type)
    .bind(key)
    .bind(payload)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Write outbox entry for fetching an article.
pub async fn write_outbox_fetch_article(
    tx: &mut Transaction<'_, Postgres>,
    dict: &str,
    article_id: i64,
) -> Result<()> {
    write_outbox_fetch_article_with_meta(tx, dict, article_id, None, "").await
}

/// Write outbox entry for fetching an article with metadata from the article
/// list.
pub async fn write_outbox_fetch_article_with_meta(
    tx: &mut Transaction<'_, Postgres>,
    dict: &str,
    article_id: i64,
    revision: Option<i64>,
    updated_at: &str,
) -> Result<()> {
    let key = format!("{dict}:{article_id}");
    let payload = serde_json::json!({
        "dictionary": dict,
        "article_id": article_id,
        "revision": revision,
        "updated_at": updated_at,
    });
    write_outbox(tx, "fetch_article", &key, &payload).await
}

/// Write outbox entry for indexing an article.
pub async fn write_outbox_index_article(
    tx: &mut Transaction<'_, Postgres>,
    dict: &str,
    article_id: i64,
) -> Result<()> {
    let key = format!("{dict}:{article_id}");
    let payload = serde_json::json!({"article_keys": [&key]});
    write_outbox(tx, "batch_index", &key, &payload).await
}

/// Write outbox entry for fetching a bibliography entry.
pub async fn write_outbox_fetch_bibl(
    tx: &mut Transaction<'_, Postgres>,
    bibl_id: i64,
) -> Result<()> {
    let key = bibl_id.to_string();
    let payload = serde_json::json!({"bibl_id": bibl_id});
    write_outbox(tx, "fetch_bibliography", &key, &payload).await
}

/// Write outbox entry for fetching a place entry.
pub async fn write_outbox_fetch_place(
    tx: &mut Transaction<'_, Postgres>,
    place_id: i64,
) -> Result<()> {
    let key = place_id.to_string();
    let payload = serde_json::json!({"place_id": place_id});
    write_outbox(tx, "fetch_place", &key, &payload).await
}

/// Write outbox entry for resolving an inline ref code.
pub async fn write_outbox_resolve_code(
    tx: &mut Transaction<'_, Postgres>,
    code: &str,
) -> Result<()> {
    let payload = serde_json::json!({"code": code});
    write_outbox(tx, "resolve_inline_code", code, &payload).await
}

/// Write outbox entry for batch indexing articles.
pub async fn write_outbox_batch_index(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    article_keys: &[(String, i64)],
) -> Result<()> {
    let keys: Vec<String> = article_keys
        .iter()
        .map(|(d, id)| format!("{d}:{id}"))
        .collect();
    let payload = serde_json::json!({"article_keys": keys});
    write_outbox(tx, "batch_index", key, &payload).await
}

/// Load the concept map for a dictionary from the `dictionary_metadata` table.
pub async fn load_concepts(db: &PgPool, dictionary: &str) -> Result<ConceptMap> {
    let row: Option<(Value,)> = sqlx::query_as(
        "SELECT data FROM dictionary_metadata WHERE dictionary = $1 AND key = 'concepts'",
    )
    .bind(dictionary)
    .fetch_optional(db)
    .await?;

    Ok(match row {
        Some((data,)) => crate::extraction::build_concept_map(&data),
        None => HashMap::new(),
    })
}

/// Claim articles for indexing.
pub async fn claim_articles_for_indexing(
    db: &PgPool,
    article_keys: &[(String, i64)],
) -> Result<Vec<(String, i64)>> {
    let dict_strs: Vec<&str> = article_keys.iter().map(|(d, _)| d.as_str()).collect();
    let ids: Vec<i64> = article_keys.iter().map(|(_, id)| *id).collect();

    let claimed: Vec<(String, i64)> = sqlx::query_as(
        "UPDATE articles
         SET sync_status = 'pending_index', status_changed_at = now()
         WHERE (dictionary, id) IN (SELECT * FROM UNNEST($1::text[], $2::bigint[]))
           AND sync_status = 'pending_index'
         RETURNING dictionary, id",
    )
    .bind(&dict_strs)
    .bind(&ids)
    .fetch_all(db)
    .await?;

    Ok(claimed)
}

/// Load place data for a batch of articles.
pub async fn load_batch_place_data(
    db: &PgPool,
    articles: &[(String, i64)],
) -> Result<(ArticlePlaceMap, HashMap<i64, (String, String, String)>)> {
    let dicts: Vec<&str> = articles.iter().map(|(d, _)| d.as_str()).collect();
    let ids: Vec<i64> = articles.iter().map(|(_, id)| *id).collect();

    let place_rows: Vec<(String, i64, i64, String)> = sqlx::query_as(
        "SELECT ap.dictionary, ap.article_id, ap.place_id, ap.context
         FROM article_place ap
         WHERE (ap.dictionary, ap.article_id) IN
         (SELECT * FROM UNNEST($1::text[], $2::bigint[]))",
    )
    .bind(&dicts)
    .bind(&ids)
    .fetch_all(db)
    .await?;

    let mut article_place_map: ArticlePlaceMap = HashMap::new();
    let mut needed_place_ids: HashSet<i64> = HashSet::new();

    for (dict, article_id, place_id, context) in place_rows {
        needed_place_ids.insert(place_id);
        let entry = article_place_map
            .entry(dict)
            .or_default()
            .entry(article_id)
            .or_default();
        match context.as_str() {
            "dialect" => entry.0.push(place_id),
            "attestation" => entry.1.push(place_id),
            _ => {}
        }
    }

    let place_map = if needed_place_ids.is_empty() {
        HashMap::new()
    } else {
        let place_id_vec: Vec<i64> = needed_place_ids.into_iter().collect();
        let place_meta: Vec<(i64, String, String, String)> = sqlx::query_as(
            "SELECT id, place_name, place_name_full, place_type FROM places WHERE id = ANY($1)",
        )
        .bind(&place_id_vec)
        .fetch_all(db)
        .await?;

        place_meta
            .into_iter()
            .map(|(id, name, full_name, ptype)| (id, (name, full_name, ptype)))
            .collect()
    };

    Ok((article_place_map, place_map))
}

/// Load bibliography entries by ID.
pub async fn load_bibliography_by_ids(
    db: &PgPool,
    ids: &[i64],
) -> Result<HashMap<i64, (String, String, String, String)>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows: Vec<(i64, String, String, String, String)> =
        sqlx::query_as("SELECT id, code, author, title, year FROM bibliography WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(db)
            .await?;

    Ok(rows
        .into_iter()
        .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
        .collect())
}

/// Load article data for a dictionary in chunks.
pub async fn load_articles_data(
    db: &PgPool,
    dict: &str,
    article_ids: &[i64],
) -> Result<Vec<(i64, Value)>> {
    let rows: Vec<(i64, Value)> = sqlx::query_as(
        "SELECT id, data FROM articles WHERE dictionary = $1 AND id = ANY($2::bigint[])",
    )
    .bind(dict)
    .bind(article_ids)
    .fetch_all(db)
    .await?;

    Ok(rows)
}

/// Mark articles as indexed.
pub async fn mark_articles_indexed(db: &PgPool, dict: &str, article_ids: &[i64]) -> Result<()> {
    sqlx::query(
        "UPDATE articles SET sync_status = 'idle', status_changed_at = now()
         WHERE dictionary = $1 AND id = ANY($2::bigint[]) AND sync_status = 'pending_index'",
    )
    .bind(dict)
    .bind(article_ids)
    .execute(db)
    .await?;
    Ok(())
}
