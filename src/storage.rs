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

/// Metadata stored in the DB for an article.
#[derive(Debug)]
pub struct StoredArticleMetadata {
    pub primary_lemma: String,
    pub revision: Option<i64>,
    pub updated_at: String,
    pub sync_status: SyncStatus,
}

/// Transition articles to pending fetch.
pub async fn mark_articles_pending_fetch(
    tx: &mut Transaction<'_, Postgres>,
    dict: UibDictionary,
    article_ids: &[i64],
) -> Result<Vec<i64>> {
    if article_ids.is_empty() {
        return Ok(Vec::new());
    }

    let dict_str = dict.as_str();

    sqlx::query(
        "INSERT INTO articles (dictionary, id, data, sync_status, status_changed_at)
         SELECT $1, unnest($2::bigint[]), '{}'::jsonb, 'pending_fetch', now()
         ON CONFLICT (dictionary, id) DO NOTHING",
    )
    .bind(dict_str)
    .bind(article_ids)
    .execute(&mut **tx)
    .await?;

    let _transitioned: Vec<(i64,)> = sqlx::query_as(
        "UPDATE articles
         SET sync_status = 'pending_fetch', status_changed_at = now()
         WHERE dictionary = $1 AND id = ANY($2) AND sync_status = 'idle'
         RETURNING id",
    )
    .bind(dict_str)
    .bind(article_ids)
    .fetch_all(&mut **tx)
    .await?;

    let newly_inserted: Vec<(i64,)> = sqlx::query_as(
        "SELECT id FROM articles
         WHERE dictionary = $1 AND id = ANY($2) AND sync_status = 'pending_fetch'",
    )
    .bind(dict_str)
    .bind(article_ids)
    .fetch_all(&mut **tx)
    .await?;

    let result: HashSet<i64> = newly_inserted.into_iter().map(|(id,)| id).collect();
    Ok(result.into_iter().collect())
}

/// Mark an entity (bibliography or place) as not found.
pub async fn mark_entity_not_found(db: &PgPool, table: &str, id: i64) -> Result<()> {
    let query = format!(
        "UPDATE {table} SET sync_status = 'not_found', status_changed_at = now() WHERE id = $1"
    );
    sqlx::query(&query).bind(id).execute(db).await?;
    Ok(())
}

/// Mark bibliography entries as pending fetch.
pub async fn mark_bibl_pending_fetch(
    tx: &mut Transaction<'_, Postgres>,
    bibl_ids: &[i64],
) -> Result<Vec<i64>> {
    if bibl_ids.is_empty() {
        return Ok(Vec::new());
    }

    sqlx::query(
        "INSERT INTO bibliography (id, sync_status, status_changed_at)
         VALUES (unnest($1::bigint[]), 'pending_fetch', now())
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(bibl_ids)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "UPDATE bibliography
         SET sync_status = 'pending_fetch', status_changed_at = now()
         WHERE id = ANY($1) AND (
             sync_status = 'idle'
             OR (sync_status = 'not_found' AND status_changed_at < now() - interval '24 hours')
         )",
    )
    .bind(bibl_ids)
    .execute(&mut **tx)
    .await?;

    let result: Vec<(i64,)> = sqlx::query_as(
        "SELECT id FROM bibliography
         WHERE id = ANY($1) AND sync_status = 'pending_fetch'",
    )
    .bind(bibl_ids)
    .fetch_all(&mut **tx)
    .await?;

    Ok(result.into_iter().map(|(id,)| id).collect())
}

/// Mark place entries as pending fetch.
pub async fn mark_places_pending_fetch(
    tx: &mut Transaction<'_, Postgres>,
    place_ids: &[i64],
) -> Result<Vec<i64>> {
    if place_ids.is_empty() {
        return Ok(Vec::new());
    }

    sqlx::query(
        "INSERT INTO places (id, sync_status, status_changed_at)
         VALUES (unnest($1::bigint[]), 'pending_fetch', now())
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(place_ids)
    .execute(&mut **tx)
    .await?;

    sqlx::query(
        "UPDATE places
         SET sync_status = 'pending_fetch', status_changed_at = now()
         WHERE id = ANY($1) AND (
             sync_status = 'idle'
             OR (sync_status = 'not_found' AND status_changed_at < now() - interval '24 hours')
         )",
    )
    .bind(place_ids)
    .execute(&mut **tx)
    .await?;

    let result: Vec<(i64,)> = sqlx::query_as(
        "SELECT id FROM places WHERE id = ANY($1) AND sync_status = 'pending_fetch'",
    )
    .bind(place_ids)
    .fetch_all(&mut **tx)
    .await?;

    Ok(result.into_iter().map(|(id,)| id).collect())
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

    let all_bibl_ids: Vec<i64> = analysis
        .bibl_ids
        .iter()
        .chain(inline_resolved_bibl_ids.iter())
        .copied()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    let bibl_to_fetch = mark_bibl_pending_fetch(&mut tx, &all_bibl_ids).await?;

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

    let places_to_fetch = mark_places_pending_fetch(&mut tx, &all_place_ids).await?;

    let related_to_fetch = if analysis.related_article_ids.is_empty() {
        Vec::new()
    } else {
        mark_articles_pending_fetch(&mut tx, dict, &analysis.related_article_ids).await?
    };

    write_store_article_outbox(
        &mut tx,
        dict_str,
        article_id,
        &bibl_to_fetch,
        &places_to_fetch,
        &related_to_fetch,
        &unresolved_codes,
    )
    .await?;

    tx.commit().await?;

    Ok(StoreArticleResult {
        bibl_fetched: bibl_to_fetch.len(),
        places_fetched: places_to_fetch.len(),
        related_fetched: related_to_fetch.len(),
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

/// Write outbox entries for a stored article and its relationships.
async fn write_store_article_outbox(
    tx: &mut Transaction<'_, Postgres>,
    dict_str: &str,
    article_id: i64,
    bibl_to_fetch: &[i64],
    places_to_fetch: &[i64],
    related_to_fetch: &[i64],
    unresolved_codes: &[String],
) -> Result<()> {
    write_outbox_index_article(&mut *tx, dict_str, article_id).await?;

    for bibl_id in bibl_to_fetch {
        write_outbox_fetch_bibl(&mut *tx, *bibl_id).await?;
    }
    for place_id in places_to_fetch {
        write_outbox_fetch_place(&mut *tx, *place_id).await?;
    }
    for related_id in related_to_fetch {
        write_outbox_fetch_article(&mut *tx, dict_str, *related_id).await?;
    }
    for code in unresolved_codes {
        write_outbox_resolve_code(&mut *tx, code).await?;
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

/// Mark articles that depend on a bibliography entry for re-indexing.
pub async fn mark_articles_for_reindex_by_bibl(
    db: &PgPool,
    bibl_id: i64,
) -> Result<Vec<(String, i64)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "UPDATE articles a
         SET sync_status = 'pending_index', status_changed_at = now()
         FROM article_bibliography ab
         WHERE a.dictionary = ab.dictionary AND a.id = ab.article_id
           AND ab.bibl_id = $1 AND a.sync_status = 'idle'
         RETURNING a.dictionary, a.id",
    )
    .bind(bibl_id)
    .fetch_all(db)
    .await?;

    Ok(rows)
}

/// Mark articles that depend on a place entry for re-indexing.
pub async fn mark_articles_for_reindex_by_place(
    db: &PgPool,
    place_id: i64,
) -> Result<Vec<(String, i64)>> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "UPDATE articles a
         SET sync_status = 'pending_index', status_changed_at = now()
         FROM article_place ap
         WHERE a.dictionary = ap.dictionary AND a.id = ap.article_id
           AND ap.place_id = $1 AND a.sync_status = 'idle'
         RETURNING a.dictionary, a.id",
    )
    .bind(place_id)
    .fetch_all(db)
    .await?;

    Ok(rows)
}

/// Transition an article to idle after successful indexing.
pub async fn mark_article_indexed(db: &PgPool, dict: &str, article_id: i64) -> Result<()> {
    sqlx::query(
        "UPDATE articles SET sync_status = 'idle', status_changed_at = now()
         WHERE dictionary = $1 AND id = $2 AND sync_status = 'pending_index'",
    )
    .bind(dict)
    .bind(article_id)
    .execute(db)
    .await?;
    Ok(())
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
async fn store_inline_refs(
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
                let mut names = vec![c.to_string()];
                if let Some(stripped) = c.strip_suffix('M') {
                    names.push(stripped.to_string());
                }
                names
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
            .push_bind(r.quote_content.clone())
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
        .map(|r| r.code.clone())
        .collect::<HashSet<_>>()
        .into_iter()
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

/// Write an outbox entry.
async fn write_outbox(
    tx: &mut Transaction<'_, Postgres>,
    job_type: &str,
    key: &str,
    payload: &serde_json::Value,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO job_outbox (job_type, job_key, payload)
         VALUES ($1, $2, $3)
         ON CONFLICT (job_type, job_key) WHERE processed_at IS NULL DO NOTHING",
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
    let key = format!("{dict}:{article_id}");
    let payload = serde_json::json!({"dictionary": dict, "article_id": article_id});
    write_outbox(tx, "fetch_article", &key, &payload).await
}

/// Write outbox entry for indexing an article.
pub async fn write_outbox_index_article(
    tx: &mut Transaction<'_, Postgres>,
    dict: &str,
    article_id: i64,
) -> Result<()> {
    let key = format!("{dict}:{article_id}");
    let payload = serde_json::json!({"dictionary": dict, "article_id": article_id});
    write_outbox(tx, "index_article", &key, &payload).await
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
