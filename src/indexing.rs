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

use anyhow::{Result, anyhow};
use meilisearch_sdk::client::Client as MeiliClient;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

use crate::extraction;
use crate::meili;
use crate::state::UibDictionary;
use crate::storage;

/// Maps articles to location data.
type ArticlePlaceMap = HashMap<(String, i64), (Vec<i64>, Vec<i64>)>;

/// Index a single article into Meilisearch.
pub async fn index_article(
    meili: &MeiliClient,
    db: &PgPool,
    dict: UibDictionary,
    article_id: i64,
) -> Result<()> {
    let row: Option<(Value,)> =
        sqlx::query_as("SELECT data FROM articles WHERE dictionary = $1 AND id = $2")
            .bind(dict.as_str())
            .bind(article_id)
            .fetch_optional(db)
            .await?;

    let (article_data,) =
        row.ok_or_else(|| anyhow!("Article {dict}:{article_id} not found for indexing."))?;

    let bib = load_article_bibliography(db, &article_data).await?;
    let places = load_article_place_data(db, dict, article_id).await?;
    let doc = meili::build_search_document(
        dict.as_str(),
        article_id,
        &article_data,
        Some(&bib),
        Some(&places),
    );

    let idx = meili.index(meili::index_name(dict.as_str()));
    idx.add_or_replace(&[doc], Some("id"))
        .await
        .map_err(|e| anyhow!("Failed to index article in Meilisearch: {e}"))?;

    storage::mark_article_indexed(db, dict.as_str(), article_id).await?;
    Ok(())
}

/// Index multiple articles into Meilisearch in a batch.
pub async fn batch_index_articles(
    meili: &MeiliClient,
    db: &PgPool,
    article_keys: &[(String, i64)],
) -> Result<()> {
    if article_keys.is_empty() {
        return Ok(());
    }

    // Only index articles that are still pending indexing.
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

    if claimed.is_empty() {
        return Ok(());
    }

    let (article_place_map, place_map) = load_batch_place_data(db, &claimed).await?;
    let bib_map = load_batch_bibliography(db).await?;

    let mut by_dict: HashMap<String, Vec<i64>> = HashMap::new();
    for (dict, id) in &claimed {
        by_dict.entry(dict.clone()).or_default().push(*id);
    }

    for (dict, article_ids) in &by_dict {
        index_dict_batch(
            meili,
            db,
            dict,
            article_ids,
            &article_place_map,
            &place_map,
            &bib_map,
        )
        .await?;
    }

    Ok(())
}

/// Load place data for a batch of articles.
async fn load_batch_place_data(
    db: &PgPool,
    claimed: &[(String, i64)],
) -> Result<(ArticlePlaceMap, HashMap<i64, (String, String, String)>)> {
    let claimed_dicts: Vec<&str> = claimed.iter().map(|(d, _)| d.as_str()).collect();
    let claimed_ids: Vec<i64> = claimed.iter().map(|(_, id)| *id).collect();

    let place_rows: Vec<(String, i64, i64, String)> = sqlx::query_as(
        "SELECT ap.dictionary, ap.article_id, ap.place_id, ap.context
         FROM article_place ap
         WHERE (ap.dictionary, ap.article_id) IN
         (SELECT * FROM UNNEST($1::text[], $2::bigint[]))",
    )
    .bind(&claimed_dicts)
    .bind(&claimed_ids)
    .fetch_all(db)
    .await?;

    let mut article_place_map: ArticlePlaceMap = HashMap::new();
    let mut needed_place_ids: HashSet<i64> = HashSet::new();

    for (dict, article_id, place_id, context) in place_rows {
        needed_place_ids.insert(place_id);
        let entry = article_place_map.entry((dict, article_id)).or_default();
        match context.as_str() {
            "dialect" => entry.0.push(place_id),
            "attestation" => entry.1.push(place_id),
            _ => {}
        }
    }

    let place_id_vec: Vec<i64> = needed_place_ids.into_iter().collect();
    let place_meta: Vec<(i64, String, String, String)> = if place_id_vec.is_empty() {
        Vec::new()
    } else {
        sqlx::query_as(
            "SELECT id, place_name, place_name_full, place_type FROM places WHERE id = ANY($1)",
        )
        .bind(&place_id_vec)
        .fetch_all(db)
        .await?
    };

    let place_map: HashMap<i64, (String, String, String)> = place_meta
        .into_iter()
        .map(|(id, name, full_name, ptype)| (id, (name, full_name, ptype)))
        .collect();

    Ok((article_place_map, place_map))
}

/// Load bibliography metadata for a batch of articles.
async fn load_batch_bibliography(
    db: &PgPool,
) -> Result<HashMap<i64, (String, String, String, String)>> {
    let bib_rows: Vec<(i64, String, String, String, String)> =
        sqlx::query_as("SELECT id, code, author, title, year FROM bibliography")
            .fetch_all(db)
            .await?;

    Ok(bib_rows
        .into_iter()
        .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
        .collect())
}

/// Index a batch of articles for a single dictionary.
async fn index_dict_batch(
    meili: &MeiliClient,
    db: &PgPool,
    dict: &str,
    article_ids: &[i64],
    article_place_map: &ArticlePlaceMap,
    place_map: &HashMap<i64, (String, String, String)>,
    bib_map: &HashMap<i64, (String, String, String, String)>,
) -> Result<()> {
    let idx = meili.index(meili::index_name(dict));
    let mut batch: Vec<meili::ArticleSearchDocument> = Vec::with_capacity(5000);
    let mut tasks = Vec::new();

    for chunk in article_ids.chunks(5000) {
        let rows: Vec<(i64, Value)> = sqlx::query_as(
            "SELECT id, data FROM articles WHERE dictionary = $1 AND id = ANY($2::bigint[])",
        )
        .bind(dict)
        .bind(chunk)
        .fetch_all(db)
        .await?;

        for (id, data) in &rows {
            let bib = meili::build_article_bibliography(data, bib_map);
            let (dialect_ids, attestation_ids) = article_place_map
                .get(&(dict.to_string(), *id))
                .cloned()
                .unwrap_or_default();
            let places =
                meili::build_article_place_data_split(&dialect_ids, &attestation_ids, place_map);
            batch.push(meili::build_search_document(
                dict,
                *id,
                data,
                Some(&bib),
                Some(&places),
            ));

            if batch.len() >= 5000 {
                tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
                batch.clear();
            }
        }
    }

    if !batch.is_empty() {
        tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
    }

    let timeout = Some(std::time::Duration::from_mins(5));
    for task in tasks {
        task.wait_for_completion(meili, None, timeout).await?;
    }

    // Mark all articles in this dictionary as indexed.
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

/// Index a bibliography entry in Meilisearch.
pub async fn index_bibliography(meili: &MeiliClient, bibl_id: i64, entry: &Value) -> Result<()> {
    let doc = meili::build_bibliography_document(bibl_id, entry);
    let idx = meili.index(meili::BIBLIOGRAPHY_INDEX);
    idx.add_or_replace(&[doc], Some("id"))
        .await
        .map_err(|e| anyhow!("Failed to index bibliography in Meilisearch: {e}"))?;
    Ok(())
}

/// Index a place entry in Meilisearch.
pub async fn index_place(meili: &MeiliClient, place_id: i64, entry: &Value) -> Result<()> {
    let doc = meili::PlaceSearchDocument {
        id: place_id,
        place_name: entry
            .get("place_name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        place_name_full: entry
            .get("place_name_full")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        place_type: entry
            .get("place_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        parent_id: entry.get("parent_id").and_then(serde_json::Value::as_i64),
        municipality_nr: entry
            .get("municipality_nr")
            .and_then(|v| v.as_str())
            .map(std::string::ToString::to_string),
    };

    let idx = meili.index(meili::PLACE_INDEX);
    idx.add_or_replace(&[doc], Some("id"))
        .await
        .map_err(|e| anyhow!("Failed to index place in Meilisearch: {e}"))?;
    Ok(())
}

/// Load bibliography metadata for building an article search document.
async fn load_article_bibliography(
    db: &PgPool,
    article_data: &Value,
) -> Result<meili::ArticleBibliography> {
    let mut bibl_ids = HashSet::new();
    extraction::collect_bibl_ids(article_data, &mut bibl_ids);

    if bibl_ids.is_empty() {
        return Ok(meili::ArticleBibliography::empty());
    }

    let ids_vec: Vec<i64> = bibl_ids.into_iter().collect();
    let rows: Vec<(i64, String, String, String, String)> =
        sqlx::query_as("SELECT id, code, author, title, year FROM bibliography WHERE id = ANY($1)")
            .bind(&ids_vec)
            .fetch_all(db)
            .await?;

    let bib_map: HashMap<i64, (String, String, String, String)> = rows
        .into_iter()
        .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
        .collect();

    Ok(meili::build_article_bibliography(article_data, &bib_map))
}

/// Load place metadata for building an article search document.
async fn load_article_place_data(
    db: &PgPool,
    dict: UibDictionary,
    article_id: i64,
) -> Result<meili::ArticlePlaceData> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT place_id, context FROM article_place WHERE dictionary = $1 AND article_id = $2",
    )
    .bind(dict.as_str())
    .bind(article_id)
    .fetch_all(db)
    .await?;

    if rows.is_empty() {
        return Ok(meili::ArticlePlaceData::default());
    }

    let mut dialect_ids = Vec::new();
    let mut attestation_ids = Vec::new();
    let mut all_ids = HashSet::new();
    for (pid, ctx) in &rows {
        all_ids.insert(*pid);
        match ctx.as_str() {
            "dialect" => dialect_ids.push(*pid),
            "attestation" => attestation_ids.push(*pid),
            _ => {}
        }
    }

    let unique_ids: Vec<i64> = all_ids.into_iter().collect();
    let place_rows: Vec<(i64, String, String, String)> = sqlx::query_as(
        "SELECT id, place_name, place_name_full, place_type FROM places WHERE id = ANY($1)",
    )
    .bind(&unique_ids)
    .fetch_all(db)
    .await?;

    let place_map: HashMap<i64, (String, String, String)> = place_rows
        .into_iter()
        .map(|(id, name, full_name, place_type)| (id, (name, full_name, place_type)))
        .collect();

    Ok(meili::build_article_place_data_split(
        &dialect_ids,
        &attestation_ids,
        &place_map,
    ))
}
