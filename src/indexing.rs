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
use std::collections::HashMap;

use crate::extraction;
use crate::meili;
use crate::storage;

use storage::ArticlePlaceMap;

/// Index multiple articles into Meilisearch in a batch.
pub async fn batch_index_articles(
    meili: &MeiliClient,
    db: &PgPool,
    article_keys: &[(String, i64)],
) -> Result<()> {
    if article_keys.is_empty() {
        return Ok(());
    }

    let claimed = storage::claim_articles_for_indexing(db, article_keys).await?;
    if claimed.is_empty() {
        return Ok(());
    }

    let (article_place_map, place_map) = storage::load_batch_place_data(db, &claimed).await?;

    let mut by_dict: HashMap<String, Vec<i64>> = HashMap::new();
    for (dict, id) in claimed {
        by_dict.entry(dict).or_default().push(id);
    }

    for (dict, article_ids) in &by_dict {
        index_dict_batch(meili, db, dict, article_ids, &article_place_map, &place_map).await?;
    }

    Ok(())
}

/// Index a batch of articles for a single dictionary.
async fn index_dict_batch(
    meili: &MeiliClient,
    db: &PgPool,
    dict: &str,
    article_ids: &[i64],
    article_place_map: &ArticlePlaceMap,
    place_map: &HashMap<i64, (String, String, String)>,
) -> Result<()> {
    let idx = meili.index(meili::index_name(dict));
    let concepts = storage::load_concepts(db, dict).await?;
    let mut batch: Vec<meili::ArticleSearchDocument> = Vec::with_capacity(5000);
    let mut tasks = Vec::new();
    let empty_place = (Vec::new(), Vec::new());

    for chunk in article_ids.chunks(5000) {
        let rows = storage::load_articles_data(db, dict, chunk).await?;

        let bibl_ids = extraction::collect_all_bibl_ids(rows.iter().map(|(_, data)| data));
        let bib_map = storage::load_bibliography_by_ids(db, &bibl_ids).await?;
        let dict_places = article_place_map.get(dict);

        for (id, data) in &rows {
            let bib = extraction::build_article_bibliography(data, &bib_map);
            let (dialect_ids, attestation_ids) =
                dict_places.and_then(|m| m.get(id)).unwrap_or(&empty_place);
            let places =
                extraction::build_article_place_data(dialect_ids, attestation_ids, place_map);
            batch.push(meili::build_search_document(
                dict,
                *id,
                data,
                Some(bib),
                Some(places),
                &concepts,
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

    storage::mark_articles_indexed(db, dict, article_ids).await?;
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
