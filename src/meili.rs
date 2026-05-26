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

use crate::extraction::{ArticleBibliography, ArticlePlaceData};
use anyhow::Result;
use futures::StreamExt;
use meilisearch_sdk::settings::Settings;
use meilisearch_sdk::{client::Client, features::ExperimentalFeatures};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, warn};

/// Increment this when the indexes change.
pub const MEILI_SCHEMA_VERSION: i64 = 4;

/// The document shape indexed into Meilisearch for search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArticleSearchDocument {
    /// Composite key. ("{dictionary}_{article_id}")
    pub id: String,
    pub article_id: i64,
    pub dictionary: String,
    /// All lemma strings from the article.
    pub lemmas: Vec<String>,
    /// Suggestion strings.
    pub suggest: Vec<String>,
    /// All inflected word forms.
    pub inflections: Vec<String>,
    /// Concatenated etymology text.
    pub etymology_text: String,
    /// Concatenated pronunciation text.
    pub pronunciation_text: String,
    /// Dialect form strings.
    pub dialect_forms: Vec<String>,
    /// Paradigm tags, e.g. NOUN, VERB.
    pub paradigm_tags: Vec<String>,
    /// Inflection tags, e.g. Sing, Def.
    pub inflection_tags: Vec<String>,
    /// Whether the article has a split infinitive.
    pub has_split_inf: bool,
    /// Place names from visible dialect sources.
    pub dialect_places: Vec<String>,
    /// Codes from older source references.
    pub older_source_codes: Vec<String>,
    /// Authors from older source references.
    pub older_source_authors: Vec<String>,
    /// Titles from older source references.
    pub older_source_titles: Vec<String>,
    /// Years from older source references.
    pub older_source_years: Vec<String>,
    /// Codes from written form sources.
    pub written_form_source_codes: Vec<String>,
    /// Authors from written form sources.
    pub written_form_source_authors: Vec<String>,
    /// Titles from written form sources.
    pub written_form_source_titles: Vec<String>,
    /// Years from written form sources.
    pub written_form_source_years: Vec<String>,
    /// Codes from attestation sources.
    pub attestation_source_codes: Vec<String>,
    /// Authors from attestation sources.
    pub attestation_source_authors: Vec<String>,
    /// Titles from attestation sources.
    pub attestation_source_titles: Vec<String>,
    /// Years from attestation sources.
    pub attestation_source_years: Vec<String>,
    /// Etymology source language concept IDs.
    pub etymology_languages: Vec<String>,
    /// Concatenated definition text.
    pub definition_text: String,
    /// Concatenated example quote text.
    pub example_text: String,
    /// Written form strings.
    pub written_forms: Vec<String>,
    /// Lemma strings from sub-articles.
    pub sub_article_lemmas: Vec<String>,
    /// Codes from all bibliographical entries.
    pub bibliography_codes: Vec<String>,
    /// Authors from all bibliographical entries.
    pub bibliography_authors: Vec<String>,
    /// Titles from all bibliographical entries.
    pub bibliography_titles: Vec<String>,
    /// Publication years from all bibliographical entries.
    pub bibliography_years: Vec<String>,
    /// Place names from all referenced places.
    pub place_names: Vec<String>,
    /// Place codes from all referenced places.
    pub place_codes: Vec<String>,
    /// Place types from all referenced places.
    pub place_types: Vec<String>,
    /// Place names from dialect context places.
    pub dialect_place_names: Vec<String>,
    /// Place codes from dialect context places.
    pub dialect_place_codes: Vec<String>,
    /// Place types from dialect context places.
    pub dialect_place_types: Vec<String>,
    /// Place names from attestation context places.
    pub attestation_place_names: Vec<String>,
    /// Place codes from attestation context places.
    pub attestation_place_codes: Vec<String>,
    /// Place types from attestation context places.
    pub attestation_place_types: Vec<String>,
}

/// The index name for bibliography entries.
pub const BIBLIOGRAPHY_INDEX: &str = "bibliography";

/// Bibliography entry indexed in Meilisearch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BibliographySearchDocument {
    pub id: i64,
    pub code: String,
    pub author: String,
    pub title: String,
    pub year: String,
}

/// The index name for place entries.
pub const PLACE_INDEX: &str = "places";

/// Place entry indexed in Meilisearch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaceSearchDocument {
    pub id: i64,
    pub place_name: String,
    pub place_name_full: String,
    pub place_type: String,
    pub parent_id: Option<i64>,
    pub municipality_nr: Option<String>,
}

/// Build a `BibliographySearchDocument` from a Clarino API response entry.
#[must_use]
pub fn build_bibliography_document(
    bibl_id: i64,
    entry: &serde_json::Value,
) -> BibliographySearchDocument {
    BibliographySearchDocument {
        id: bibl_id,
        code: entry
            .get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        author: entry
            .get("author")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        title: entry
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        year: entry
            .get("year")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    }
}

/// Index name for a given dictionary.
#[must_use]
pub fn index_name(dict: &str) -> String {
    format!("articles-{dict}")
}

/// Build an ArticleSearchDocument from an article.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn build_search_document(
    dictionary: &str,
    article_id: i64,
    data: &serde_json::Value,
    bibliography: Option<ArticleBibliography>,
    place_data: Option<ArticlePlaceData>,
    concepts: &crate::extraction::ConceptMap,
) -> ArticleSearchDocument {
    let lemma_data = crate::extraction::extract_lemma_data(data);
    let body = crate::extraction::extract_body_content(data, concepts);
    let ArticleBibliography {
        older_source,
        written_form_source,
        attestation_source,
        all: all_bib,
        ..
    } = bibliography.unwrap_or_default();

    let place = place_data.unwrap_or_default();

    ArticleSearchDocument {
        id: format!("{dictionary}_{article_id}"),
        article_id,
        dictionary: dictionary.to_string(),
        lemmas: lemma_data.lemmas,
        suggest: lemma_data.suggest,
        inflections: lemma_data.inflections,
        etymology_text: body.etymology_parts.join(" "),
        pronunciation_text: body.pronunciation_parts.join(" "),
        dialect_forms: body.dialect_form_parts,
        paradigm_tags: lemma_data.paradigm_tags,
        inflection_tags: lemma_data.inflection_tags,
        has_split_inf: lemma_data.has_split_inf,
        dialect_places: body.dialect_places,
        older_source_codes: older_source.codes,
        older_source_authors: older_source.authors,
        older_source_titles: older_source.titles,
        older_source_years: older_source.years,
        written_form_source_codes: written_form_source.codes,
        written_form_source_authors: written_form_source.authors,
        written_form_source_titles: written_form_source.titles,
        written_form_source_years: written_form_source.years,
        attestation_source_codes: attestation_source.codes,
        attestation_source_authors: attestation_source.authors,
        attestation_source_titles: attestation_source.titles,
        attestation_source_years: attestation_source.years,
        bibliography_codes: all_bib.codes,
        bibliography_authors: all_bib.authors,
        bibliography_titles: all_bib.titles,
        bibliography_years: all_bib.years,
        place_names: place.names,
        place_codes: place.codes,
        place_types: place.types,
        dialect_place_names: place.dialect_names,
        dialect_place_codes: place.dialect_codes,
        dialect_place_types: place.dialect_types,
        attestation_place_names: place.attestation_names,
        attestation_place_codes: place.attestation_codes,
        attestation_place_types: place.attestation_types,
        etymology_languages: body.etymology_languages,
        definition_text: body.definition_parts.join(" "),
        example_text: body.example_parts.join(" "),
        written_forms: body.written_forms,
        sub_article_lemmas: body.sub_article_lemmas,
    }
}

/// Ensure all dictionary indexes exist with correct settings.
pub async fn setup_indexes(client: &Client) -> Result<()> {
    let mut features = ExperimentalFeatures::new(client);
    features.set_contains_filter(true);
    if let Err(e) = features.update().await {
        warn!(
            "Could not enable experimental features: {e}. Enable them manually if not already done."
        );
    }

    for dict in ["bm", "nn", "no"] {
        setup_article_index(client, dict).await?;
    }
    setup_bibliography_index(client).await?;
    setup_place_index(client).await?;

    Ok(())
}

async fn setup_article_index(client: &Client, dict: &str) -> Result<()> {
    let idx = index_name(dict);
    info!("Configuring Meilisearch index '{idx}'…");

    let task = client.create_index(&idx, Some("id")).await?;
    task.wait_for_completion(client, None, None).await?;

    let index = client.index(&idx);

    let settings = Settings::new()
        .with_searchable_attributes([
            "lemmas",
            "suggest",
            "inflections",
            "etymology_text",
            "pronunciation_text",
            "dialect_forms",
            "definition_text",
            "example_text",
            "written_forms",
            "sub_article_lemmas",
        ])
        .with_filterable_attributes([
            "paradigm_tags",
            "inflection_tags",
            "has_split_inf",
            "dialect_places",
            "place_names",
            "place_codes",
            "place_types",
            "dialect_place_names",
            "dialect_place_codes",
            "dialect_place_types",
            "attestation_place_names",
            "attestation_place_codes",
            "attestation_place_types",
            "older_source_codes",
            "older_source_authors",
            "older_source_titles",
            "older_source_years",
            "written_form_source_codes",
            "written_form_source_authors",
            "written_form_source_titles",
            "written_form_source_years",
            "attestation_source_codes",
            "attestation_source_authors",
            "attestation_source_titles",
            "attestation_source_years",
            "bibliography_codes",
            "bibliography_authors",
            "bibliography_titles",
            "bibliography_years",
            "etymology_languages",
            "lemmas",
            "inflections",
            "suggest",
            "dictionary",
            "article_id",
            "definition_text",
            "example_text",
            "etymology_text",
            "pronunciation_text",
            "dialect_forms",
            "written_forms",
            "sub_article_lemmas",
        ])
        .with_sortable_attributes(["article_id"])
        .with_ranking_rules([
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness",
        ])
        .with_stop_words(Vec::<String>::new())
        .with_pagination(meilisearch_sdk::settings::PaginationSetting {
            max_total_hits: 500_000,
        })
        .with_max_values_per_facet(10_000);

    let task = index.set_settings(&settings).await?;
    task.wait_for_completion(client, None, Some(std::time::Duration::from_mins(10)))
        .await?;

    info!("Meilisearch index '{idx}' configured.");
    Ok(())
}

/// Set up the bibliography index.
async fn setup_bibliography_index(client: &Client) -> Result<()> {
    info!("Configuring Meilisearch index '{BIBLIOGRAPHY_INDEX}'…");

    let task = client.create_index(BIBLIOGRAPHY_INDEX, Some("id")).await?;
    task.wait_for_completion(client, None, None).await?;

    let index = client.index(BIBLIOGRAPHY_INDEX);

    let settings = Settings::new()
        .with_searchable_attributes(["code", "author", "title", "year"])
        .with_filterable_attributes(["bibl_id", "code", "author", "title", "year"])
        .with_sortable_attributes(["year", "author"])
        .with_ranking_rules([
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness",
        ])
        .with_pagination(meilisearch_sdk::settings::PaginationSetting {
            max_total_hits: 10_000,
        });

    let task = index.set_settings(&settings).await?;
    task.wait_for_completion(client, None, Some(std::time::Duration::from_mins(1)))
        .await?;

    info!("Meilisearch index '{BIBLIOGRAPHY_INDEX}' configured.");
    Ok(())
}

/// Set up the place index.
async fn setup_place_index(client: &Client) -> Result<()> {
    info!("Configuring Meilisearch index '{PLACE_INDEX}'…");

    let task = client.create_index(PLACE_INDEX, Some("id")).await?;
    task.wait_for_completion(client, None, None).await?;

    let index = client.index(PLACE_INDEX);

    let settings = Settings::new()
        .with_searchable_attributes(["place_name", "place_name_full", "place_type"])
        .with_filterable_attributes([
            "id",
            "place_name",
            "place_name_full",
            "place_type",
            "parent_id",
            "municipality_nr",
        ])
        .with_sortable_attributes(["place_name"])
        .with_ranking_rules([
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness",
        ])
        .with_pagination(meilisearch_sdk::settings::PaginationSetting {
            max_total_hits: 10_000,
        });

    let task = index.set_settings(&settings).await?;
    task.wait_for_completion(client, None, Some(std::time::Duration::from_mins(1)))
        .await?;

    info!("Meilisearch index '{PLACE_INDEX}' configured.");
    Ok(())
}

/// Re-index all articles from PostgreSQL into Meilisearch if the schema
/// version has changed since the last indexing.
pub async fn reindex_if_needed(client: &Client, db: &PgPool) -> Result<()> {
    let stored: Option<(String,)> = sqlx::query_as(
        "SELECT value FROM sync_state WHERE dictionary = '_global' AND key = 'meili_schema_version'",
    )
    .fetch_optional(db)
    .await?;

    let current_version = stored
        .as_ref()
        .and_then(|r| r.0.parse::<i64>().ok())
        .unwrap_or(0);

    if current_version >= MEILI_SCHEMA_VERSION {
        info!("Meilisearch schema is up to date at version {current_version}.");
        return Ok(());
    }

    info!(
        "Meilisearch schema version changed from {current_version} to {MEILI_SCHEMA_VERSION}, re-indexing from PostgreSQL…"
    );

    reindex_all(client, db).await?;

    info!("Re-index complete. Schema version set to {MEILI_SCHEMA_VERSION}.");
    Ok(())
}

/// Re-index all Meilisearch data from PostgreSQL and save the current schema
/// version.
pub async fn reindex_all(client: &Client, db: &PgPool) -> Result<()> {
    reindex_articles(client, db).await?;
    reindex_bibliography(client, db).await?;
    reindex_places(client, db).await?;

    sqlx::query(
        "INSERT INTO sync_state (dictionary, key, value)
         VALUES ('_global', 'meili_schema_version', $1)
         ON CONFLICT (dictionary, key) DO UPDATE SET value = $1",
    )
    .bind(MEILI_SCHEMA_VERSION.to_string())
    .execute(db)
    .await?;

    Ok(())
}

/// Re-index all articles from PostgreSQL in Meilisearch.
async fn reindex_articles(client: &Client, db: &PgPool) -> Result<()> {
    let bib_rows: Vec<(i64, String, String, String, String)> =
        sqlx::query_as("SELECT id, code, author, title, year FROM bibliography")
            .fetch_all(db)
            .await?;

    let bib_map: std::collections::HashMap<i64, (String, String, String, String)> = bib_rows
        .into_iter()
        .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
        .collect();

    let place_rows: Vec<(i64, String, String, String)> =
        sqlx::query_as("SELECT id, place_name, place_name_full, place_type FROM places")
            .fetch_all(db)
            .await?;

    let place_map: std::collections::HashMap<i64, (String, String, String)> = place_rows
        .into_iter()
        .map(|(id, name, full_name, place_type)| (id, (name, full_name, place_type)))
        .collect();

    let article_place_rows: Vec<(String, i64, i64, String)> =
        sqlx::query_as("SELECT dictionary, article_id, place_id, context FROM article_place")
            .fetch_all(db)
            .await?;

    let mut article_place_map: crate::storage::ArticlePlaceMap = std::collections::HashMap::new();
    for (dict, article_id, place_id, context) in article_place_rows {
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

    for dict in ["bm", "nn", "no"] {
        let idx = client.index(index_name(dict));

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles WHERE dictionary = $1")
            .bind(dict)
            .fetch_one(db)
            .await?;

        if count.0 == 0 {
            info!("  [{dict}] No articles in DB, skipping.");
            continue;
        }

        let concepts = crate::storage::load_concepts(db, dict).await?;

        info!("  [{dict}] Re-indexing {} articles…", count.0);

        let mut stream = sqlx::query_as::<_, (i64, serde_json::Value)>(
            "SELECT id, data FROM articles WHERE dictionary = $1",
        )
        .bind(dict)
        .fetch(db);

        let mut batch: Vec<ArticleSearchDocument> = Vec::with_capacity(5000);
        let mut tasks = Vec::new();
        let dict_places = article_place_map.get(dict);
        let empty_place = (Vec::new(), Vec::new());

        while let Some(row) = stream.next().await {
            let (id, data) = row?;
            let bib = crate::extraction::build_article_bibliography(&data, &bib_map);
            let (dialect_ids, attestation_ids) =
                dict_places.and_then(|m| m.get(&id)).unwrap_or(&empty_place);
            let places = crate::extraction::build_article_place_data(
                dialect_ids,
                attestation_ids,
                &place_map,
            );
            batch.push(build_search_document(
                dict,
                id,
                &data,
                Some(bib),
                Some(places),
                &concepts,
            ));

            if batch.len() >= 5000 {
                tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
                batch.clear();
            }
        }

        if !batch.is_empty() {
            tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
        }

        let timeout = Some(std::time::Duration::from_mins(10));
        for task in tasks {
            task.wait_for_completion(client, None, timeout).await?;
        }

        info!("  [{dict}] Done.");
    }

    Ok(())
}

/// Re-index all bibliography entries from PostgreSQL in Meilisearch.
async fn reindex_bibliography(client: &Client, db: &PgPool) -> Result<()> {
    let idx = client.index(BIBLIOGRAPHY_INDEX);
    let rows: Vec<(i64, String, String, String, String)> =
        sqlx::query_as("SELECT id, code, author, title, year FROM bibliography")
            .fetch_all(db)
            .await?;

    if rows.is_empty() {
        info!("  [bibliography] No entries in DB, skipping.");
        return Ok(());
    }

    info!("  [bibliography] Re-indexing {} entries…", rows.len());

    let docs: Vec<BibliographySearchDocument> = rows
        .into_iter()
        .map(
            |(id, code, author, title, year)| BibliographySearchDocument {
                id,
                code,
                author,
                title,
                year,
            },
        )
        .collect();

    let mut tasks = Vec::new();
    for chunk in docs.chunks(5000) {
        tasks.push(idx.add_or_replace(chunk, Some("id")).await?);
    }

    let timeout = Some(std::time::Duration::from_mins(1));
    for task in tasks {
        task.wait_for_completion(client, None, timeout).await?;
    }

    info!("  [bibliography] Done.");
    Ok(())
}

/// Re-index all places from PostgreSQL in Meilisearch.
async fn reindex_places(client: &Client, db: &PgPool) -> Result<()> {
    let idx = client.index(PLACE_INDEX);
    let rows: Vec<PlaceSearchDocument> = sqlx::query_as::<_, (i64, String, String, String, Option<i64>, Option<String>)>(
        "SELECT id, place_name, place_name_full, place_type, parent_id, municipality_nr FROM places",
    )
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|(id, place_name, place_name_full, place_type, parent_id, municipality_nr)| {
        PlaceSearchDocument { id, place_name, place_name_full, place_type, parent_id, municipality_nr }
    })
    .collect();

    if rows.is_empty() {
        info!("  [places] No entries in DB, skipping.");
        return Ok(());
    }

    info!("  [places] Re-indexing {} entries…", rows.len());

    let mut tasks = Vec::new();
    for chunk in rows.chunks(5000) {
        tasks.push(idx.add_or_replace(chunk, Some("id")).await?);
    }

    let timeout = Some(std::time::Duration::from_mins(1));
    for task in tasks {
        task.wait_for_completion(client, None, timeout).await?;
    }

    info!("  [places] Done.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_index_name() {
        assert_eq!(index_name("bm"), "articles-bm");
        assert_eq!(index_name("nn"), "articles-nn");
        assert_eq!(index_name("no"), "articles-no");
    }

    #[test]
    fn test_build_search_document_empty_data() {
        let data = json!({});
        let concepts = std::collections::HashMap::new();
        let doc = build_search_document("no", 99, &data, None, None, &concepts);

        assert_eq!(doc.id, "no_99");
        assert!(doc.lemmas.is_empty());
        assert!(doc.suggest.is_empty());
        assert!(doc.inflections.is_empty());
        assert!(doc.paradigm_tags.is_empty());
        assert!(doc.inflection_tags.is_empty());
        assert_eq!(doc.etymology_text, "");
        assert!(!doc.has_split_inf);
        assert!(doc.dialect_places.is_empty());
        assert!(doc.older_source_codes.is_empty());
        assert!(doc.written_form_source_codes.is_empty());
        assert!(doc.attestation_source_codes.is_empty());
    }

    #[test]
    fn test_build_search_document_assembles_fields() {
        let data = json!({
            "lemmas": [
                {
                    "lemma": "test",
                    "paradigm_info": [
                        {
                            "tags": ["NOUN"],
                            "inflection": [
                                { "word_form": "test", "tags": ["Sing"] }
                            ]
                        }
                    ]
                }
            ],
            "suggest": ["test"]
        });
        let concepts = std::collections::HashMap::new();
        let doc = build_search_document("bm", 1, &data, None, None, &concepts);

        assert_eq!(doc.id, "bm_1");
        assert_eq!(doc.article_id, 1);
        assert_eq!(doc.dictionary, "bm");
        assert_eq!(doc.lemmas, vec!["test"]);
    }
}
