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
use futures::StreamExt;
use meilisearch_sdk::settings::Settings;
use meilisearch_sdk::{client::Client, features::ExperimentalFeatures};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::info;

/// Increment this when the indexes change.
pub const MEILI_SCHEMA_VERSION: i64 = 2;

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

/// Build a `BibliographySearchDocument` from a Clarino API response entry.
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

/// Extract bibliography IDs from article JSON and build bibliography metadata
/// from a lookup map.
pub fn build_article_bibliography(
    data: &serde_json::Value,
    bib_map: &std::collections::HashMap<i64, (String, String, String, String)>,
) -> ArticleBibliography {
    let mut all_bibl_ids: Vec<i64> = Vec::new();
    let mut older_source_ids: Vec<i64> = Vec::new();
    let mut written_form_source_ids: Vec<i64> = Vec::new();
    let mut attestation_source_ids: Vec<i64> = Vec::new();

    // Older sources
    if let Some(sources) = data
        .get("body")
        .and_then(|b| b.get("older_source"))
        .and_then(|v| v.as_array())
    {
        for source in sources {
            if let Some(id) = source.get("bibl_id").and_then(|v| v.as_i64()) {
                if !older_source_ids.contains(&id) {
                    older_source_ids.push(id);
                }
                if !all_bibl_ids.contains(&id) {
                    all_bibl_ids.push(id);
                }
            }
        }
    }

    // Written form sources.
    if let Some(wf_arr) = data
        .get("body")
        .and_then(|b| b.get("written_form"))
        .and_then(|v| v.as_array())
    {
        for wf in wf_arr {
            if let Some(forms) = wf.get("forms").and_then(|v| v.as_array()) {
                for form in forms {
                    if let Some(sources) = form.get("sources").and_then(|v| v.as_array()) {
                        for source in sources {
                            if let Some(id) = source.get("bibl_id").and_then(|v| v.as_i64()) {
                                if !written_form_source_ids.contains(&id) {
                                    written_form_source_ids.push(id);
                                }
                                if !all_bibl_ids.contains(&id) {
                                    all_bibl_ids.push(id);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Attestation sources.
    collect_attestation_bibl_ids(data, &mut attestation_source_ids, &mut all_bibl_ids);

    // Also collect any remaining bibl_ids we haven't categorized
    collect_bibl_ids(data, &mut all_bibl_ids);

    let older_source = build_category(&older_source_ids, bib_map);
    let written_form_source = build_category(&written_form_source_ids, bib_map);
    let attestation_source = build_category(&attestation_source_ids, bib_map);
    let all = build_category(&all_bibl_ids, bib_map);

    ArticleBibliography {
        older_source,
        written_form_source,
        attestation_source,
        all,
    }
}

fn build_category(
    ids: &[i64],
    bib_map: &std::collections::HashMap<i64, (String, String, String, String)>,
) -> BibliographyCategory {
    let mut codes = Vec::new();
    let mut authors = Vec::new();
    let mut titles = Vec::new();
    let mut years = Vec::new();

    for &id in ids {
        if let Some((code, author, title, year)) = bib_map.get(&id) {
            if !code.is_empty() && !codes.contains(code) {
                codes.push(code.clone());
            }
            if !author.is_empty() && !authors.contains(author) {
                authors.push(author.clone());
            }
            if !title.is_empty() && !titles.contains(title) {
                titles.push(title.clone());
            }
            if !year.is_empty() && !years.contains(year) {
                years.push(year.clone());
            }
        }
    }

    BibliographyCategory {
        codes,
        authors,
        titles,
        years,
    }
}

fn collect_attestation_bibl_ids(
    value: &serde_json::Value,
    attestation_ids: &mut Vec<i64>,
    all_ids: &mut Vec<i64>,
) {
    if let Some(defs) = value
        .get("body")
        .and_then(|b| b.get("definitions"))
        .and_then(|v| v.as_array())
    {
        collect_attestation_from_defs(defs, attestation_ids, all_ids);
    }
}

fn collect_attestation_from_defs(
    defs: &[serde_json::Value],
    attestation_ids: &mut Vec<i64>,
    all_ids: &mut Vec<i64>,
) {
    for def in defs {
        if let Some(elements) = def.get("elements").and_then(|v| v.as_array()) {
            for element in elements {
                if let Some(place_refs) = element.get("place_refs").and_then(|v| v.as_array()) {
                    for pr in place_refs {
                        let vis = pr.get("vis").and_then(|v| v.as_i64()).unwrap_or(0);
                        if vis == 1
                            && let Some(id) = pr.get("bibl_id").and_then(|v| v.as_i64())
                        {
                            if !attestation_ids.contains(&id) {
                                attestation_ids.push(id);
                            }
                            if !all_ids.contains(&id) {
                                all_ids.push(id);
                            }
                        }
                    }
                }
            }
        }
        if let Some(sub_defs) = def.get("sub_definitions").and_then(|v| v.as_array()) {
            collect_attestation_from_defs(sub_defs, attestation_ids, all_ids);
        }
    }
}

fn collect_bibl_ids(value: &serde_json::Value, ids: &mut Vec<i64>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(bibl_id) = map.get("bibl_id").and_then(|v| v.as_i64())
                && !ids.contains(&bibl_id)
            {
                ids.push(bibl_id);
            }
            for v in map.values() {
                collect_bibl_ids(v, ids);
            }
        }
        serde_json::Value::Array(arr) => {
            for v in arr {
                collect_bibl_ids(v, ids);
            }
        }
        _ => {}
    }
}

/// Index name for a given dictionary.
pub fn index_name(dict: &str) -> String {
    format!("articles-{dict}")
}

/// Bibliography metadata to embed in an article search document.
pub struct ArticleBibliography {
    /// Per-category data for older sources.
    pub older_source: BibliographyCategory,
    /// Per-category data for written form sources.
    pub written_form_source: BibliographyCategory,
    /// Per-category data for attestation sources.
    pub attestation_source: BibliographyCategory,
    /// All sources combined.
    pub all: BibliographyCategory,
}

/// Bibliography metadata for a single category.
pub struct BibliographyCategory {
    pub codes: Vec<String>,
    pub authors: Vec<String>,
    pub titles: Vec<String>,
    pub years: Vec<String>,
}

/// Build an `ArticleSearchDocument` from an article's JSON data.
pub fn build_search_document(
    dictionary: &str,
    article_id: i64,
    data: &serde_json::Value,
    bibliography: Option<&ArticleBibliography>,
) -> ArticleSearchDocument {
    let mut lemmas = Vec::new();
    let mut suggest = Vec::new();
    let mut inflections = Vec::new();
    let mut paradigm_tags = Vec::new();
    let mut inflection_tags = Vec::new();
    let mut etymology_parts = Vec::new();
    let mut etymology_languages = Vec::new();
    let mut pronunciation_parts = Vec::new();
    let mut dialect_form_parts: Vec<String> = Vec::new();
    let mut has_split_inf = false;
    let mut dialect_places = Vec::new();
    let mut definition_parts = Vec::new();
    let mut example_parts = Vec::new();
    let mut written_forms: Vec<String> = Vec::new();
    let mut sub_article_lemmas: Vec<String> = Vec::new();

    // Extract lemmas and inflection data
    if let Some(lemma_arr) = data.get("lemmas").and_then(|v| v.as_array()) {
        for lemma_obj in lemma_arr {
            if let Some(lemma_str) = lemma_obj.get("lemma").and_then(|v| v.as_str()) {
                lemmas.push(lemma_str.to_string());
            }
            if lemma_obj
                .get("split_inf")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                has_split_inf = true;
            }
            if let Some(paradigms) = lemma_obj.get("paradigm_info").and_then(|v| v.as_array()) {
                for paradigm in paradigms {
                    if let Some(tags) = paradigm.get("tags").and_then(|v| v.as_array()) {
                        for tag in tags {
                            if let Some(s) = tag.as_str()
                                && !paradigm_tags.contains(&s.to_string())
                            {
                                paradigm_tags.push(s.to_string());
                            }
                        }
                    }
                    if let Some(infl_arr) = paradigm.get("inflection").and_then(|v| v.as_array()) {
                        for infl in infl_arr {
                            if let Some(wf) = infl.get("word_form").and_then(|v| v.as_str())
                                && !inflections.contains(&wf.to_string())
                            {
                                inflections.push(wf.to_string());
                            }
                            if let Some(tags) = infl.get("tags").and_then(|v| v.as_array()) {
                                for tag in tags {
                                    if let Some(s) = tag.as_str()
                                        && !inflection_tags.contains(&s.to_string())
                                    {
                                        inflection_tags.push(s.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Extract suggest.
    if let Some(suggest_arr) = data.get("suggest").and_then(|v| v.as_array()) {
        for s in suggest_arr {
            if let Some(st) = s.as_str() {
                suggest.push(st.to_string());
            }
        }
    }

    // Extract etymology text and languages.
    let etym_source = data
        .get("body")
        .and_then(|b| b.get("etymology"))
        .and_then(|v| v.as_array());
    if let Some(etym_arr) = etym_source {
        for etym in etym_arr {
            if let Some(items) = etym.get("items").and_then(|v| v.as_array()) {
                for item in items {
                    if let Some(text) = item.get("text").and_then(|v| v.as_str()) {
                        etymology_parts.push(text.to_string());
                    }
                    if item.get("type_").and_then(|v| v.as_str()) == Some("language")
                        && let Some(id) = item.get("id").and_then(|v| v.as_str())
                        && !id.is_empty()
                        && !etymology_languages.contains(&id.to_string())
                    {
                        etymology_languages.push(id.to_string());
                    }
                }
            }
        }
    }

    // Extract pronunciation text.
    if let Some(pron_arr) = data
        .get("body")
        .and_then(|b| b.get("pronunciation"))
        .and_then(|v| v.as_array())
    {
        for pron in pron_arr {
            if let Some(content) = pron.get("content").and_then(|v| v.as_str())
                && !content.is_empty()
            {
                pronunciation_parts.push(content.to_string());
            }
        }
    }

    if let Some(dialect_arr) = data
        .get("body")
        .and_then(|b| b.get("dialect"))
        .and_then(|v| v.as_array())
    {
        for dialect in dialect_arr {
            if let Some(subcats) = dialect.get("subcats").and_then(|v| v.as_array()) {
                for subcat in subcats {
                    if let Some(forms) = subcat.get("forms").and_then(|v| v.as_array()) {
                        for form in forms {
                            // Extract dialect form text for search.
                            if let Some(form_str) = form.get("form").and_then(|v| v.as_str()) {
                                if !form_str.is_empty()
                                    && !dialect_form_parts.contains(&form_str.to_string())
                                {
                                    dialect_form_parts.push(form_str.to_string());
                                }
                            } else if let Some(form_obj) =
                                form.get("form").and_then(|v| v.as_object())
                                && let Some(content) =
                                    form_obj.get("content").and_then(|v| v.as_str())
                                && !content.is_empty()
                                && !dialect_form_parts.contains(&content.to_string())
                            {
                                dialect_form_parts.push(content.to_string());
                            }
                            if let Some(sources) = form.get("sources").and_then(|v| v.as_array()) {
                                for source in sources {
                                    let show =
                                        source.get("show").and_then(|v| v.as_i64()).unwrap_or(0);
                                    // Exclude hidden items, that is, where show isn't 1. Norsk Ordbok
                                    // doesn't show these in their UI, so it's safe to infer that they
                                    // shouldn't be used for search results either.
                                    if show == 1
                                        && let Some(name) =
                                            source.get("place_name").and_then(|v| v.as_str())
                                        && !dialect_places.contains(&name.to_string())
                                    {
                                        dialect_places.push(name.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(wf_arr) = data
        .get("body")
        .and_then(|b| b.get("written_form"))
        .and_then(|v| v.as_array())
    {
        for wf in wf_arr {
            if let Some(forms) = wf.get("forms").and_then(|v| v.as_array()) {
                for form in forms {
                    if let Some(wf_str) = form.get("written_form").and_then(|v| v.as_str())
                        && !wf_str.is_empty()
                        && !written_forms.contains(&wf_str.to_string())
                    {
                        written_forms.push(wf_str.to_string());
                    }
                }
            }
        }
    }

    fn extract_definition_content(
        defs: &[serde_json::Value],
        def_parts: &mut Vec<String>,
        ex_parts: &mut Vec<String>,
        sub_lemmas: &mut Vec<String>,
    ) {
        for def in defs {
            if let Some(elements) = def.get("elements").and_then(|v| v.as_array()) {
                for element in elements {
                    let type_ = element.get("type_").and_then(|v| v.as_str()).unwrap_or("");
                    match type_ {
                        "explanation" => {
                            if let Some(content) = element.get("content").and_then(|v| v.as_str())
                                && !content.is_empty()
                            {
                                def_parts.push(content.to_string());
                            }
                        }
                        "example" => {
                            if let Some(content) = element
                                .get("quote")
                                .and_then(|q| q.get("content"))
                                .and_then(|v| v.as_str())
                                && !content.is_empty()
                            {
                                ex_parts.push(content.to_string());
                            }
                        }
                        "sub_article" => {
                            if let Some(lemmas_arr) =
                                element.get("lemmas").and_then(|v| v.as_array())
                            {
                                for l in lemmas_arr {
                                    if let Some(s) = l.as_str()
                                        && !s.is_empty()
                                        && !sub_lemmas.contains(&s.to_string())
                                    {
                                        sub_lemmas.push(s.to_string());
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            if let Some(sub_defs) = def.get("sub_definitions").and_then(|v| v.as_array()) {
                extract_definition_content(sub_defs, def_parts, ex_parts, sub_lemmas);
            }
        }
    }

    if let Some(defs) = data
        .get("body")
        .and_then(|b| b.get("definitions"))
        .and_then(|v| v.as_array())
    {
        extract_definition_content(
            defs,
            &mut definition_parts,
            &mut example_parts,
            &mut sub_article_lemmas,
        );
    }

    ArticleSearchDocument {
        id: format!("{dictionary}_{article_id}"),
        article_id,
        dictionary: dictionary.to_string(),
        lemmas,
        suggest,
        inflections,
        etymology_text: etymology_parts.join(" "),
        pronunciation_text: pronunciation_parts.join(" "),
        dialect_forms: dialect_form_parts,
        paradigm_tags,
        inflection_tags,
        has_split_inf,
        dialect_places,
        older_source_codes: bibliography
            .as_ref()
            .map(|b| b.older_source.codes.clone())
            .unwrap_or_default(),
        older_source_authors: bibliography
            .as_ref()
            .map(|b| b.older_source.authors.clone())
            .unwrap_or_default(),
        older_source_titles: bibliography
            .as_ref()
            .map(|b| b.older_source.titles.clone())
            .unwrap_or_default(),
        older_source_years: bibliography
            .as_ref()
            .map(|b| b.older_source.years.clone())
            .unwrap_or_default(),
        written_form_source_codes: bibliography
            .as_ref()
            .map(|b| b.written_form_source.codes.clone())
            .unwrap_or_default(),
        written_form_source_authors: bibliography
            .as_ref()
            .map(|b| b.written_form_source.authors.clone())
            .unwrap_or_default(),
        written_form_source_titles: bibliography
            .as_ref()
            .map(|b| b.written_form_source.titles.clone())
            .unwrap_or_default(),
        written_form_source_years: bibliography
            .as_ref()
            .map(|b| b.written_form_source.years.clone())
            .unwrap_or_default(),
        attestation_source_codes: bibliography
            .as_ref()
            .map(|b| b.attestation_source.codes.clone())
            .unwrap_or_default(),
        attestation_source_authors: bibliography
            .as_ref()
            .map(|b| b.attestation_source.authors.clone())
            .unwrap_or_default(),
        attestation_source_titles: bibliography
            .as_ref()
            .map(|b| b.attestation_source.titles.clone())
            .unwrap_or_default(),
        attestation_source_years: bibliography
            .as_ref()
            .map(|b| b.attestation_source.years.clone())
            .unwrap_or_default(),
        bibliography_codes: bibliography
            .as_ref()
            .map(|b| b.all.codes.clone())
            .unwrap_or_default(),
        bibliography_authors: bibliography
            .as_ref()
            .map(|b| b.all.authors.clone())
            .unwrap_or_default(),
        bibliography_titles: bibliography
            .as_ref()
            .map(|b| b.all.titles.clone())
            .unwrap_or_default(),
        bibliography_years: bibliography
            .as_ref()
            .map(|b| b.all.years.clone())
            .unwrap_or_default(),
        etymology_languages,
        definition_text: definition_parts.join(" "),
        example_text: example_parts.join(" "),
        written_forms,
        sub_article_lemmas,
    }
}

/// Ensure all dictionary indexes exist with correct settings.
pub async fn setup_indexes(client: &Client) -> Result<()> {
    let mut features = ExperimentalFeatures::new(client);

    features.set_contains_filter(true);
    features.update().await?;

    for dict in ["bm", "nn", "no"] {
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
                max_total_hits: 1000,
            });

        let task = index.set_settings(&settings).await?;
        task.wait_for_completion(client, None, Some(std::time::Duration::from_secs(600)))
            .await?;

        info!("Meilisearch index '{idx}' configured.");
    }

    // Configure bibliography index.
    {
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
                max_total_hits: 1000,
            });

        let task = index.set_settings(&settings).await?;
        task.wait_for_completion(client, None, Some(std::time::Duration::from_secs(60)))
            .await?;

        info!("Meilisearch index '{BIBLIOGRAPHY_INDEX}' configured.");
    }

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

    // Load all bibliography entries for enrichment during reindex.
    let bib_rows: Vec<(i64, String, String, String, String)> =
        sqlx::query_as("SELECT id, code, author, title, year FROM bibliography")
            .fetch_all(db)
            .await?;

    let bib_map: std::collections::HashMap<i64, (String, String, String, String)> = bib_rows
        .into_iter()
        .map(|(id, code, author, title, year)| (id, (code, author, title, year)))
        .collect();

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

        info!("  [{dict}] Re-indexing {} articles…", count.0);

        let mut stream = sqlx::query_as::<_, (i64, serde_json::Value)>(
            "SELECT id, data FROM articles WHERE dictionary = $1",
        )
        .bind(dict)
        .fetch(db);

        let mut batch: Vec<ArticleSearchDocument> = Vec::with_capacity(5000);
        let mut tasks = Vec::new();

        while let Some(row) = stream.next().await {
            let (id, data) = row?;
            let bib = build_article_bibliography(&data, &bib_map);
            batch.push(build_search_document(dict, id, &data, Some(&bib)));

            if batch.len() >= 5000 {
                tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
                batch.clear();
            }
        }

        if !batch.is_empty() {
            tasks.push(idx.add_or_replace(&batch, Some("id")).await?);
        }

        let timeout = Some(std::time::Duration::from_secs(600));
        for task in tasks {
            task.wait_for_completion(client, None, timeout).await?;
        }

        info!("  [{dict}] Done.");
    }

    // Re-index bibliography entries.
    {
        let idx = client.index(BIBLIOGRAPHY_INDEX);
        let rows: Vec<(i64, String, String, String, String)> =
            sqlx::query_as("SELECT id, code, author, title, year FROM bibliography")
                .fetch_all(db)
                .await?;

        if rows.is_empty() {
            info!("  [bibliography] No entries in DB, skipping.");
        } else {
            info!("  [bibliography] Re-indexing {} entries…", rows.len());

            let mut tasks = Vec::new();
            for chunk in rows.chunks(5000) {
                let docs: Vec<BibliographySearchDocument> = chunk
                    .iter()
                    .map(
                        |(id, code, author, title, year)| BibliographySearchDocument {
                            id: *id,
                            code: code.clone(),
                            author: author.clone(),
                            title: title.clone(),
                            year: year.clone(),
                        },
                    )
                    .collect();

                tasks.push(idx.add_or_replace(&docs, Some("id")).await?);
            }

            let timeout = Some(std::time::Duration::from_secs(60));
            for task in tasks {
                task.wait_for_completion(client, None, timeout).await?;
            }

            info!("  [bibliography] Done.");
        }
    }

    sqlx::query(
        "INSERT INTO sync_state (dictionary, key, value)
         VALUES ('_global', 'meili_schema_version', $1)
         ON CONFLICT (dictionary, key) DO UPDATE SET value = $1",
    )
    .bind(MEILI_SCHEMA_VERSION.to_string())
    .execute(db)
    .await?;

    info!("Re-index complete. Schema version set to {MEILI_SCHEMA_VERSION}.");
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
    fn test_build_search_document_noun_with_inflections() {
        let data = json!({
            "lemmas": [
                {
                    "hgno": 0,
                    "id": 90001,
                    "lemma": "fjordsting",
                    "split_inf": false,
                    "inflection_class": "m1, f1",
                    "paradigm_info": [
                        {
                            "from": "1996-01-01",
                            "to": null,
                            "inflection_group": "NOUN_regular",
                            "paradigm_id": 564,
                            "standardisation": "STANDARD",
                            "tags": ["NOUN", "Masc"],
                            "inflection": [
                                { "word_form": "fjordsting", "tags": ["Sing", "Ind"] },
                                { "word_form": "fjordstingen", "tags": ["Sing", "Def"] },
                                { "word_form": "fjordstinger", "tags": ["Plur", "Ind"] },
                                { "word_form": "fjordstingene", "tags": ["Plur", "Def"] },
                            ]
                        },
                        {
                            "from": "1996-01-01",
                            "to": null,
                            "inflection_group": "NOUN_regular",
                            "paradigm_id": 760,
                            "standardisation": "STANDARD",
                            "tags": ["NOUN", "Fem"],
                            "inflection": [
                                { "word_form": "fjordsting", "tags": ["Sing", "Ind"] },
                                { "word_form": "fjordstinga", "tags": ["Sing", "Def"] },
                                { "word_form": "fjordstinger", "tags": ["Plur", "Ind"] },
                                { "word_form": "fjordstingene", "tags": ["Plur", "Def"] },
                            ]
                        }
                    ]
                }
            ],
            "suggest": ["fjordsting"],
            "body": {
                "pronunciation": [],
                "etymology": [
                    {
                        "type_": "etymology_language",
                        "content": "av norrønt $",
                        "items": [
                            { "type_": "usage", "text": "fjǫrðr" },
                            { "type_": "usage", "text": "þing" },
                        ]
                    }
                ],
                "definitions": []
            }
        });

        let doc = build_search_document("bm", 55001, &data, None);

        assert_eq!(doc.id, "bm_55001");
        assert_eq!(doc.article_id, 55001);
        assert_eq!(doc.dictionary, "bm");
        assert_eq!(doc.lemmas, vec!["fjordsting"]);
        assert_eq!(doc.suggest, vec!["fjordsting"]);
        assert_eq!(
            doc.inflections,
            vec![
                "fjordsting",
                "fjordstingen",
                "fjordstinger",
                "fjordstingene",
                "fjordstinga",
            ]
        );
        assert_eq!(doc.paradigm_tags, vec!["NOUN", "Masc", "Fem"]);
        assert_eq!(doc.inflection_tags, vec!["Sing", "Ind", "Def", "Plur"]);
        assert_eq!(doc.etymology_text, "fjǫrðr þing");
        assert!(!doc.has_split_inf);
        assert!(doc.dialect_places.is_empty());
        assert!(doc.older_source_codes.is_empty());
        assert!(doc.written_form_source_codes.is_empty());
        assert!(doc.attestation_source_codes.is_empty());
    }

    #[test]
    fn test_build_search_document_abbreviation() {
        let data = json!({
            "lemmas": [
                {
                    "hgno": 1,
                    "id": 90002,
                    "lemma": "F",
                    "paradigm_info": [
                        {
                            "from": "1996-01-01",
                            "to": null,
                            "inflection_group": "ABBR",
                            "paradigm_id": 40,
                            "standardisation": "STANDARD",
                            "tags": ["ABBR"],
                            "inflection": [
                                { "word_form": "F", "tags": [] }
                            ]
                        }
                    ]
                },
                {
                    "hgno": 1,
                    "id": 90003,
                    "lemma": "f",
                    "paradigm_info": [
                        {
                            "from": "1996-01-01",
                            "to": null,
                            "inflection_group": "ABBR",
                            "paradigm_id": 40,
                            "standardisation": "STANDARD",
                            "tags": ["ABBR"],
                            "inflection": [
                                { "word_form": "f", "tags": [] }
                            ]
                        }
                    ]
                }
            ],
            "suggest": ["F", "f"]
        });

        let doc = build_search_document("bm", 90000, &data, None);

        assert_eq!(doc.lemmas, vec!["F", "f"]);
        assert_eq!(doc.suggest, vec!["F", "f"]);
        assert_eq!(doc.inflections, vec!["F", "f"]);
        assert_eq!(doc.paradigm_tags, vec!["ABBR"]);
        assert!(doc.inflection_tags.is_empty());
        assert_eq!(doc.etymology_text, "");
        assert!(!doc.has_split_inf);
    }

    #[test]
    fn test_build_search_document_verb_split_inf() {
        let data = json!({
            "lemmas": [
                {
                    "hgno": 0,
                    "id": 90004,
                    "lemma": "velja",
                    "split_inf": true,
                    "paradigm_info": [
                        {
                            "from": "2012-01-01",
                            "to": null,
                            "inflection_group": "VERB_regular",
                            "paradigm_id": 1100,
                            "standardisation": "STANDARD",
                            "tags": ["VERB"],
                            "inflection": [
                                { "word_form": "vel", "tags": ["Pres"] },
                                { "word_form": "valde", "tags": ["Past"] },
                            ]
                        }
                    ]
                }
            ],
            "suggest": ["velja"]
        });

        let doc = build_search_document("nn", 70001, &data, None);

        assert!(doc.has_split_inf);
        assert_eq!(doc.lemmas, vec!["velja"]);
        assert_eq!(doc.inflections, vec!["vel", "valde"]);
        assert_eq!(doc.paradigm_tags, vec!["VERB"]);
        assert_eq!(doc.inflection_tags, vec!["Pres", "Past"]);
    }

    #[test]
    fn test_build_search_document_empty_data() {
        let data = json!({});
        let doc = build_search_document("no", 99, &data, None);

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
    fn test_build_search_document_no_with_dialect_and_sources() {
        let data = json!({
            "lemmas": [
                {
                    "id": 90010,
                    "lemma": "trollskog",
                    "paradigm_info": [
                        {
                            "tags": ["NOUN", "Masc"],
                            "inflection": [
                                { "word_form": "trollskog", "tags": ["Sing", "Ind"] }
                            ]
                        }
                    ]
                }
            ],
            "suggest": [],
            "body": {
                "pronunciation": [{ "items": [{ "text": "trållskåg", "type_": "text" }] }],
                "etymology": [
                    {
                        "type_": "etymology_lang",
                        "items": [{ "text": "trollskógr", "type_": "usage" }]
                    }
                ],
                "dialect": [
                    {
                        "subcats": [
                            {
                                "forms": [
                                    {
                                        "form": "trållskåg",
                                        "sources": [
                                            { "show": 1, "place_name": "Nordfjell", "place_id": 2 },
                                            { "show": 0, "place_name": "Sørdal", "place_id": 41 },
                                            { "show": 1, "place_name": "Vestmark", "place_id": 85 }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ],
                "older_source": [
                    { "code": "FiktA", "bibl_id": 100 },
                    { "code": "FiktB", "bibl_id": 200 }
                ],
                "written_form": [
                    {
                        "forms": [
                            {
                                "written_form": "trollskog",
                                "sources": [
                                    { "code": "E.DiktAS", "bibl_id": 2027 },
                                    { "code": "SagaOH", "bibl_id": 10482 }
                                ]
                            }
                        ]
                    }
                ],
                "definitions": [
                    {
                        "elements": [
                            {
                                "place_refs": [
                                    { "vis": 1, "code": "X", "place_name": "Nordfjell" },
                                    { "vis": 0, "code": "Y", "place_name": "Sørdal" }
                                ]
                            }
                        ],
                        "literature_refs": [
                            { "code": "FiktSag" },
                            { "code": "TrollRef" }
                        ]
                    }
                ]
            }
        });

        let mut bib_map = std::collections::HashMap::new();
        bib_map.insert(
            100,
            (
                "FiktA".to_string(),
                "Author A".to_string(),
                "Title A".to_string(),
                "2000".to_string(),
            ),
        );
        bib_map.insert(
            200,
            (
                "FiktB".to_string(),
                "Author B".to_string(),
                "Title B".to_string(),
                "2001".to_string(),
            ),
        );
        bib_map.insert(
            2027,
            (
                "E.DiktAS".to_string(),
                "Dikt Author".to_string(),
                "Dikt Title".to_string(),
                "1990".to_string(),
            ),
        );
        bib_map.insert(
            10482,
            (
                "SagaOH".to_string(),
                "Saga Author".to_string(),
                "Saga Title".to_string(),
                "1850".to_string(),
            ),
        );

        let bib = build_article_bibliography(&data, &bib_map);
        let doc = build_search_document("no", 99001, &data, Some(&bib));

        assert_eq!(doc.dialect_places, vec!["Nordfjell", "Vestmark"]);
        assert_eq!(doc.older_source_codes, vec!["FiktA", "FiktB"]);
        assert_eq!(doc.written_form_source_codes, vec!["E.DiktAS", "SagaOH"]);
        assert!(doc.attestation_source_codes.is_empty());
    }
}
