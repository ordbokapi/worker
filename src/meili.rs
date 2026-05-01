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
use meilisearch_sdk::client::Client;
use meilisearch_sdk::settings::Settings;
use serde::{Deserialize, Serialize};
use tracing::info;

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
    /// Paradigm tags, e.g. NOUN, VERB.
    pub paradigm_tags: Vec<String>,
    /// Inflection tags, e.g. Sing, Def.
    pub inflection_tags: Vec<String>,
    /// Whether the article has a split infinitive.
    pub has_split_inf: bool,
}

/// Index name for a given dictionary.
pub fn index_name(dict: &str) -> String {
    format!("articles-{dict}")
}

/// Build an `ArticleSearchDocument` from an article's JSON data.
pub fn build_search_document(
    dictionary: &str,
    article_id: i64,
    data: &serde_json::Value,
) -> ArticleSearchDocument {
    let mut lemmas = Vec::new();
    let mut suggest = Vec::new();
    let mut inflections = Vec::new();
    let mut paradigm_tags = Vec::new();
    let mut inflection_tags = Vec::new();
    let mut etymology_parts = Vec::new();
    let mut has_split_inf = false;

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

    // Extract etymology text.
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
                }
            }
        }
    }

    ArticleSearchDocument {
        id: format!("{dictionary}_{article_id}"),
        article_id,
        dictionary: dictionary.to_string(),
        lemmas,
        suggest,
        inflections,
        etymology_text: etymology_parts.join(" "),
        paradigm_tags,
        inflection_tags,
        has_split_inf,
    }
}

/// Ensure all dictionary indexes exist with correct settings.
pub async fn setup_indexes(client: &Client) -> Result<()> {
    for dict in ["bm", "nn", "no"] {
        let idx = index_name(dict);
        info!("Configuring Meilisearch index '{idx}'…");

        let task = client.create_index(&idx, Some("id")).await?;
        task.wait_for_completion(client, None, None).await?;

        let index = client.index(&idx);

        let settings = Settings::new()
            .with_searchable_attributes(["lemmas", "suggest", "inflections", "etymology_text"])
            .with_filterable_attributes([
                "paradigm_tags",
                "inflection_tags",
                "has_split_inf",
                "lemmas",
                "inflections",
                "suggest",
                "dictionary",
                "article_id",
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
        task.wait_for_completion(client, None, None).await?;

        info!("Meilisearch index '{idx}' configured.");
    }

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

        let doc = build_search_document("bm", 55001, &data);

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

        let doc = build_search_document("bm", 90000, &data);

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

        let doc = build_search_document("nn", 70001, &data);

        assert!(doc.has_split_inf);
        assert_eq!(doc.lemmas, vec!["velja"]);
        assert_eq!(doc.inflections, vec!["vel", "valde"]);
        assert_eq!(doc.paradigm_tags, vec!["VERB"]);
        assert_eq!(doc.inflection_tags, vec!["Pres", "Past"]);
    }

    #[test]
    fn test_build_search_document_empty_data() {
        let data = json!({});
        let doc = build_search_document("no", 99, &data);

        assert_eq!(doc.id, "no_99");
        assert!(doc.lemmas.is_empty());
        assert!(doc.suggest.is_empty());
        assert!(doc.inflections.is_empty());
        assert!(doc.paradigm_tags.is_empty());
        assert!(doc.inflection_tags.is_empty());
        assert_eq!(doc.etymology_text, "");
        assert!(!doc.has_split_inf);
    }
}
