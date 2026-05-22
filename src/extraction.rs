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

use regex::Regex;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::LazyLock;

use crate::state::UibDictionary;

/// Result of an analysis of an article's JSON.
#[derive(Debug, Clone)]
pub struct ArticleAnalysis {
    pub primary_lemma: String,
    pub bibl_ids: HashSet<i64>,
    pub dialect_place_ids: HashSet<i64>,
    pub attestation_place_ids: HashSet<i64>,
    pub inline_refs: Vec<InlineRef>,
    pub related_article_ids: Vec<i64>,
}

/// A parsed inline bibliography reference found in text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineRef {
    pub quote_content: String,
    pub offset_start: usize,
    pub offset_end: usize,
    pub code: String,
    pub spec: Option<String>,
}

/// Matches inline bibliography references in text.
static INLINE_REF_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:\S| )\(([^)]+)\)").unwrap());

/// Metadata from the UiB article list.
#[derive(Debug)]
pub struct ArticleListEntry {
    pub article_id: i64,
    pub primary_lemma: String,
    pub revision: Option<i64>,
    pub updated_at: String,
}

/// Parse a raw metadata array from the UiB article list.
#[must_use]
pub fn parse_article_list_entry(raw: &Value) -> Option<ArticleListEntry> {
    let arr = raw.as_array()?;
    let article_id = arr.first().and_then(serde_json::Value::as_i64)?;
    let lemma = arr
        .get(1)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let revision = Some(arr.get(2).and_then(serde_json::Value::as_i64).unwrap_or(0));
    let updated_at = arr
        .get(3)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    Some(ArticleListEntry {
        article_id,
        primary_lemma: lemma,
        revision,
        updated_at,
    })
}

/// Analyze an article's JSON.
#[must_use]
pub fn analyze_article(dict: UibDictionary, article_data: &Value) -> ArticleAnalysis {
    let primary_lemma = find_first_lemma(article_data);
    let mut bibl_ids = HashSet::new();
    let mut related_article_ids = Vec::new();

    collect_bibl_ids(article_data, &mut bibl_ids);
    find_related_article_ids(article_data, &mut related_article_ids);

    let dialect_place_ids = extract_dialect_place_ids(article_data);
    let attestation_place_ids = extract_attestation_place_ids(article_data);

    let inline_refs = if dict == UibDictionary::NorskOrdbok {
        extract_inline_refs(article_data)
    } else {
        Vec::new()
    };

    ArticleAnalysis {
        primary_lemma,
        bibl_ids,
        dialect_place_ids,
        attestation_place_ids,
        inline_refs,
        related_article_ids,
    }
}

/// Get the first lemma from article JSON.
#[must_use]
pub fn find_first_lemma(article_json: &Value) -> String {
    article_json
        .get("lemmas")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|lemma| lemma.get("lemma"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

/// Recursively collect all bibl_id values from JSON.
pub fn collect_bibl_ids<S: std::hash::BuildHasher>(value: &Value, ids: &mut HashSet<i64, S>) {
    match value {
        Value::Object(map) => {
            if let Some(bibl_id) = map.get("bibl_id").and_then(serde_json::Value::as_i64) {
                ids.insert(bibl_id);
            }
            for v in map.values() {
                collect_bibl_ids(v, ids);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_bibl_ids(v, ids);
            }
        }
        _ => {}
    }
}

/// Recursively scan JSON for article_ref or sub_article entries.
pub fn find_related_article_ids(value: &Value, ids: &mut Vec<i64>) {
    match value {
        Value::Object(map) => {
            let type_field = map.get("type_").and_then(|v| v.as_str());
            if let Some(t) = type_field
                && (t == "article_ref" || t == "sub_article")
                && let Some(aid) = map.get("article_id").and_then(serde_json::Value::as_i64)
                && !ids.contains(&aid)
            {
                ids.push(aid);
            }
            for v in map.values() {
                find_related_article_ids(v, ids);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                find_related_article_ids(v, ids);
            }
        }
        _ => {}
    }
}

/// Extract place IDs from dialect sources.
#[must_use]
pub fn extract_dialect_place_ids(article: &Value) -> HashSet<i64> {
    article
        .pointer("/body/dialect")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .flat_map(|d| {
            d.get("subcats")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
        })
        .flat_map(|sc| {
            sc.get("forms")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
        })
        .flat_map(|f| {
            f.get("sources")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
        })
        .filter_map(|s| s.get("place_id").and_then(serde_json::Value::as_i64))
        .collect()
}

/// Extract place IDs from attestation place_refs.
#[must_use]
pub fn extract_attestation_place_ids(article: &Value) -> HashSet<i64> {
    article
        .pointer("/body/definitions")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .flat_map(|def| {
            def.get("elements")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
        })
        .flat_map(|elem| {
            elem.get("place_refs")
                .and_then(|v| v.as_array())
                .into_iter()
                .flatten()
        })
        .filter_map(|pr| pr.get("place")?.get("place_id")?.as_i64())
        .collect()
}

/// Extract children place IDs from a place API response.
#[must_use]
pub fn extract_child_place_ids(entry: &Value) -> HashSet<i64> {
    entry
        .get("child_places")
        .and_then(|v| v.as_array())
        .into_iter()
        .flatten()
        .filter_map(|child| child.get("place_id").and_then(serde_json::Value::as_i64))
        .collect()
}

/// Extract inline references from all text in an article.
#[must_use]
pub fn extract_inline_refs(article: &Value) -> Vec<InlineRef> {
    let mut refs = Vec::new();
    if let Some(body) = article.get("body") {
        collect_inline_refs_recursive(body, &mut refs);
    }
    refs
}

/// Recursively scan JSON for text fields to extract inline references from.
fn collect_inline_refs_recursive(value: &Value, refs: &mut Vec<InlineRef>) {
    match value {
        Value::Object(map) => {
            let type_ = map.get("type_").and_then(|v| v.as_str());
            if type_ == Some("example") {
                if let Some(content) = map
                    .get("quote")
                    .and_then(|q| q.get("content"))
                    .and_then(|c| c.as_str())
                {
                    extract_refs_from_quote(content, refs);
                }
            } else if type_ == Some("explanation")
                && let Some(content) = map.get("content").and_then(|c| c.as_str())
            {
                extract_refs_from_quote(content, refs);
            }
            for v in map.values() {
                collect_inline_refs_recursive(v, refs);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_inline_refs_recursive(v, refs);
            }
        }
        _ => {}
    }
}

/// Parse inline references from a single content string.
pub fn extract_refs_from_quote(content: &str, refs: &mut Vec<InlineRef>) {
    for cap in INLINE_REF_REGEX.captures_iter(content) {
        let full_match = cap.get(0).unwrap();
        let inner = &cap[1];

        let paren_start = full_match.start() + full_match.as_str().find('(').unwrap();
        let paren_end = full_match.end();

        for segment in inner.split(';') {
            let segment = segment.trim();
            if segment.is_empty() {
                continue;
            }

            let (code, spec) = segment.find(' ').map_or((segment, None), |pos| {
                let code = &segment[..pos];
                let spec = segment[pos + 1..].trim();
                (code, if spec.is_empty() { None } else { Some(spec) })
            });

            let first_char = code.chars().next().unwrap_or(' ');
            if !first_char.is_uppercase() {
                continue;
            }

            refs.push(InlineRef {
                quote_content: content.to_string(),
                offset_start: paren_start,
                offset_end: paren_end,
                code: code.to_string(),
                spec: spec.map(std::string::ToString::to_string),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_article_list_entry() {
        let raw = json!([58083, "fjordsting", 2, "2026-04-30 14:55:59.171553"]);
        let meta = parse_article_list_entry(&raw).unwrap();
        assert_eq!(meta.article_id, 58083);
        assert_eq!(meta.primary_lemma, "fjordsting");
        assert_eq!(meta.revision, Some(2));
        assert_eq!(meta.updated_at, "2026-04-30 14:55:59.171553");
    }

    #[test]
    fn test_parse_article_list_entry_id_only() {
        let raw = json!([12345]);
        let meta = parse_article_list_entry(&raw).unwrap();
        assert_eq!(meta.article_id, 12345);
        assert_eq!(meta.primary_lemma, "");
        assert_eq!(meta.revision, Some(0));
        assert_eq!(meta.updated_at, "");
    }

    #[test]
    fn test_parse_article_list_entry_not_array() {
        let raw = json!({ "article_id": 1 });
        assert!(parse_article_list_entry(&raw).is_none());
    }

    #[test]
    fn test_parse_article_list_entry_empty_array() {
        let raw = json!([]);
        assert!(parse_article_list_entry(&raw).is_none());
    }

    #[test]
    fn test_find_first_lemma() {
        let data = json!({
            "lemmas": [
                { "hgno": 0, "id": 90001, "lemma": "strandskog" },
                { "hgno": 0, "id": 90002, "lemma": "strandskogen" }
            ]
        });
        assert_eq!(find_first_lemma(&data), "strandskog");
    }

    #[test]
    fn test_find_first_lemma_missing() {
        assert_eq!(find_first_lemma(&json!({})), "");
        assert_eq!(find_first_lemma(&json!({ "lemmas": [] })), "");
    }

    #[test]
    fn test_find_related_article_ids_in_definitions() {
        let data = json!({
            "body": {
                "definitions": [{
                    "type_": "definition",
                    "elements": [{
                        "type_": "explanation",
                        "content": "eit slag $",
                        "items": [{
                            "type_": "article_ref",
                            "article_id": 2002,
                            "lemmas": [{ "type_": "lemma", "hgno": 0, "id": 2529, "lemma": "skog" }],
                            "definition_id": null
                        }]
                    }],
                    "id": 2
                }]
            }
        });
        let mut ids = Vec::new();
        find_related_article_ids(&data, &mut ids);
        assert_eq!(ids, vec![2002]);
    }

    #[test]
    fn test_find_related_article_ids_sub_article() {
        let data = json!({
            "body": {
                "definitions": [{
                    "type_": "definition",
                    "elements": [{
                        "type_": "sub_article",
                        "article_id": 5001,
                        "lemmas": []
                    }],
                    "id": 3
                }]
            }
        });
        let mut ids = Vec::new();
        find_related_article_ids(&data, &mut ids);
        assert_eq!(ids, vec![5001]);
    }

    #[test]
    fn test_find_related_article_ids_deduplicates() {
        let data = json!({
            "items": [
                { "type_": "article_ref", "article_id": 3000 },
                { "type_": "article_ref", "article_id": 3000 },
            ]
        });
        let mut ids = Vec::new();
        find_related_article_ids(&data, &mut ids);
        assert_eq!(ids, vec![3000]);
    }

    #[test]
    fn test_find_related_article_ids_none() {
        let data = json!({
            "body": {
                "pronunciation": [],
                "definitions": [{
                    "type_": "definition",
                    "elements": [
                        { "type_": "explanation", "content": "noko", "items": [] }
                    ],
                    "id": 1
                }]
            }
        });
        let mut ids = Vec::new();
        find_related_article_ids(&data, &mut ids);
        assert!(ids.is_empty());
    }

    #[test]
    fn test_extract_refs_simple() {
        let mut refs = Vec::new();
        extract_refs_from_quote("dei dreiv med fjordfiske(Fj.Skr III,42)", &mut refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Fj.Skr");
        assert_eq!(refs[0].spec.as_deref(), Some("III,42"));
        assert_eq!(refs[0].offset_start, 24);
        assert_eq!(
            refs[0].offset_end,
            "dei dreiv med fjordfiske(Fj.Skr III,42)".len()
        );
    }

    #[test]
    fn test_extract_refs_no_spec() {
        let mut refs = Vec::new();
        extract_refs_from_quote("ho sette seg ned og kvilde(HaBrev)", &mut refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "HaBrev");
        assert_eq!(refs[0].spec, None);
    }

    #[test]
    fn test_extract_refs_with_trailing_text() {
        let mut refs = Vec::new();
        extract_refs_from_quote("han tok ljaaen sin(Fj.Skr II,87)og gjekk ut", &mut refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Fj.Skr");
        assert_eq!(refs[0].spec.as_deref(), Some("II,87"));
    }

    #[test]
    fn test_extract_refs_semicolon_separated() {
        let mut refs = Vec::new();
        extract_refs_from_quote("dei slo graset tidleg(ordt, Vik; DalOrdt 15)", &mut refs);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "DalOrdt");
        assert_eq!(refs[0].spec.as_deref(), Some("15"));
    }

    #[test]
    fn test_extract_refs_skips_editorial_parens_with_space() {
        let mut refs = Vec::new();
        extract_refs_from_quote(
            "garden (den gamle) var stor, og dei (folket) trivdest godt der(Heim.S 1901)",
            &mut refs,
        );
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Heim.S");
        assert_eq!(refs[0].spec.as_deref(), Some("1901"));
    }

    #[test]
    fn test_extract_refs_no_refs() {
        let mut refs = Vec::new();
        extract_refs_from_quote("det var stilt i fjorden den kvelden", &mut refs);
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_refs_skips_lowercase_code() {
        let mut refs = Vec::new();
        extract_refs_from_quote("dei budde langt inne i dalen(ordt, Vik)", &mut refs);
        assert!(refs.is_empty());
    }

    #[test]
    fn test_extract_inline_refs_from_article() {
        let article = json!({
            "body": {
                "definitions": [{
                    "type_": "definition",
                    "id": 1,
                    "elements": [{
                        "type_": "example",
                        "quote": { "content": "dei rodde ut kvar morgon(Fj.Skr 104)", "items": [] },
                        "attest": [],
                        "explanation": { "content": "", "items": [] }
                    }, {
                        "type_": "example",
                        "quote": { "content": "vanleg tekst utan kjelde", "items": [] },
                        "attest": [],
                        "explanation": { "content": "", "items": [] }
                    }]
                }]
            }
        });
        let refs = extract_inline_refs(&article);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].code, "Fj.Skr");
        assert_eq!(refs[0].spec.as_deref(), Some("104"));
    }
}
