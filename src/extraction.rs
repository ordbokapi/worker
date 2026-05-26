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

use indexmap::IndexSet;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use crate::state::UibDictionary;

/// Iterate over a JSON array field.
pub fn json_array<'a>(value: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    value
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

/// Iterate over a JSON array at a pointer path.
pub fn json_array_at<'a>(value: &'a Value, pointer: &str) -> impl Iterator<Item = &'a Value> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
}

/// Recursively visit all JSON objects in a value tree.
fn walk_json_objects<F>(value: &Value, f: &mut F)
where
    F: FnMut(&serde_json::Map<String, Value>),
{
    match value {
        Value::Object(map) => {
            f(map);
            for v in map.values() {
                walk_json_objects(v, f);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                walk_json_objects(v, f);
            }
        }
        _ => {}
    }
}

/// Metadata from the UiB article list.
#[derive(Debug)]
pub struct ArticleListEntry {
    pub article_id: i64,
    pub primary_lemma: String,
    pub revision: Option<i64>,
    pub updated_at: String,
}

/// Extracted lemma and inflection data from an article.
pub struct LemmaData {
    pub lemmas: Vec<String>,
    pub suggest: Vec<String>,
    pub inflections: Vec<String>,
    pub paradigm_tags: Vec<String>,
    pub inflection_tags: Vec<String>,
    pub has_split_inf: bool,
}

/// Extracted body content from an article for search indexing.
#[derive(Default)]
pub struct BodyContent {
    pub etymology_parts: Vec<String>,
    pub etymology_languages: Vec<String>,
    pub pronunciation_parts: Vec<String>,
    pub dialect_form_parts: Vec<String>,
    pub dialect_places: Vec<String>,
    pub written_forms: Vec<String>,
    pub definition_parts: Vec<String>,
    pub example_parts: Vec<String>,
    pub sub_article_lemmas: Vec<String>,
}

/// A parsed inline bibliography reference found in text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineRef {
    pub quote_content: Arc<str>,
    pub offset_start: usize,
    pub offset_end: usize,
    pub code: String,
    pub spec: Option<String>,
}

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

/// Bibliography metadata for a single category.
#[derive(Default)]
pub struct BibliographyCategory {
    pub codes: Vec<String>,
    pub authors: Vec<String>,
    pub titles: Vec<String>,
    pub years: Vec<String>,
}

/// Bibliography metadata to embed in an article search document.
#[derive(Default)]
pub struct ArticleBibliography {
    pub older_source: BibliographyCategory,
    pub written_form_source: BibliographyCategory,
    pub attestation_source: BibliographyCategory,
    pub all: BibliographyCategory,
}

/// Place metadata to embed in an article search document.
#[derive(Default)]
pub struct ArticlePlaceData {
    pub dialect_names: Vec<String>,
    pub dialect_codes: Vec<String>,
    pub dialect_types: Vec<String>,
    pub attestation_names: Vec<String>,
    pub attestation_codes: Vec<String>,
    pub attestation_types: Vec<String>,
    pub names: Vec<String>,
    pub codes: Vec<String>,
    pub types: Vec<String>,
}

/// Concept map from ID to expansion.
pub type ConceptMap = HashMap<String, String>;

/// Matches inline bibliography references in text.
static INLINE_REF_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:\S| )\(([^)]+)\)").unwrap());

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

/// Extract lemma and inflection data from an article.
#[must_use]
pub fn extract_lemma_data(data: &Value) -> LemmaData {
    let mut lemmas = Vec::new();
    let mut inflections = IndexSet::new();
    let mut paradigm_tags = IndexSet::new();
    let mut inflection_tags = IndexSet::new();
    let mut has_split_inf = false;

    for lemma_obj in json_array(data, "lemmas") {
        if let Some(lemma_str) = lemma_obj.get("lemma").and_then(Value::as_str) {
            lemmas.push(lemma_str.to_string());
        }
        has_split_inf |= lemma_obj
            .get("split_inf")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        for paradigm in json_array(lemma_obj, "paradigm_info") {
            for tag in json_array(paradigm, "tags").filter_map(Value::as_str) {
                paradigm_tags.insert(tag.to_string());
            }
            for infl in json_array(paradigm, "inflection") {
                if let Some(wf) = infl.get("word_form").and_then(Value::as_str) {
                    inflections.insert(wf.to_string());
                }
                for tag in json_array(infl, "tags").filter_map(Value::as_str) {
                    inflection_tags.insert(tag.to_string());
                }
            }
        }
    }

    let suggest: Vec<String> = json_array(data, "suggest")
        .filter_map(Value::as_str)
        .map(String::from)
        .collect();

    LemmaData {
        lemmas,
        suggest,
        inflections: inflections.into_iter().collect(),
        paradigm_tags: paradigm_tags.into_iter().collect(),
        inflection_tags: inflection_tags.into_iter().collect(),
        has_split_inf,
    }
}

/// Build a concept map from the raw concepts JSON stored in `dictionary_metadata`.
#[must_use]
pub fn build_concept_map(concepts_json: &Value) -> ConceptMap {
    concepts_json
        .get("concepts")
        .and_then(|v| v.as_object())
        .into_iter()
        .flatten()
        .filter_map(|(id, entry)| {
            let expansion = entry.get("expansion")?.as_str()?;
            Some((id.clone(), expansion.to_string()))
        })
        .collect()
}

/// Build textContent from a template element.
#[must_use]
pub fn format_element_text(content: &str, items: &[Value], concepts: &ConceptMap) -> String {
    let content = content.strip_prefix("/>").unwrap_or(content);

    let mut result = String::new();
    for (i, segment) in content.split('$').enumerate() {
        if i > 0
            && let Some(item) = items.get(i - 1)
        {
            result.push_str(&resolve_item(item, concepts));
        }
        result.push_str(segment);
    }

    result
}

/// Resolve a single item to its text representation.
fn resolve_item(item: &Value, concepts: &ConceptMap) -> String {
    let type_ = item.get("type_").and_then(|v| v.as_str()).unwrap_or("");
    match type_ {
        "usage" => item
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        "language" | "relation" | "entity" => item
            .get("id")
            .and_then(|v| v.as_str())
            .map_or_else(String::new, |id| {
                concepts.get(id).cloned().unwrap_or_else(|| id.to_string())
            }),
        _ => item
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    }
}

/// Extract body content from an article.
#[must_use]
pub fn extract_body_content(data: &Value, concepts: &ConceptMap) -> BodyContent {
    let mut etymology_parts = Vec::new();
    let mut etymology_languages: IndexSet<String> = IndexSet::new();
    let mut pronunciation_parts = Vec::new();
    let mut dialect_form_parts: IndexSet<String> = IndexSet::new();
    let mut dialect_places: IndexSet<String> = IndexSet::new();
    let mut written_forms: IndexSet<String> = IndexSet::new();
    let mut definition_parts = Vec::new();
    let mut example_parts = Vec::new();
    let mut sub_article_lemmas: IndexSet<String> = IndexSet::new();

    let Some(body) = data.get("body") else {
        return BodyContent::default();
    };

    for etym in json_array(body, "etymology") {
        if let Some(content) = etym.get("content").and_then(Value::as_str) {
            let items = etym
                .get("items")
                .and_then(Value::as_array)
                .map_or(&[] as &[_], Vec::as_slice);
            let text = format_element_text(content, items, concepts);
            let text = text.trim();
            if !text.is_empty() {
                etymology_parts.push(text.to_string());
            }
        }

        for item in json_array(etym, "items") {
            if item.get("type_").and_then(Value::as_str) == Some("language")
                && let Some(id) = item.get("id").and_then(Value::as_str)
                && !id.is_empty()
            {
                etymology_languages.insert(id.to_string());
            }
        }
    }

    for content in json_array(body, "pronunciation")
        .filter_map(|pron| pron.get("content").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
    {
        pronunciation_parts.push(content.to_string());
    }

    if let Some(dialect_arr) = body.get("dialect").and_then(Value::as_array) {
        extract_dialect_content(dialect_arr, &mut dialect_form_parts, &mut dialect_places);
    }

    for wf_str in json_array(body, "written_form")
        .flat_map(|wf| json_array(wf, "forms"))
        .filter_map(|form| form.get("written_form").and_then(Value::as_str))
        .filter(|s| !s.is_empty())
    {
        written_forms.insert(wf_str.to_string());
    }

    if let Some(defs) = body.get("definitions").and_then(Value::as_array) {
        extract_definition_content(
            defs,
            &mut definition_parts,
            &mut example_parts,
            &mut sub_article_lemmas,
        );
    }

    BodyContent {
        etymology_parts,
        etymology_languages: etymology_languages.into_iter().collect(),
        pronunciation_parts,
        dialect_form_parts: dialect_form_parts.into_iter().collect(),
        dialect_places: dialect_places.into_iter().collect(),
        written_forms: written_forms.into_iter().collect(),
        definition_parts,
        example_parts,
        sub_article_lemmas: sub_article_lemmas.into_iter().collect(),
    }
}

fn extract_dialect_content(
    dialect_arr: &[Value],
    dialect_form_parts: &mut IndexSet<String>,
    dialect_places: &mut IndexSet<String>,
) {
    let forms = dialect_arr
        .iter()
        .flat_map(|d| json_array(d, "subcats"))
        .flat_map(|sc| json_array(sc, "forms"));

    for form in forms {
        let form_text = form.get("form").and_then(|f| {
            f.as_str()
                .or_else(|| f.get("content").and_then(Value::as_str))
        });
        if let Some(text) = form_text
            && !text.is_empty()
        {
            dialect_form_parts.insert(text.to_string());
        }

        for name in json_array(form, "sources")
            .filter(|s| s.get("show").and_then(Value::as_i64) == Some(1))
            .filter_map(|s| s.get("place_name").and_then(Value::as_str))
        {
            dialect_places.insert(name.to_string());
        }
    }
}

fn extract_definition_content(
    defs: &[Value],
    def_parts: &mut Vec<String>,
    ex_parts: &mut Vec<String>,
    sub_lemmas: &mut IndexSet<String>,
) {
    for def in defs {
        for element in json_array(def, "elements") {
            let type_ = element.get("type_").and_then(Value::as_str).unwrap_or("");
            match type_ {
                "explanation" => {
                    if let Some(content) = element.get("content").and_then(Value::as_str)
                        && !content.is_empty()
                    {
                        def_parts.push(content.to_string());
                    }
                }
                "example" => {
                    if let Some(content) = element
                        .get("quote")
                        .and_then(|q| q.get("content"))
                        .and_then(Value::as_str)
                        && !content.is_empty()
                    {
                        ex_parts.push(content.to_string());
                    }
                }
                "sub_article" => {
                    for s in json_array(element, "lemmas")
                        .filter_map(Value::as_str)
                        .filter(|s| !s.is_empty())
                    {
                        sub_lemmas.insert(s.to_string());
                    }
                }
                _ => {}
            }
        }
        if let Some(sub_defs) = def.get("sub_definitions").and_then(Value::as_array) {
            extract_definition_content(sub_defs, def_parts, ex_parts, sub_lemmas);
        }
    }
}

/// Recursively collect all bibl_id values from JSON.
pub fn collect_bibl_ids<S: std::hash::BuildHasher>(value: &Value, ids: &mut HashSet<i64, S>) {
    walk_json_objects(value, &mut |map| {
        if let Some(bibl_id) = map.get("bibl_id").and_then(Value::as_i64) {
            ids.insert(bibl_id);
        }
    });
}

/// Collect all unique bibl_ids referenced by multiple articles.
pub fn collect_all_bibl_ids<'a>(articles: impl Iterator<Item = &'a Value>) -> Vec<i64> {
    let mut ids = HashSet::new();
    for data in articles {
        collect_bibl_ids(data, &mut ids);
    }
    ids.into_iter().collect()
}

/// Collect attestation bibl_ids from an article's definitions.
#[must_use]
pub fn collect_attestation_bibl_ids(data: &Value) -> HashSet<i64> {
    let mut ids = HashSet::new();
    if let Some(defs) = data.pointer("/body/definitions").and_then(|v| v.as_array()) {
        collect_attestation_from_defs(defs, &mut ids);
    }
    ids
}

fn collect_attestation_from_defs(defs: &[Value], ids: &mut HashSet<i64>) {
    for def in defs {
        for pr in json_array(def, "elements").flat_map(|el| json_array(el, "place_refs")) {
            let vis = pr.get("vis").and_then(Value::as_i64).unwrap_or(0);
            if vis == 1
                && let Some(id) = pr.get("bibl_id").and_then(Value::as_i64)
            {
                ids.insert(id);
            }
        }
        if let Some(sub_defs) = def.get("sub_definitions").and_then(Value::as_array) {
            collect_attestation_from_defs(sub_defs, ids);
        }
    }
}

/// Recursively scan JSON for article_ref or sub_article entries.
pub fn find_related_article_ids(value: &Value, ids: &mut IndexSet<i64>) {
    walk_json_objects(value, &mut |map| {
        if let Some(t) = map.get("type_").and_then(Value::as_str)
            && (t == "article_ref" || t == "sub_article")
            && let Some(aid) = map.get("article_id").and_then(Value::as_i64)
        {
            ids.insert(aid);
        }
    });
}

/// Extract place IDs from dialect sources.
#[must_use]
pub fn extract_dialect_place_ids(article: &Value) -> HashSet<i64> {
    json_array_at(article, "/body/dialect")
        .flat_map(|d| json_array(d, "subcats"))
        .flat_map(|sc| json_array(sc, "forms"))
        .flat_map(|f| json_array(f, "sources"))
        .filter_map(|s| s.get("place_id").and_then(Value::as_i64))
        .collect()
}

/// Extract place IDs from attestation place_refs.
#[must_use]
pub fn extract_attestation_place_ids(article: &Value) -> HashSet<i64> {
    json_array_at(article, "/body/definitions")
        .flat_map(|def| json_array(def, "elements"))
        .flat_map(|elem| json_array(elem, "place_refs"))
        .filter_map(|pr| pr.get("place")?.get("place_id")?.as_i64())
        .collect()
}

/// Extract children place IDs from a place API response.
#[must_use]
pub fn extract_child_place_ids(entry: &Value) -> HashSet<i64> {
    json_array(entry, "child_places")
        .filter_map(|child| child.get("place_id").and_then(Value::as_i64))
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

fn collect_inline_refs_recursive(value: &Value, refs: &mut Vec<InlineRef>) {
    walk_json_objects(value, &mut |map| {
        let content = match map.get("type_").and_then(Value::as_str) {
            Some("example") => map
                .get("quote")
                .and_then(|q| q.get("content"))
                .and_then(Value::as_str),
            Some("explanation") => map.get("content").and_then(Value::as_str),
            _ => None,
        };
        if let Some(content) = content {
            extract_refs_from_quote(content, refs);
        }
    });
}

/// Parse inline references from a single content string.
pub fn extract_refs_from_quote(content: &str, refs: &mut Vec<InlineRef>) {
    let shared_content: Arc<str> = Arc::from(content);
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
                quote_content: Arc::clone(&shared_content),
                offset_start: paren_start,
                offset_end: paren_end,
                code: code.to_string(),
                spec: spec.map(std::string::ToString::to_string),
            });
        }
    }
}

/// Extract bibliography IDs from article JSON and build bibliography metadata
/// from a lookup map.
#[must_use]
pub fn build_article_bibliography<S: std::hash::BuildHasher>(
    data: &Value,
    bib_map: &HashMap<i64, (String, String, String, String), S>,
) -> ArticleBibliography {
    let mut all_bibl_ids = HashSet::new();
    collect_bibl_ids(data, &mut all_bibl_ids);

    let mut older_source_ids: IndexSet<i64> = IndexSet::new();
    let mut written_form_source_ids: IndexSet<i64> = IndexSet::new();

    for id in json_array_at(data, "/body/older_source")
        .filter_map(|source| source.get("bibl_id").and_then(Value::as_i64))
    {
        older_source_ids.insert(id);
    }

    for id in json_array_at(data, "/body/written_form")
        .flat_map(|wf| json_array(wf, "forms"))
        .flat_map(|form| json_array(form, "sources"))
        .filter_map(|source| source.get("bibl_id").and_then(Value::as_i64))
    {
        written_form_source_ids.insert(id);
    }

    let attestation_source_ids: Vec<i64> = collect_attestation_bibl_ids(data).into_iter().collect();
    let all_bibl_ids: Vec<i64> = all_bibl_ids.into_iter().collect();
    let older_source_ids: Vec<i64> = older_source_ids.into_iter().collect();
    let written_form_source_ids: Vec<i64> = written_form_source_ids.into_iter().collect();

    ArticleBibliography {
        older_source: build_bib_category(&older_source_ids, bib_map),
        written_form_source: build_bib_category(&written_form_source_ids, bib_map),
        attestation_source: build_bib_category(&attestation_source_ids, bib_map),
        all: build_bib_category(&all_bibl_ids, bib_map),
    }
}

fn build_bib_category<S: std::hash::BuildHasher>(
    ids: &[i64],
    bib_map: &HashMap<i64, (String, String, String, String), S>,
) -> BibliographyCategory {
    let mut codes: IndexSet<&str> = IndexSet::new();
    let mut authors: IndexSet<&str> = IndexSet::new();
    let mut titles: IndexSet<&str> = IndexSet::new();
    let mut years: IndexSet<&str> = IndexSet::new();

    for &id in ids {
        if let Some((code, author, title, year)) = bib_map.get(&id) {
            if !code.is_empty() {
                codes.insert(code);
            }
            if !author.is_empty() {
                authors.insert(author);
            }
            if !title.is_empty() {
                titles.insert(title);
            }
            if !year.is_empty() {
                years.insert(year);
            }
        }
    }

    BibliographyCategory {
        codes: codes.into_iter().map(String::from).collect(),
        authors: authors.into_iter().map(String::from).collect(),
        titles: titles.into_iter().map(String::from).collect(),
        years: years.into_iter().map(String::from).collect(),
    }
}

/// Build split place metadata from dialect and attestation place ID lists.
#[must_use]
pub fn build_article_place_data<S: std::hash::BuildHasher>(
    dialect_ids: &[i64],
    attestation_ids: &[i64],
    place_map: &HashMap<i64, (String, String, String), S>,
) -> ArticlePlaceData {
    let (dialect_names, dialect_codes, dialect_types) =
        collect_place_fields(dialect_ids, place_map);
    let (attestation_names, attestation_codes, attestation_types) =
        collect_place_fields(attestation_ids, place_map);

    let mut all_ids: Vec<i64> = dialect_ids
        .iter()
        .chain(attestation_ids.iter())
        .copied()
        .collect();
    all_ids.sort_unstable();
    all_ids.dedup();
    let (names, codes, types) = collect_place_fields(&all_ids, place_map);

    ArticlePlaceData {
        dialect_names,
        dialect_codes,
        dialect_types,
        attestation_names,
        attestation_codes,
        attestation_types,
        names,
        codes,
        types,
    }
}

fn collect_place_fields<S: std::hash::BuildHasher>(
    ids: &[i64],
    place_map: &HashMap<i64, (String, String, String), S>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut names: IndexSet<&str> = IndexSet::new();
    let mut codes: IndexSet<&str> = IndexSet::new();
    let mut types: IndexSet<&str> = IndexSet::new();

    for &id in ids {
        if let Some((code, full_name, place_type)) = place_map.get(&id) {
            let display_name = if full_name.is_empty() {
                code
            } else {
                full_name
            };
            if !display_name.is_empty() {
                names.insert(display_name);
            }
            if !code.is_empty() {
                codes.insert(code);
            }
            if !place_type.is_empty() {
                types.insert(place_type);
            }
        }
    }

    (
        names.into_iter().map(String::from).collect(),
        codes.into_iter().map(String::from).collect(),
        types.into_iter().map(String::from).collect(),
    )
}

/// Analyze an article's JSON.
#[must_use]
pub fn analyze_article(dict: UibDictionary, article_data: &Value) -> ArticleAnalysis {
    let primary_lemma = find_first_lemma(article_data);
    let mut bibl_ids = HashSet::new();
    let mut related_article_ids = IndexSet::new();

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
        related_article_ids: related_article_ids.into_iter().collect(),
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
    fn test_extract_lemma_data_noun_with_inflections() {
        let data = json!({
            "lemmas": [
                {
                    "lemma": "fjordsting",
                    "split_inf": false,
                    "paradigm_info": [
                        {
                            "tags": ["NOUN", "Masc"],
                            "inflection": [
                                { "word_form": "fjordsting", "tags": ["Sing", "Ind"] },
                                { "word_form": "fjordstingen", "tags": ["Sing", "Def"] },
                                { "word_form": "fjordstinger", "tags": ["Plur", "Ind"] },
                                { "word_form": "fjordstingene", "tags": ["Plur", "Def"] },
                            ]
                        },
                        {
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
            "suggest": ["fjordsting"]
        });

        let ld = extract_lemma_data(&data);
        assert_eq!(ld.lemmas, vec!["fjordsting"]);
        assert_eq!(ld.suggest, vec!["fjordsting"]);
        assert_eq!(
            ld.inflections,
            vec![
                "fjordsting",
                "fjordstingen",
                "fjordstinger",
                "fjordstingene",
                "fjordstinga",
            ]
        );
        assert_eq!(ld.paradigm_tags, vec!["NOUN", "Masc", "Fem"]);
        assert_eq!(ld.inflection_tags, vec!["Sing", "Ind", "Def", "Plur"]);
        assert!(!ld.has_split_inf);
    }

    #[test]
    fn test_extract_lemma_data_abbreviation() {
        let data = json!({
            "lemmas": [
                {
                    "lemma": "F",
                    "paradigm_info": [
                        { "tags": ["ABBR"], "inflection": [{ "word_form": "F", "tags": [] }] }
                    ]
                },
                {
                    "lemma": "f",
                    "paradigm_info": [
                        { "tags": ["ABBR"], "inflection": [{ "word_form": "f", "tags": [] }] }
                    ]
                }
            ],
            "suggest": ["F", "f"]
        });

        let ld = extract_lemma_data(&data);
        assert_eq!(ld.lemmas, vec!["F", "f"]);
        assert_eq!(ld.suggest, vec!["F", "f"]);
        assert_eq!(ld.inflections, vec!["F", "f"]);
        assert_eq!(ld.paradigm_tags, vec!["ABBR"]);
        assert!(ld.inflection_tags.is_empty());
        assert!(!ld.has_split_inf);
    }

    #[test]
    fn test_extract_lemma_data_verb_split_inf() {
        let data = json!({
            "lemmas": [
                {
                    "lemma": "velja",
                    "split_inf": true,
                    "paradigm_info": [
                        {
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

        let ld = extract_lemma_data(&data);
        assert!(ld.has_split_inf);
        assert_eq!(ld.lemmas, vec!["velja"]);
        assert_eq!(ld.inflections, vec!["vel", "valde"]);
        assert_eq!(ld.paradigm_tags, vec!["VERB"]);
        assert_eq!(ld.inflection_tags, vec!["Pres", "Past"]);
    }

    #[test]
    fn test_extract_lemma_data_empty() {
        let data = json!({});
        let ld = extract_lemma_data(&data);
        assert!(ld.lemmas.is_empty());
        assert!(ld.suggest.is_empty());
        assert!(ld.inflections.is_empty());
        assert!(ld.paradigm_tags.is_empty());
        assert!(ld.inflection_tags.is_empty());
        assert!(!ld.has_split_inf);
    }

    #[test]
    fn test_extract_body_content_etymology() {
        let data = json!({
            "body": {
                "etymology": [
                    {
                        "content": "av norrønt $ $",
                        "items": [
                            { "type_": "usage", "text": "fjǫrðr" },
                            { "type_": "usage", "text": "þing" },
                        ]
                    }
                ]
            }
        });
        let concepts = HashMap::new();
        let body = extract_body_content(&data, &concepts);
        assert_eq!(body.etymology_parts, vec!["av norrønt fjǫrðr þing"]);
    }

    #[test]
    fn test_extract_body_content_dialect() {
        let data = json!({
            "body": {
                "dialect": [
                    {
                        "subcats": [
                            {
                                "forms": [
                                    {
                                        "form": "trållskåg",
                                        "sources": [
                                            { "show": 1, "place_name": "Nordfjell" },
                                            { "show": 0, "place_name": "Sørdal" },
                                            { "show": 1, "place_name": "Vestmark" }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        });
        let concepts = HashMap::new();
        let body = extract_body_content(&data, &concepts);
        assert_eq!(body.dialect_form_parts, vec!["trållskåg"]);
        assert_eq!(body.dialect_places, vec!["Nordfjell", "Vestmark"]);
    }

    #[test]
    fn test_extract_body_content_empty() {
        let data = json!({});
        let concepts = HashMap::new();
        let body = extract_body_content(&data, &concepts);
        assert!(body.etymology_parts.is_empty());
        assert!(body.dialect_form_parts.is_empty());
        assert!(body.definition_parts.is_empty());
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
        let mut ids = IndexSet::new();
        find_related_article_ids(&data, &mut ids);
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![2002]);
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
        let mut ids = IndexSet::new();
        find_related_article_ids(&data, &mut ids);
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![5001]);
    }

    #[test]
    fn test_find_related_article_ids_deduplicates() {
        let data = json!({
            "items": [
                { "type_": "article_ref", "article_id": 3000 },
                { "type_": "article_ref", "article_id": 3000 },
            ]
        });
        let mut ids = IndexSet::new();
        find_related_article_ids(&data, &mut ids);
        assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![3000]);
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
        let mut ids = IndexSet::new();
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

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_build_article_bibliography() {
        let data = json!({
            "body": {
                "older_source": [
                    { "bibl_id": 100 },
                    { "bibl_id": 200 }
                ],
                "written_form": [
                    {
                        "forms": [
                            {
                                "sources": [
                                    { "bibl_id": 2027 },
                                    { "bibl_id": 10482 }
                                ]
                            }
                        ]
                    }
                ],
                "definitions": []
            }
        });

        let mut bib_map = HashMap::new();
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
        assert_eq!(bib.older_source.codes, vec!["FiktA", "FiktB"]);
        assert_eq!(bib.written_form_source.codes, vec!["E.DiktAS", "SagaOH"]);
        assert!(bib.attestation_source.codes.is_empty());
        assert_eq!(bib.all.codes.len(), 4);
    }
}
