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

use serde::{Deserialize, Serialize};

/// Fetch the full article list for a dictionary from UiB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchArticleListJob {
    pub dictionary: String,
    #[serde(default)]
    pub force: bool,
}

impl FetchArticleListJob {
    pub const NAMESPACE: &str = "apalis:article-list";
}

/// Fetch a single article from UiB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchArticleJob {
    pub dictionary: String,
    pub article_id: i64,
    #[serde(default)]
    pub revision: Option<i64>,
    #[serde(default)]
    pub updated_at: String,
}

impl FetchArticleJob {
    pub const NAMESPACE: &str = "apalis:article";
}

/// Batch-index multiple articles in Meilisearch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchIndexJob {
    /// Vec of "dict:article_id" keys.
    pub article_keys: Vec<String>,
}

impl BatchIndexJob {
    pub const NAMESPACE: &str = "apalis:batch-index";
}

/// Fetch dictionary metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchDictionaryMetadataJob {
    pub dictionary: String,
}

impl FetchDictionaryMetadataJob {
    pub const NAMESPACE: &str = "apalis:dict-metadata";
}

/// Send a Matrix notification message.
#[cfg(feature = "matrix_notifs")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMatrixMessageJob {
    pub message: String,
}

#[cfg(feature = "matrix_notifs")]
impl SendMatrixMessageJob {
    pub const NAMESPACE: &str = "apalis:matrix-notify";
}

/// Fetch a single bibliography entry from the word bank API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchBibliographyJob {
    pub bibl_id: i64,
}

impl FetchBibliographyJob {
    pub const NAMESPACE: &str = "apalis:bibliography";
}

/// Fetch a single place entry from the word bank API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchPlaceJob {
    pub place_id: i64,
}

impl FetchPlaceJob {
    pub const NAMESPACE: &str = "apalis:place";
}

/// Backfill inline refs for one or more articles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillInlineRefsJob {
    pub article_ids: Vec<i64>,
}

impl BackfillInlineRefsJob {
    pub const NAMESPACE: &str = "apalis:backfill-inline-refs";
}

/// Attempt to resolve an unresolved inline code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveInlineCodeJob {
    pub code: String,
}

impl ResolveInlineCodeJob {
    pub const NAMESPACE: &str = "apalis:resolve-inline-code";
}

/// Periodic sweep of stuck or failed items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepJob;

impl SweepJob {
    pub const NAMESPACE: &str = "apalis:sweep";
}

/// All queue namespaces used by the worker.
pub const QUEUE_NAMESPACES: &[&str] = &[
    FetchArticleListJob::NAMESPACE,
    FetchArticleJob::NAMESPACE,
    BatchIndexJob::NAMESPACE,
    FetchDictionaryMetadataJob::NAMESPACE,
    FetchBibliographyJob::NAMESPACE,
    FetchPlaceJob::NAMESPACE,
    BackfillInlineRefsJob::NAMESPACE,
    ResolveInlineCodeJob::NAMESPACE,
    SweepJob::NAMESPACE,
    #[cfg(feature = "matrix_notifs")]
    SendMatrixMessageJob::NAMESPACE,
];
