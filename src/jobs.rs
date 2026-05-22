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
}

/// Fetch a single article from UiB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchArticleJob {
    pub dictionary: String,
    pub article_id: i64,
}

/// Index a single article in Meilisearch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexArticleJob {
    pub dictionary: String,
    pub article_id: i64,
}

/// Batch-index multiple articles in Meilisearch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchIndexJob {
    /// Vec of "dict:article_id" keys.
    pub article_keys: Vec<String>,
}

/// Fetch dictionary metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchDictionaryMetadataJob {
    pub dictionary: String,
}

/// Send a Matrix notification message.
#[cfg(feature = "matrix_notifs")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMatrixMessageJob {
    pub message: String,
}

/// Fetch a single bibliography entry from the word bank API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchBibliographyJob {
    pub bibl_id: i64,
}

/// Fetch a single place entry from the word bank API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchPlaceJob {
    pub place_id: i64,
}

/// Backfill inline refs for one or more articles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillInlineRefsJob {
    pub article_ids: Vec<i64>,
}

/// Attempt to resolve an unresolved inline code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveInlineCodeJob {
    pub code: String,
}

/// Periodic sweep of stuck or failed items.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SweepJob;
