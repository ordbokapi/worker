-- SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
-- SPDX-License-Identifier: AGPL-3.0-or-later
--
-- This file is part of Ordbok API.
--
-- Ordbok API is free software: you can redistribute it and/or modify it under
-- the terms of the GNU Affero General Public License as published by the Free
-- Software Foundation, either version 3 of the License, or (at your option) any
-- later version.
--
-- Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
-- WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
-- A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
-- details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.

-- Initial schema for Ordbok API.

-- Articles table stores the full article JSON from UiB.
CREATE TABLE articles (
    dictionary TEXT NOT NULL,
    id BIGINT NOT NULL,
    data JSONB NOT NULL,
    primary_lemma TEXT NOT NULL DEFAULT '',
    revision BIGINT NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dictionary, id)
);

-- Dictionary metadata, concepts, word_classes, and word_subclasses.
CREATE TABLE dictionary_metadata (
    dictionary TEXT NOT NULL,
    key TEXT NOT NULL,
    data JSONB NOT NULL,
    modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dictionary, key)
);

-- Sync state tracking.
CREATE TABLE sync_state (
    dictionary TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (dictionary, key)
);
