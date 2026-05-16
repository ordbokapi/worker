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

CREATE TABLE places (
    id BIGINT PRIMARY KEY,
    place_name TEXT NOT NULL DEFAULT '',
    place_name_full TEXT NOT NULL DEFAULT '',
    place_type TEXT NOT NULL DEFAULT '',
    parent_id BIGINT,
    place_order INT NOT NULL DEFAULT 0,
    municipality_nr TEXT,
    weight_threshold INT NOT NULL DEFAULT 0,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE article_place (
    dictionary TEXT NOT NULL,
    article_id BIGINT NOT NULL,
    place_id BIGINT NOT NULL,
    context TEXT NOT NULL DEFAULT 'dialect',
    PRIMARY KEY (dictionary, article_id, place_id, context),
    FOREIGN KEY (dictionary, article_id) REFERENCES articles(dictionary, id) ON DELETE CASCADE
);

CREATE INDEX idx_article_place_place_id ON article_place (place_id);
CREATE INDEX idx_article_place_context ON article_place (dictionary, article_id, context);

-- Backfill from existing article data.
INSERT INTO article_place (dictionary, article_id, place_id, context)
SELECT DISTINCT sub.dictionary, sub.article_id, sub.place_id, sub.context FROM (
    -- place_ids from dialect sources.
    SELECT a.dictionary, a.id AS article_id, (s->>'place_id')::bigint AS place_id, 'dialect' AS context
    FROM articles a,
    LATERAL jsonb_array_elements(a.data->'body'->'dialect') d,
    LATERAL jsonb_array_elements(d->'subcats') sc,
    LATERAL jsonb_array_elements(sc->'forms') f,
    LATERAL jsonb_array_elements(f->'sources') s
    WHERE a.dictionary = 'no' AND s->>'place_id' IS NOT NULL
    UNION
    -- place_ids from place_refs in definitions.
    SELECT a.dictionary, a.id AS article_id, (pr->'place'->>'place_id')::bigint AS place_id, 'attestation' AS context
    FROM articles a,
    LATERAL jsonb_array_elements(a.data->'body'->'definitions') def,
    LATERAL jsonb_array_elements(def->'elements') elem,
    LATERAL jsonb_array_elements(elem->'place_refs') pr
    WHERE a.dictionary = 'no' AND pr->'place'->>'place_id' IS NOT NULL
) sub
ON CONFLICT DO NOTHING;
