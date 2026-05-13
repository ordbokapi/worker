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

CREATE TABLE article_bibliography (
    dictionary TEXT NOT NULL,
    article_id BIGINT NOT NULL,
    bibl_id BIGINT NOT NULL,
    PRIMARY KEY (dictionary, article_id, bibl_id),
    FOREIGN KEY (dictionary, article_id) REFERENCES articles(dictionary, id) ON DELETE CASCADE
);

CREATE INDEX idx_article_bibliography_bibl_id ON article_bibliography (bibl_id);

-- Backfill from existing article data.
INSERT INTO article_bibliography (dictionary, article_id, bibl_id)
SELECT DISTINCT a.dictionary, a.id, (val::text)::bigint
FROM articles a, LATERAL jsonb_path_query(a.data, '$.**.bibl_id') AS val
WHERE val IS NOT NULL AND jsonb_typeof(val) = 'number'
ON CONFLICT DO NOTHING;
