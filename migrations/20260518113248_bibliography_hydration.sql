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

CREATE TABLE inline_ref_parse (
    dictionary TEXT NOT NULL,
    article_id BIGINT NOT NULL,
    quote_content TEXT NOT NULL,
    offset_start INT NOT NULL,
    offset_end INT NOT NULL,
    code TEXT NOT NULL,
    spec TEXT,
    ref_type TEXT,
    bibl_id BIGINT REFERENCES bibliography(id),
    place_id BIGINT REFERENCES places(id),
    FOREIGN KEY (dictionary, article_id) REFERENCES articles(dictionary, id) ON DELETE CASCADE
);

CREATE INDEX idx_inline_ref_lookup
    ON inline_ref_parse (dictionary, article_id);

CREATE INDEX idx_inline_ref_unresolved
    ON inline_ref_parse (code) WHERE ref_type IS NULL;
