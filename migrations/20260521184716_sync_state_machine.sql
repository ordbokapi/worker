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

ALTER TABLE articles ADD COLUMN sync_status TEXT NOT NULL DEFAULT 'idle';
ALTER TABLE articles ADD COLUMN status_changed_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE bibliography ADD COLUMN sync_status TEXT NOT NULL DEFAULT 'idle';
ALTER TABLE bibliography ADD COLUMN status_changed_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE places ADD COLUMN sync_status TEXT NOT NULL DEFAULT 'idle';
ALTER TABLE places ADD COLUMN status_changed_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX idx_articles_pending ON articles (sync_status, status_changed_at)
    WHERE sync_status != 'idle';
CREATE INDEX idx_bibliography_pending ON bibliography (sync_status, status_changed_at)
    WHERE sync_status != 'idle';
CREATE INDEX idx_places_pending ON places (sync_status, status_changed_at)
    WHERE sync_status != 'idle';

CREATE TABLE job_outbox (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    job_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_job_outbox_dedup ON job_outbox (job_type, job_key)
    WHERE processed_at IS NULL;

CREATE INDEX idx_job_outbox_unprocessed ON job_outbox (id)
    WHERE processed_at IS NULL;
