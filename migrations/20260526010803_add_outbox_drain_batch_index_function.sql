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

CREATE FUNCTION outbox_drain_batch_index(target_keys int)
RETURNS TABLE(id bigint, payload jsonb)
LANGUAGE plpgsql AS $$
DECLARE
  row record;
  row_keys int;
  total int := 0;
BEGIN
  FOR row IN
    SELECT o.id, o.payload
    FROM job_outbox o
    WHERE o.processed_at IS NULL AND o.job_type = 'batch_index'
    ORDER BY o.id
    FOR UPDATE SKIP LOCKED
  LOOP
    row_keys := jsonb_array_length(row.payload -> 'article_keys');
    IF total + row_keys > target_keys AND total > 0 THEN
      EXIT;
    END IF;
    id := row.id;
    payload := row.payload;
    total := total + row_keys;
    RETURN NEXT;
  END LOOP;
END;
$$;

