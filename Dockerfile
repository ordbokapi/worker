# SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Ordbok API.
#
# Ordbok API is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.

# -----------------------------------------
# Build stage
# -----------------------------------------
FROM rust:1.93 AS builder

WORKDIR /usr/src/ordbokapi-worker

# Copy Cargo.toml and Cargo.lock
COPY Cargo.toml Cargo.lock ./

# Needed at compile time by sqlx::migrate! macro.
COPY migrations migrations

# Create dummy main.rs to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies
RUN cargo build --release --features "matrix_notifs sentry_integration"
RUN rm src/main.rs

# Copy source code
COPY . .

# Build the application with all features
RUN cargo build --release --features "matrix_notifs sentry_integration"

# -----------------------------------------
# Run stage
# -----------------------------------------

# FROM gcr.io/distroless/base-debian12 # Can be used when statically linking with MUSL
FROM debian:stable-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y tini ca-certificates curl && rm -rf /var/lib/apt/lists/* && update-ca-certificates

# Copy the binary from the builder
COPY --from=builder /usr/src/ordbokapi-worker/target/release/ordbokapi-worker /usr/local/bin/ordbokapi-worker

EXPOSE 3001

CMD ["/usr/bin/tini", "--", "ordbokapi-worker"]
