# -----------------------------------------
# Build stage
# -----------------------------------------
FROM rust:1.84 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y libssl-dev pkg-config openssh-client && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/ordbokapi-worker

# Copy Cargo.toml and Cargo.lock
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies
RUN cargo build --release --all-features
RUN rm src/main.rs

# Copy source code
COPY . .

# Build the application with all features
RUN cargo build --release --all-features

# -----------------------------------------
# Run stage
# -----------------------------------------

# FROM gcr.io/distroless/base-debian12 # Can be used when statically linking with MUSL
FROM debian:stable-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y libssl3 && rm -rf /var/lib/apt/lists/*

# Copy the binary from the builder
COPY --from=builder /usr/src/ordbokapi-worker/target/release/ordbokapi-worker /usr/local/bin/ordbokapi-worker

# Set the entrypoint
ENTRYPOINT ["ordbokapi-worker"]
CMD []
