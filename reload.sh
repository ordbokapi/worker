#!/bin/sh
mkdir -p .redis-backup
mkdir -p .redis
docker compose down
rm -rf .redis
cp -r .redis-backup .redis
docker compose up -d
