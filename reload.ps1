if (-not (Test-Path .redis)) {
    New-Item -ItemType Directory -Name .redis
}
if (-not (Test-Path .redis-backup)) {
    New-Item -ItemType Directory -Name .redis-backup
}
docker compose down
rm -rec -for .redis
cp -rec .redis-backup .redis
docker compose up -d
