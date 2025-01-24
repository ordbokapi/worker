docker compose down
rm -rec -for .redis
cp -rec .redis-backup .redis
docker compose up -d
