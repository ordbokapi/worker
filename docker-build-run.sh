#!/bin/sh

# builds the Dockerfile in the current directory and runs the container interactively. When the container is stopped, it is removed.
# Usage: ./docker-build-run.sh [args]

dockerfile="Dockerfile"
dockerimageName="ordbokapi-worker"

echo "Building Docker image $dockerimageName from $dockerfile..."
docker build -t $dockerimageName -f $dockerfile .

echo "Running Docker container $dockerimageName interactively..."
docker run --rm -it --env-file .env --network host $dockerimageName "$@"
