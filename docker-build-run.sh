#!/bin/sh

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

# builds the Dockerfile in the current directory and runs the container interactively. When the container is stopped, it is removed.
# Usage: ./docker-build-run.sh [args]

dockerfile="Dockerfile"
dockerimageName="ordbokapi-worker"

echo "Building Docker image $dockerimageName from $dockerfile..."
docker build -t $dockerimageName -f $dockerfile .

echo "Running Docker container $dockerimageName interactively..."
docker run --rm -it --env-file .env --network host $dockerimageName "$@"
