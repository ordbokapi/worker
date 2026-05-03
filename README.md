<!--
SPDX-FileCopyrightText: Copyright (C) 2026 Adaline Simonian
SPDX-License-Identifier: AGPL-3.0-or-later

This file is part of Ordbok API.

Ordbok API is free software: you can redistribute it and/or modify it under
the terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

Ordbok API is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
details.

You should have received a copy of the GNU Affero General Public License
along with Ordbok API. If not, see <https://www.gnu.org/licenses/>.
-->

# Ordbok API Worker

Arbeidarprosessen for å synkronisere dataa frå UiB med databasen til Ordbok API.

## Korleis byggje

### Fyrste gong

1. Last ned og installer [Rust](https://www.rust-lang.org/tools/install).
2. Last ned og installer [Docker](https://docs.docker.com/get-docker/).
3. Opprett `.env`: `cp template.env .env`. Standardverdiane fungerer saman med `docker-compose.yml` utan endringar. Har du alt sett opp [API-kodelageret](https://github.com/ordbokapi/api) med same standardverdiar, deler dei same Docker-tenestene automatisk.
4. Køyr `docker compose up -d` for å starte lokale tenester (PostgreSQL, MeiliSearch, Valkey). Dersom du alt har starta tenestene frå API-kodelageret, kan du hoppe over dette steget.
5. Køyr `./run.sh` eller `.\run.ps1` for å byggje og køyre arbeidarprosessen med alle funksjonar. Du kan ogso køyre `cargo run --features use_dotenv` for å køyre med berre grunnfunksjonane (utan [Matrix](https://matrix.org/)-varsel o.l.).

### Skript

- `./run.sh` eller `.\run.ps1` for å byggje og køyre arbeidarprosessen.
- `./reset-queues.sh` eller `.\reset-queues.ps1` for å tømme køane. Kødata vert lagra i `.valkey-backup` og kan gjenopprettast derifrå. Erstattar alt i `.valkey-backup` om det finst data i mappa.
- `./register-bot.sh` eller `.\register-bot.ps1` for å registrere ein Matrix-bottkonto du kan bruke til å sende varsel. (Krev at du har [PowerShell](https://learn.microsoft.com/powershell/scripting/install/installing-powershell) installert, sjølv om du brukar Linux eller macOS.)
- `./check.sh` eller `.\check.ps1` for å sjekke kode for stilfeil og andre problem med `clippy` og `cargo fmt`. Det er tilrådd å køyre dette minst éin gong med og utan valfrie funksjonar ([cargo features](https://doc.rust-lang.org/cargo/reference/features.html)) før du opnar ein pull request.

## Lisens

[AGPL-3.0-or-later](COPYING)
