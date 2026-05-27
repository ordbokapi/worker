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
3. Opprett `.env`: `cp template.env .env`. Standardverdiane fungerer saman med `docker-compose.yml` utan endringar.

> [!NOTE]
> Har du alt starta tenestene i API-kodelageret, må du stoppe dei før du køyrer dette steget. Du kan kopiere `.meilidata`, `.pgdata` og `.valkey` frå API-kodelageret til dette kodelageret for å unngå å måtte synkronisere på nytt.
>
> Vil du bruke tenestene i dette kodelageret med API-et, kontroller at du har same verdiane for port og slikt i `.env` i begge kodelagera.

4. Køyr `docker compose up bootstrap` for å fylle databasen med siste dagleg snapshot frå Ordbok API. Dette tek somme minutt. Du kan sjå framdrifta i loggane.

> [!IMPORTANT]
> **Det er svært tilrådd å køyre bootstrap** før du startar arbeidarprosessen, for å unngå at han må synkronisere alt frå starten av, som kan taka veldig lang tid og rammar tenestene til UiB med mange førespurnader.
>
> Dette vert òg gjort automatisk når ein køyrer `docker compose up -d` for fyrste gong, men då får ein ikkje sjå loggane.

5. Køyr `docker compose up -d` for å starte lokale tenester (PostgreSQL, MeiliSearch, Valkey).
6. Køyr `./run.sh` eller `.\run.ps1` for å byggje og køyre arbeidarprosessen med alle funksjonar. Du kan ogso køyre `cargo run --features use_dotenv` for å køyre med berre grunnfunksjonane (utan [Matrix](https://matrix.org/)-varsel o.l.).

### Skript

- `./run.sh` eller `.\run.ps1` for å byggje og køyre arbeidarprosessen.
- `./reset-queues.sh` eller `.\reset-queues.ps1` for å tømme køane. Kødata vert lagra i `.valkey-backup` og kan gjenopprettast derifrå. Erstattar alt i `.valkey-backup` om det finst data i mappa.
- `./register-bot.sh` eller `.\register-bot.ps1` for å registrere ein Matrix-bottkonto du kan bruke til å sende varsel. (Krev at du har [PowerShell](https://learn.microsoft.com/powershell/scripting/install/installing-powershell) installert, sjølv om du brukar Linux eller macOS.)
- `./check.sh` eller `.\check.ps1` for å sjekke kode for stilfeil og andre problem med `clippy` og `cargo fmt`. Det er tilrådd å køyre dette minst éin gong med og utan valfrie funksjonar ([cargo features](https://doc.rust-lang.org/cargo/reference/features.html)) før du opnar ein pull request.

## Lisens

[AGPL-3.0-or-later](COPYING)
