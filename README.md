# Ordbok API Worker

Arbeidarprosessen for å synkronisere dataa frå UiB med Ordbok API sin database.

## Korleis byggje

### Fyrste gong

1. Last ned og installer [Rust](https://www.rust-lang.org/tools/install).
2. Last ned og installer [Docker](https://docs.docker.com/get-docker/).
3. Opprett ein kopi av `template.env` og kall den `.env`. Her legg du inn dei nødvendige miljøvariablane.
4. Køyr `docker-compose up -d` for å starte ein lokal Redis-database.
5. Køyr `./run.sh` eller `.\run.ps1` for å byggje og køyre arbeidarprosessen. Du kan ogso køyre `cargo run --features use_dotenv` for å køyre utan støtte for [Matrix](https://matrix.org/)-varsel.

### Skript

- `./run.sh` eller `.\run.ps1` for å byggje og køyre arbeidarprosessen.
- `./reload.sh` eller `.\reload.ps1` for å slette databasen i `.redis` og erstatte han med ein kopi av han i `.redis-backup`.
- `./register-bot.sh` eller `.\register-bot.ps1` for å registrere ein Matrix bott-konto du kan bruke til å sende varsel. (Krev at du har [Powershell](;https://learn.microsoft.com/powershell/scripting/install/installing-powershell) installert, sjølv om du brukar Linux eller MacOS.)
- `./check.sh` eller `.\check.ps1` for å sjekke kode for stilfeil og andre problem med `clippy` og `cargo fmt`. Det er tilrådd å køyre dette minst ein gong med og utan valfrie funksjonar ([cargo features](https://doc.rust-lang.org/cargo/reference/features.html)) før du opnar ein pull request.

[ISC](LICENCE)
