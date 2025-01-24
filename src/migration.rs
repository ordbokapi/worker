// src/migration.rs

use anyhow::{anyhow, Result};
use log::info;
use redis::AsyncCommands;

#[derive(Debug)]
pub struct MigrationVersion {
    pub version: i64,
    /// A list of migrations to run in order for that version.
    pub migrations: Vec<MigrationFn>,
}

/// Each MigrationFn runs commands on a pipeline (or multi/exec) to do the changes.
pub type MigrationFn = fn(&mut redis::Pipeline) -> Result<()>;

/// Key where we store the current DB version
const VERSION_KEY: &str = "ordbokapi:version";

/// Our version array. This corresponds to your TypeScript code (versions 1 & 2).
pub fn get_versions() -> Vec<MigrationVersion> {
    vec![
        MigrationVersion {
            version: 1,
            migrations: vec![migration_v1_1],
        },
        MigrationVersion {
            version: 2,
            migrations: vec![migration_v2_1],
        },
    ]
}

/// Migration #1:
/// - config set maxmemory-policy noeviction
/// - ft.config set DEFAULT_DIALECT 3
/// - ft.create idx:article (with multiple JSONpaths)
/// - ft.create idx:article:bm, idx:article:nn, idx:article:no, etc.
fn migration_v1_1(pipe: &mut redis::Pipeline) -> Result<()> {
    // FT.CREATE idx:article ON JSON PREFIX article: STOPWORDS 0
    // SCHEMA $.lemmas[*].lemma AS lemma TEXT SORTABLE …
    pipe.add_command(
        redis::cmd("FT.CREATE")
            .arg("idx:article")
            .arg("ON")
            .arg("JSON")
            .arg("PREFIX")
            .arg("1") // number of prefixes
            .arg("article:")
            .arg("STOPWORDS")
            .arg("0")
            // fields:
            .arg("SCHEMA")
            .arg("$.lemmas[*].lemma")
            .arg("AS")
            .arg("lemma")
            .arg("TEXT")
            .arg("SORTABLE")
            //
            .arg("$.suggest[*]")
            .arg("AS")
            .arg("suggest")
            .arg("TEXT")
            .arg("SORTABLE")
            //
            .arg("$.etymology[*].items[*].text")
            .arg("AS")
            .arg("etymology")
            .arg("TEXT")
            .arg("SORTABLE")
            //
            .arg("$.lemmas[*].paradigm_info[*].tags[*]")
            .arg("AS")
            .arg("paradigm_tags")
            .arg("TAG")
            //
            .arg("$.lemmas[*].paradigm_info[*].inflection[*].tags[*]")
            .arg("AS")
            .arg("inflection_tags")
            .arg("TAG")
            //
            .arg("$.lemmas[*].paradigm_info[*].inflection[*].word_form")
            .arg("AS")
            .arg("inflection")
            .arg("TEXT")
            .arg("SORTABLE")
            //
            .arg("$.__ordbokapi__.hasSplitInf")
            .arg("AS")
            .arg("split_infinitive")
            .arg("TAG")
            .to_owned(),
    );

    // Create the same index for each dictionary separately.
    for dict in ["bm", "nn", "no"] {
        let idx_name = format!("idx:article:{dict}");
        let prefix = format!("article:{dict}:");
        pipe.add_command(
            redis::cmd("FT.CREATE")
                .arg(&idx_name)
                .arg("ON")
                .arg("JSON")
                .arg("PREFIX")
                .arg("1")
                .arg(&prefix)
                .arg("STOPWORDS")
                .arg("0")
                .arg("SCHEMA")
                .arg("$.lemmas[*].lemma")
                .arg("AS")
                .arg("lemma")
                .arg("TEXT")
                .arg("SORTABLE")
                //
                .arg("$.suggest[*]")
                .arg("AS")
                .arg("suggest")
                .arg("TEXT")
                .arg("SORTABLE")
                //
                .arg("$.etymology[*].items[*].text")
                .arg("AS")
                .arg("etymology")
                .arg("TEXT")
                .arg("SORTABLE")
                //
                .arg("$.lemmas[*].paradigm_info[*].tags[*]")
                .arg("AS")
                .arg("paradigm_tags")
                .arg("TAG")
                //
                .arg("$.lemmas[*].paradigm_info[*].inflection[*].tags[*]")
                .arg("AS")
                .arg("inflection_tags")
                .arg("TAG")
                //
                .arg("$.lemmas[*].paradigm_info[*].inflection[*].word_form")
                .arg("AS")
                .arg("inflection")
                .arg("TEXT")
                .arg("SORTABLE")
                //
                .arg("$.__ordbokapi__.hasSplitInf")
                .arg("AS")
                .arg("split_infinitive")
                .arg("TAG")
                .to_owned(),
        );
    }

    Ok(())
}

/// Migration #2:
/// - FT.ALTER idx:article SCHEMA ADD … for exact matching
/// - etc. Also apply to idx:article:bm, nn, no
fn migration_v2_1(pipe: &mut redis::Pipeline) -> Result<()> {
    // e.g. "FT.ALTER idx:article SCHEMA ADD $.lemmas[*].lemma AS lemma_exact TAG SORTABLE"
    fn alter_index_commands(pipe: &mut redis::Pipeline, index_name: &str) {
        pipe.add_command(
            redis::cmd("FT.ALTER")
                .arg(index_name)
                .arg("SCHEMA")
                .arg("ADD")
                .arg("$.lemmas[*].lemma")
                .arg("AS")
                .arg("lemma_exact")
                .arg("TAG")
                .arg("SORTABLE")
                .to_owned(),
        );

        pipe.add_command(
            redis::cmd("FT.ALTER")
                .arg(index_name)
                .arg("SCHEMA")
                .arg("ADD")
                .arg("$.suggest[*]")
                .arg("AS")
                .arg("suggest_exact")
                .arg("TAG")
                .arg("SORTABLE")
                .to_owned(),
        );

        pipe.add_command(
            redis::cmd("FT.ALTER")
                .arg(index_name)
                .arg("SCHEMA")
                .arg("ADD")
                .arg("$.lemmas[*].paradigm_info[*].inflection[*].word_form")
                .arg("AS")
                .arg("inflection_exact")
                .arg("TAG")
                .arg("SORTABLE")
                .to_owned(),
        );
    }

    alter_index_commands(pipe, "idx:article");
    for dict in ["bm", "nn", "no"] {
        let idx_name = format!("idx:article:{dict}");
        alter_index_commands(pipe, &idx_name);
    }

    Ok(())
}

/// Our migration service
pub struct MigrationService {
    pub redis_client: redis::aio::ConnectionManager,
}

impl MigrationService {
    /// Run all migrations from the current version to the latest.
    pub async fn migrate(&mut self) -> Result<()> {
        // Get current version.
        let current_version: Option<String> = self.redis_client.get(VERSION_KEY).await?;
        let current_v = current_version
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        // Build a list of all versions.
        let versions = get_versions();

        // Find which versions we still need to apply.
        let mut to_apply: Vec<&MigrationVersion> = versions
            .iter()
            .filter(|mv| mv.version > current_v)
            .collect();
        to_apply.sort_by_key(|mv| mv.version);

        if to_apply.is_empty() {
            info!("No Redis migrations needed. Current version is {current_v}");
            return Ok(());
        }

        // Apply each version in ascending order.
        for mv in to_apply {
            info!("Applying migrations for version {}", mv.version);

            // Run a MULTI/EXEC pipeline.
            let mut pipe = redis::pipe();
            pipe.atomic(); // ensures MULTI/EXEC

            // Perform each migration.
            for func in &mv.migrations {
                func(&mut pipe)?;
            }

            // Finally set the version to mv.version to mark it as applied.
            pipe.set(VERSION_KEY, mv.version);

            let _: () = pipe
                .query_async(&mut self.redis_client)
                .await
                .map_err(|e| anyhow!("Failed applying version {} migrations: {e}", mv.version))?;

            info!("Successfully migrated to version {}", mv.version);
        }

        Ok(())
    }
}
