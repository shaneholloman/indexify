use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{
    contexts::{MigrationContext, PrepareContext},
    migration_trait::Migration,
};
use crate::{
    data_model::{
        ApplicationBuilder,
        ApplicationEntryPoint,
        ApplicationState,
        CronScheduleEntry,
        DataPayload,
        Function,
    },
    state_store::{
        driver::{Writer, rocksdb::RocksDBDriver},
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy Application layout WITH `cron_schedule` field (v24 schema).
#[derive(Debug, Deserialize, Serialize)]
struct V24Application {
    namespace: String,
    name: String,
    tombstoned: bool,
    description: String,
    #[serde(default)]
    tags: HashMap<String, String>,
    created_at: u64,
    version: String,
    #[serde(default)]
    code: Option<DataPayload>,
    #[serde(default)]
    functions: HashMap<String, Function>,
    #[serde(default)]
    state: ApplicationState,
    #[serde(default)]
    created_at_clock: Option<u64>,
    #[serde(default)]
    updated_at_clock: Option<u64>,
    #[serde(default)]
    entrypoint: Option<ApplicationEntryPoint>,
    #[serde(default)]
    cron_schedule: Option<String>,
}

/// Legacy CronScheduleEntry without `id` and `input` fields (v24 schema).
#[derive(Debug, Deserialize, Serialize)]
struct V24CronScheduleEntry {
    namespace: String,
    application_name: String,
    cron_expression: String,
    next_fire_time_ms: u64,
    last_fired_at_ms: Option<u64>,
    created_at: u64,
    enabled: bool,
}

/// Migration to decouple cron schedules from the Application struct.
///
/// 1. Re-encodes Application records without the `cron_schedule` field.
/// 2. Migrates existing CronScheduleEntry records from the old key format
///    (`namespace|app_name`) to the new format (`namespace|app_name|id`) with
///    `id` and `input` fields added.
#[derive(Clone)]
pub struct V25CronScheduleDecoupling;

#[async_trait]
impl Migration for V25CronScheduleDecoupling {
    fn version(&self) -> u64 {
        25
    }

    fn name(&self) -> &'static str {
        "Decouple cron schedules from Application"
    }

    async fn prepare(&self, ctx: &PrepareContext) -> Result<RocksDBDriver> {
        let existing_cfs = ctx.list_cfs()?;
        let cron_cf = IndexifyObjectsColumns::CronSchedules.to_string();
        if existing_cfs.contains(&cron_cf) {
            return ctx.open_db();
        }
        ctx.reopen_with_cf_operations(|db| {
            Box::pin(async move {
                info!("Creating CronSchedules column family for v25 migration");
                db.create(
                    IndexifyObjectsColumns::CronSchedules.as_ref(),
                    &Default::default(),
                )
                .await?;
                Ok(())
            })
        })
        .await
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        // Phase 1: Re-encode Application records without cron_schedule
        let mut app_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::Applications, |key, value| {
            app_entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        let mut migrated_apps: usize = 0;
        for (key_bytes, value_bytes) in &app_entries {
            let legacy: V24Application = StateStoreEncoder::decode(value_bytes)?;

            let application = ApplicationBuilder::default()
                .namespace(legacy.namespace)
                .name(legacy.name)
                .tombstoned(legacy.tombstoned)
                .description(legacy.description)
                .tags(legacy.tags)
                .created_at(legacy.created_at)
                .version(legacy.version)
                .code(legacy.code)
                .functions(legacy.functions)
                .state(legacy.state)
                .created_at_clock(legacy.created_at_clock)
                .updated_at_clock(legacy.updated_at_clock)
                .entrypoint(legacy.entrypoint)
                .build()
                .expect("all fields provided");

            let encoded = StateStoreEncoder::encode(&application)?;
            ctx.txn
                .put(
                    IndexifyObjectsColumns::Applications.as_ref(),
                    key_bytes,
                    &encoded,
                )
                .await?;
            migrated_apps += 1;
        }

        // Phase 2: Migrate CronScheduleEntry records to new key format
        let mut cron_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::CronSchedules, |key, value| {
            cron_entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        let mut migrated_crons: usize = 0;
        for (old_key_bytes, value_bytes) in &cron_entries {
            let legacy: V24CronScheduleEntry = StateStoreEncoder::decode(value_bytes)?;

            let id = nanoid::nanoid!();
            let new_entry = CronScheduleEntry {
                id: id.clone(),
                namespace: legacy.namespace,
                application_name: legacy.application_name,
                cron_expression: legacy.cron_expression,
                input: None,
                next_fire_time_ms: legacy.next_fire_time_ms,
                last_fired_at_ms: legacy.last_fired_at_ms,
                created_at: legacy.created_at,
                enabled: legacy.enabled,
            };

            // Delete old key
            ctx.txn
                .delete(
                    IndexifyObjectsColumns::CronSchedules.as_ref(),
                    old_key_bytes,
                )
                .await?;

            // Write with new key
            let encoded = StateStoreEncoder::encode(&new_entry)?;
            ctx.txn
                .put(
                    IndexifyObjectsColumns::CronSchedules.as_ref(),
                    new_entry.key().as_bytes(),
                    &encoded,
                )
                .await?;
            migrated_crons += 1;
        }

        info!(
            "V25 cron schedule decoupling: {migrated_apps} applications re-encoded, \
             {migrated_crons} cron schedules migrated"
        );

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}
