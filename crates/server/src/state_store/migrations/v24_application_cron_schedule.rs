use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use super::{contexts::MigrationContext, migration_trait::Migration};
use crate::{
    data_model::{
        ApplicationBuilder,
        ApplicationEntryPoint,
        ApplicationState,
        DataPayload,
        Function,
    },
    state_store::{
        serializer::{StateStoreEncode, StateStoreEncoder},
        state_machine::IndexifyObjectsColumns,
    },
};

/// Legacy Application layout matching the pre-cron schema (no `cron_schedule`).
#[derive(Debug, Deserialize, Serialize)]
struct V23Application {
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
}

/// Migration to add `cron_schedule: Option<String>` to the Application struct.
///
/// Old postcard-encoded records lack this trailing field. We decode with the
/// legacy struct (without the field), then re-encode using the current
/// Application with `cron_schedule: None`.
#[derive(Clone)]
pub struct V24ApplicationCronSchedule;

#[async_trait]
impl Migration for V24ApplicationCronSchedule {
    fn version(&self) -> u64 {
        24
    }

    fn name(&self) -> &'static str {
        "Add cron_schedule to Application"
    }

    async fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        ctx.iterate(&IndexifyObjectsColumns::Applications, |key, value| {
            entries.push((key.to_vec(), value.to_vec()));
            Ok(())
        })
        .await?;

        let mut migrated: usize = 0;
        let total = entries.len();

        for (key_bytes, value_bytes) in &entries {
            let legacy: V23Application = StateStoreEncoder::decode(value_bytes)?;

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
            migrated += 1;
        }

        info!("V24 application cron_schedule field: {migrated}/{total} applications re-encoded");

        Ok(())
    }

    fn box_clone(&self) -> Box<dyn Migration> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;
    use crate::{
        data_model::{Application, ApplicationState},
        state_store::{
            driver::{Reader, Writer},
            migrations::testing::MigrationTestBuilder,
            state_machine::IndexifyObjectsColumns,
        },
    };

    #[tokio::test]
    async fn test_v24_adds_cron_schedule_field() -> Result<()> {
        let migration = V24ApplicationCronSchedule;

        // Encode an application using the V23 layout (no cron_schedule)
        let app = V23Application {
            namespace: "default".to_string(),
            name: "test_app".to_string(),
            tombstoned: false,
            description: "Test application".to_string(),
            tags: HashMap::new(),
            created_at: 1000,
            version: "v1".to_string(),
            code: None,
            functions: HashMap::new(),
            state: ApplicationState::Active,
            created_at_clock: Some(1),
            updated_at_clock: Some(1),
            entrypoint: None,
        };
        let app_bytes = StateStoreEncoder::encode(&app)?;
        let app_key = b"default|test_app";

        let mut builder = MigrationTestBuilder::new();
        for cf in IndexifyObjectsColumns::iter() {
            builder = builder.with_column_family(cf.as_ref());
        }

        builder
            .run_test(
                &migration,
                |db| {
                    Box::pin(async move {
                        db.put(
                            IndexifyObjectsColumns::Applications.as_ref(),
                            app_key,
                            &app_bytes,
                        )
                        .await?;
                        Ok(())
                    })
                },
                |db| {
                    Box::pin(async move {
                        // Verify application was migrated with cron_schedule: None
                        let result = db
                            .get(IndexifyObjectsColumns::Applications.as_ref(), app_key)
                            .await?
                            .expect("application should exist");
                        let migrated_app: Application = StateStoreEncoder::decode(&result)?;
                        assert_eq!(migrated_app.namespace, "default");
                        assert_eq!(migrated_app.name, "test_app");

                        Ok(())
                    })
                },
            )
            .await?;

        Ok(())
    }
}
