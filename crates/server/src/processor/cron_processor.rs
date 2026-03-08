use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Result;
use futures::StreamExt;
use tokio_util::time::DelayQueue;
use tracing::{error, info, warn};

use crate::{
    data_model::{ApplicationState, FunctionCallId, RequestCtxBuilder},
    state_store::{
        CronEvent,
        IndexifyState,
        driver::Writer,
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
        state_machine,
    },
    utils::get_epoch_time_in_ms,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CronScheduleKey {
    namespace: String,
    application_name: String,
}

pub struct CronProcessor {
    indexify_state: Arc<IndexifyState>,
}

impl CronProcessor {
    pub fn new(indexify_state: Arc<IndexifyState>) -> Self {
        Self { indexify_state }
    }

    pub async fn start(&self, mut shutdown_rx: tokio::sync::watch::Receiver<()>) {
        let mut cron_events_rx = self
            .indexify_state
            .cron_events_rx
            .lock()
            .unwrap()
            .take()
            .expect("cron_events_rx already taken");

        let mut delay_queue: DelayQueue<CronScheduleKey> = DelayQueue::new();
        let mut active_keys: HashMap<CronScheduleKey, (tokio_util::time::delay_queue::Key, u64)> =
            HashMap::new();

        // Full CF scan at startup to bootstrap the delay queue
        if let Err(e) = self
            .load_all_schedules(&mut delay_queue, &mut active_keys)
            .await
        {
            error!(error = %e, "failed to load initial cron schedules");
        }

        loop {
            tokio::select! {
                Some(event) = cron_events_rx.recv() => {
                    // Drain any additional pending events to batch-process
                    let mut events = vec![event];
                    while let Ok(ev) = cron_events_rx.try_recv() {
                        events.push(ev);
                    }
                    self.apply_events(events, &mut delay_queue, &mut active_keys).await;
                }
                Some(expired) = delay_queue.next() => {
                    let key: CronScheduleKey = expired.into_inner();
                    active_keys.remove(&key);

                    if let Err(e) = self.fire_cron(&key).await {
                        error!(
                            error = %e,
                            namespace = %key.namespace,
                            application = %key.application_name,
                            "failed to fire cron invocation"
                        );
                    }

                    // Re-schedule next occurrence
                    if let Err(e) = self.reschedule_after_fire(&key, &mut delay_queue, &mut active_keys).await {
                        error!(
                            error = %e,
                            namespace = %key.namespace,
                            application = %key.application_name,
                            "failed to reschedule cron after firing"
                        );
                    }
                }
                _ = shutdown_rx.changed() => {
                    info!("cron processor shutting down");
                    break;
                }
            }
        }
    }

    /// Full CF scan — only used at startup.
    async fn load_all_schedules(
        &self,
        delay_queue: &mut DelayQueue<CronScheduleKey>,
        active_keys: &mut HashMap<CronScheduleKey, (tokio_util::time::delay_queue::Key, u64)>,
    ) -> Result<()> {
        let entries = self
            .indexify_state
            .reader()
            .get_all_cron_schedules()
            .await?;

        let now_ms = get_epoch_time_in_ms();

        for entry in &entries {
            if !entry.enabled {
                continue;
            }
            let key = CronScheduleKey {
                namespace: entry.namespace.clone(),
                application_name: entry.application_name.clone(),
            };
            let delay = if entry.next_fire_time_ms <= now_ms {
                Duration::ZERO
            } else {
                Duration::from_millis(entry.next_fire_time_ms - now_ms)
            };
            let dq_key = delay_queue.insert(key.clone(), delay);
            active_keys.insert(key, (dq_key, entry.next_fire_time_ms));
        }

        info!(count = entries.len(), "loaded cron schedules at startup");
        Ok(())
    }

    /// Apply targeted cron events — point reads instead of full scan.
    async fn apply_events(
        &self,
        events: Vec<CronEvent>,
        delay_queue: &mut DelayQueue<CronScheduleKey>,
        active_keys: &mut HashMap<CronScheduleKey, (tokio_util::time::delay_queue::Key, u64)>,
    ) {
        // Deduplicate: keep only the last event per (namespace, app) pair
        let mut deduped: HashMap<(String, String), CronEvent> = HashMap::new();
        for event in events {
            let key = match &event {
                CronEvent::Upserted {
                    namespace,
                    application_name,
                } => (namespace.clone(), application_name.clone()),
                CronEvent::Removed {
                    namespace,
                    application_name,
                } => (namespace.clone(), application_name.clone()),
            };
            deduped.insert(key, event);
        }

        let now_ms = get_epoch_time_in_ms();

        for event in deduped.into_values() {
            match event {
                CronEvent::Upserted {
                    namespace,
                    application_name,
                } => {
                    match self
                        .indexify_state
                        .reader()
                        .get_cron_schedule(&namespace, &application_name)
                        .await
                    {
                        Ok(Some(entry)) if entry.enabled => {
                            let key = CronScheduleKey {
                                namespace,
                                application_name,
                            };
                            // Remove old entry if present
                            if let Some((old_dq_key, _)) = active_keys.remove(&key) {
                                delay_queue.remove(&old_dq_key);
                            }
                            let delay = if entry.next_fire_time_ms <= now_ms {
                                Duration::ZERO
                            } else {
                                Duration::from_millis(entry.next_fire_time_ms - now_ms)
                            };
                            let dq_key = delay_queue.insert(key.clone(), delay);
                            active_keys.insert(key, (dq_key, entry.next_fire_time_ms));
                        }
                        Ok(_) => {
                            // Entry doesn't exist or is disabled — remove from queue
                            let key = CronScheduleKey {
                                namespace,
                                application_name,
                            };
                            if let Some((dq_key, _)) = active_keys.remove(&key) {
                                delay_queue.remove(&dq_key);
                            }
                        }
                        Err(e) => {
                            error!(
                                error = %e,
                                namespace = %namespace,
                                application = %application_name,
                                "failed to read cron schedule for upserted app"
                            );
                        }
                    }
                }
                CronEvent::Removed {
                    namespace,
                    application_name,
                } => {
                    let key = CronScheduleKey {
                        namespace,
                        application_name,
                    };
                    if let Some((dq_key, _)) = active_keys.remove(&key) {
                        delay_queue.remove(&dq_key);
                    }
                }
            }
        }
    }

    async fn fire_cron(&self, key: &CronScheduleKey) -> Result<()> {
        let application = self
            .indexify_state
            .reader()
            .get_application(&key.namespace, &key.application_name)
            .await?;

        let Some(application) = application else {
            warn!(
                namespace = %key.namespace,
                application = %key.application_name,
                "cron: application not found, skipping"
            );
            return Ok(());
        };

        if application.tombstoned {
            return Ok(());
        }

        if let ApplicationState::Disabled { .. } = &application.state {
            return Ok(());
        }

        if application.entrypoint.is_none() {
            warn!(
                namespace = %key.namespace,
                application = %key.application_name,
                "cron: application has no entrypoint, skipping"
            );
            return Ok(());
        }

        if application.cron_schedule.is_none() {
            return Ok(());
        }

        let entrypoint = application
            .entrypoint
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("application has no entrypoint"))?;
        let entrypoint_fn = application
            .functions
            .get(&entrypoint.function_name)
            .ok_or_else(|| anyhow::anyhow!("entrypoint function not found"))?;

        let request_id = nanoid::nanoid!();

        // Cron-scheduled apps use no-arg entrypoints — no blob store write needed.
        let function_call_id = FunctionCallId(request_id.clone());
        let fn_call =
            entrypoint_fn.create_function_call(function_call_id, vec![], bytes::Bytes::new(), None);

        let app_version = self
            .indexify_state
            .reader()
            .get_application_version(&key.namespace, &application.name, &application.version)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "application version not found for {}/{}@{}",
                    key.namespace,
                    application.name,
                    application.version,
                )
            })?;

        let fn_run = app_version.create_function_run(&fn_call, vec![], &request_id)?;

        let fn_runs = HashMap::from([(fn_run.id.clone(), fn_run)]);
        let fn_calls = HashMap::from([(fn_call.function_call_id.clone(), fn_call)]);

        let request_ctx = RequestCtxBuilder::default()
            .namespace(key.namespace.clone())
            .application_name(application.name.clone())
            .application_version(application.version.clone())
            .request_id(request_id.clone())
            .created_at(get_epoch_time_in_ms())
            .function_runs(fn_runs)
            .function_calls(fn_calls)
            .build()?;

        let payload = RequestPayload::InvokeApplication(InvokeApplicationRequest {
            namespace: request_ctx.namespace.clone(),
            application_name: request_ctx.application_name.clone(),
            ctx: request_ctx,
        });

        self.indexify_state
            .write(StateMachineUpdateRequest { payload })
            .await?;

        info!(
            namespace = %key.namespace,
            application = %key.application_name,
            request_id = %request_id,
            "cron: invoked application"
        );

        Ok(())
    }

    async fn reschedule_after_fire(
        &self,
        key: &CronScheduleKey,
        delay_queue: &mut DelayQueue<CronScheduleKey>,
        active_keys: &mut HashMap<CronScheduleKey, (tokio_util::time::delay_queue::Key, u64)>,
    ) -> Result<()> {
        let now_ms = get_epoch_time_in_ms();

        let entry = self
            .indexify_state
            .reader()
            .get_cron_schedule(&key.namespace, &key.application_name)
            .await?;

        let Some(mut entry) = entry else {
            return Ok(());
        };

        let next_fire_time_ms =
            state_machine::compute_next_fire_time(&entry.cron_expression, now_ms)?;

        entry.last_fired_at_ms = Some(now_ms);
        entry.next_fire_time_ms = next_fire_time_ms;

        // Acquire write_mutex to serialize with the main write path which also
        // writes to the CronSchedules CF (via create_or_update_application).
        let _write_guard = self.indexify_state.write_mutex.lock().await;
        let txn = self.indexify_state.db.transaction();
        state_machine::put_cron_schedule_entry(&txn, &entry).await?;
        txn.commit().await?;

        let delay = Duration::from_millis(next_fire_time_ms.saturating_sub(now_ms));
        let dq_key = delay_queue.insert(key.clone(), delay);
        active_keys.insert(key.clone(), (dq_key, next_fire_time_ms));

        Ok(())
    }
}
