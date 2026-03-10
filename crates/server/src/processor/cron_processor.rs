use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use futures::StreamExt;
use tokio_util::time::DelayQueue;
use tracing::{error, info, warn};

use crate::{
    data_model::{ApplicationState, FunctionCallId, InputArgs, RequestCtxBuilder},
    state_store::{
        CronEvent,
        IndexifyState,
        requests::{
            AdvanceCronScheduleRequest,
            InvokeApplicationRequest,
            RequestPayload,
            StateMachineUpdateRequest,
        },
        state_machine,
    },
    utils::get_epoch_time_in_ms,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CronScheduleKey {
    namespace: String,
    application_name: String,
    schedule_id: String,
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

                    if let Err(e) = self.fire_and_reschedule(&key, &mut delay_queue, &mut active_keys).await {
                        error!(
                            error = %e,
                            namespace = %key.namespace,
                            application = %key.application_name,
                            schedule_id = %key.schedule_id,
                            "failed to fire cron invocation"
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
                schedule_id: entry.id.clone(),
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
        // Deduplicate: keep only the last event per schedule.
        // AllRemoved supersedes any per-schedule events for the same app.
        let mut all_removed_apps: HashSet<(String, String)> = HashSet::new();
        let mut deduped: HashMap<(String, String, String), CronEvent> = HashMap::new();

        for event in events {
            match &event {
                CronEvent::AllRemoved {
                    namespace,
                    application_name,
                } => {
                    all_removed_apps.insert((namespace.clone(), application_name.clone()));
                    // Remove any per-schedule events for this app
                    deduped.retain(|k, _| !(k.0 == *namespace && k.1 == *application_name));
                }
                CronEvent::Upserted {
                    namespace,
                    application_name,
                    schedule_id,
                } => {
                    if !all_removed_apps.contains(&(namespace.clone(), application_name.clone())) {
                        deduped.insert(
                            (
                                namespace.clone(),
                                application_name.clone(),
                                schedule_id.clone(),
                            ),
                            event,
                        );
                    }
                }
                CronEvent::Removed {
                    namespace,
                    application_name,
                    schedule_id,
                } => {
                    if !all_removed_apps.contains(&(namespace.clone(), application_name.clone())) {
                        deduped.insert(
                            (
                                namespace.clone(),
                                application_name.clone(),
                                schedule_id.clone(),
                            ),
                            event,
                        );
                    }
                }
            }
        }

        // Handle AllRemoved: remove all active keys for matching apps
        for (namespace, application_name) in &all_removed_apps {
            let keys_to_remove: Vec<CronScheduleKey> = active_keys
                .keys()
                .filter(|k| k.namespace == *namespace && k.application_name == *application_name)
                .cloned()
                .collect();
            for key in keys_to_remove {
                if let Some((dq_key, _)) = active_keys.remove(&key) {
                    delay_queue.remove(&dq_key);
                }
            }
        }

        let now_ms = get_epoch_time_in_ms();

        for event in deduped.into_values() {
            match event {
                CronEvent::Upserted {
                    namespace,
                    application_name,
                    schedule_id,
                } => {
                    match self
                        .indexify_state
                        .reader()
                        .get_cron_schedule(&namespace, &application_name, &schedule_id)
                        .await
                    {
                        Ok(Some(entry)) if entry.enabled => {
                            let key = CronScheduleKey {
                                namespace,
                                application_name,
                                schedule_id,
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
                                schedule_id,
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
                                schedule_id = %schedule_id,
                                "failed to read cron schedule for upserted event"
                            );
                        }
                    }
                }
                CronEvent::Removed {
                    namespace,
                    application_name,
                    schedule_id,
                } => {
                    let key = CronScheduleKey {
                        namespace,
                        application_name,
                        schedule_id,
                    };
                    if let Some((dq_key, _)) = active_keys.remove(&key) {
                        delay_queue.remove(&dq_key);
                    }
                }
                CronEvent::AllRemoved { .. } => {
                    // Already handled above
                }
            }
        }
    }

    /// Atomically advance the schedule to the next fire time, fire the
    /// invocation, and re-insert into the delay queue.
    ///
    /// The schedule is advanced BEFORE the invocation is written so that a
    /// crash between the two can only cause a missed fire, never a
    /// double-fire.
    async fn fire_and_reschedule(
        &self,
        key: &CronScheduleKey,
        delay_queue: &mut DelayQueue<CronScheduleKey>,
        active_keys: &mut HashMap<CronScheduleKey, (tokio_util::time::delay_queue::Key, u64)>,
    ) -> Result<()> {
        let now_ms = get_epoch_time_in_ms();

        // Read the schedule entry
        let entry = self
            .indexify_state
            .reader()
            .get_cron_schedule(&key.namespace, &key.application_name, &key.schedule_id)
            .await?;

        let Some(entry) = entry else {
            warn!(
                namespace = %key.namespace,
                application = %key.application_name,
                schedule_id = %key.schedule_id,
                "cron: schedule entry not found, skipping"
            );
            return Ok(());
        };

        // Advance the schedule in RocksDB FIRST via the standard write() path.
        // This ensures that if we crash after this write but before firing,
        // the schedule won't double-fire on restart.
        let next_fire_time_ms =
            state_machine::compute_next_fire_time(&entry.cron_expression, now_ms)?;

        self.indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::AdvanceCronSchedule(AdvanceCronScheduleRequest {
                    namespace: key.namespace.clone(),
                    application_name: key.application_name.clone(),
                    schedule_id: key.schedule_id.clone(),
                    cron_expression: entry.cron_expression.clone(),
                    fired_at_ms: now_ms,
                }),
            })
            .await?;

        // Re-insert into delay queue immediately so the next fire is scheduled
        // even if the invocation below fails.
        let delay = Duration::from_millis(next_fire_time_ms.saturating_sub(now_ms));
        let dq_key = delay_queue.insert(key.clone(), delay);
        active_keys.insert(key.clone(), (dq_key, next_fire_time_ms));

        // Now fire the invocation. If this fails the schedule is already
        // advanced so we won't retry — we just log the error and move on.
        if let Err(e) = self.fire_cron(key, &entry).await {
            error!(
                error = %e,
                namespace = %key.namespace,
                application = %key.application_name,
                schedule_id = %key.schedule_id,
                "cron: failed to create invocation (schedule already advanced)"
            );
        }

        Ok(())
    }

    async fn fire_cron(
        &self,
        key: &CronScheduleKey,
        entry: &crate::data_model::CronScheduleEntry,
    ) -> Result<()> {
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

        let entrypoint = match &application.entrypoint {
            Some(ep) => ep,
            None => {
                warn!(
                    namespace = %key.namespace,
                    application = %key.application_name,
                    "cron: application has no entrypoint, skipping"
                );
                return Ok(());
            }
        };
        let entrypoint_fn = application
            .functions
            .get(&entrypoint.function_name)
            .ok_or_else(|| anyhow::anyhow!("entrypoint function not found"))?;

        let request_id = nanoid::nanoid!();
        let function_call_id = FunctionCallId(request_id.clone());

        // Build inputs from stored DataPayload (matches the invoke route pattern)
        let (inputs, input_args) = if let Some(ref data_payload) = entry.input {
            (
                vec![data_payload.clone()],
                vec![InputArgs {
                    function_call_id: None,
                    data_payload: data_payload.clone(),
                }],
            )
        } else {
            (vec![], vec![])
        };

        let fn_call =
            entrypoint_fn.create_function_call(function_call_id, inputs, bytes::Bytes::new(), None);

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

        let fn_run = app_version.create_function_run(&fn_call, input_args, &request_id)?;

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
            schedule_id = %key.schedule_id,
            request_id = %request_id,
            "cron: invoked application"
        );

        Ok(())
    }
}
