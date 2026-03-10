#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        data_model::{
            CronScheduleEntry,
            test_objects::tests::{self as test_objects, mock_data_payload, TEST_NAMESPACE},
        },
        state_store::{
            requests::{
                CreateCronScheduleRequest,
                CreateOrUpdateApplicationRequest,
                DeleteApplicationRequest,
                DeleteCronScheduleRequest,
                RequestPayload,
                StateMachineUpdateRequest,
            },
            state_machine::compute_next_fire_time,
            test_state_store,
        },
        testing::TestService,
    };

    #[test]
    fn test_cron_schedule_entry_key_format() {
        let entry = CronScheduleEntry {
            id: "sched1".to_string(),
            namespace: "ns1".to_string(),
            application_name: "my_app".to_string(),
            cron_expression: "* * * * *".to_string(),
            input: None,
            next_fire_time_ms: 1000,
            last_fired_at_ms: None,
            created_at: 0,
            enabled: true,
        };
        assert_eq!(entry.key(), "ns1|my_app|sched1");
        assert_eq!(
            CronScheduleEntry::key_from_id("ns1", "my_app", "sched1"),
            "ns1|my_app|sched1"
        );
        assert_eq!(
            CronScheduleEntry::key_prefix_for_app("ns1", "my_app"),
            "ns1|my_app|"
        );
    }

    #[test]
    fn test_cron_schedule_entry_serialization_roundtrip() {
        use crate::state_store::serializer::{StateStoreEncode, StateStoreEncoder};

        let entry = CronScheduleEntry {
            id: "test_id".to_string(),
            namespace: "default".to_string(),
            application_name: "test_app".to_string(),
            cron_expression: "*/5 * * * *".to_string(),
            input: Some(mock_data_payload()),
            next_fire_time_ms: 1700000000000,
            last_fired_at_ms: Some(1699999700000),
            created_at: 42,
            enabled: true,
        };
        let encoded = StateStoreEncoder::encode(&entry).unwrap();
        let decoded: CronScheduleEntry = StateStoreEncoder::decode(&encoded).unwrap();
        assert_eq!(decoded.id, entry.id);
        assert_eq!(decoded.namespace, entry.namespace);
        assert_eq!(decoded.application_name, entry.application_name);
        assert_eq!(decoded.cron_expression, entry.cron_expression);
        assert!(decoded.input.is_some());
        assert_eq!(decoded.input.unwrap().size, entry.input.unwrap().size);
        assert_eq!(decoded.next_fire_time_ms, entry.next_fire_time_ms);
        assert_eq!(decoded.last_fired_at_ms, entry.last_fired_at_ms);
        assert_eq!(decoded.created_at, entry.created_at);
        assert_eq!(decoded.enabled, entry.enabled);
    }

    #[test]
    fn test_compute_next_fire_time_every_minute() {
        let from_ms: u64 = 1700000000000;
        let next = compute_next_fire_time("* * * * *", from_ms).unwrap();
        assert!(next > from_ms, "next fire time should be in the future");
        assert!(next - from_ms <= 60_000);
    }

    #[test]
    fn test_compute_next_fire_time_every_5_minutes() {
        let from_ms: u64 = 1700000000000;
        let next = compute_next_fire_time("*/5 * * * *", from_ms).unwrap();
        assert!(next > from_ms);
        assert!(next - from_ms <= 5 * 60_000);
    }

    #[test]
    fn test_compute_next_fire_time_invalid_expression() {
        let result = compute_next_fire_time("not a cron", 1700000000000);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_cron_schedule() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially no cron schedules
        let schedules = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(schedules.len(), 0);

        // Create application first (without cron)
        let app = test_objects::mock_application();
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app.clone(),
                        upgrade_requests_to_current_version: false,
                        container_pools: vec![],
                    },
                )),
            })
            .await?;

        // Create cron schedule
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                    id: nanoid::nanoid!(),
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: app.name.clone(),
                    cron_expression: "*/5 * * * *".to_string(),
                    input: Some(mock_data_payload()),
                }),
            })
            .await?;

        // Verify schedule was created
        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].namespace, TEST_NAMESPACE);
        assert_eq!(all[0].application_name, app.name);
        assert_eq!(all[0].cron_expression, "*/5 * * * *");
        assert!(all[0].input.is_some());
        assert!(all[0].enabled);
        assert!(all[0].next_fire_time_ms > 0);
        assert!(all[0].last_fired_at_ms.is_none());
        assert!(!all[0].id.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_cron_schedule() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create app and schedule
        let app = test_objects::mock_application();
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app.clone(),
                        upgrade_requests_to_current_version: false,
                        container_pools: vec![],
                    },
                )),
            })
            .await?;

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                    id: nanoid::nanoid!(),
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: app.name.clone(),
                    cron_expression: "* * * * *".to_string(),
                    input: None,
                }),
            })
            .await?;

        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 1);
        let schedule_id = all[0].id.clone();

        // Delete the schedule
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteCronSchedule(DeleteCronScheduleRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: app.name.clone(),
                    schedule_id,
                }),
            })
            .await?;

        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_cron_schedules_per_app() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create app
        let app = test_objects::mock_application();
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app.clone(),
                        upgrade_requests_to_current_version: false,
                        container_pools: vec![],
                    },
                )),
            })
            .await?;

        // Create two schedules for the same app
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                    id: nanoid::nanoid!(),
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: app.name.clone(),
                    cron_expression: "* * * * *".to_string(),
                    input: None,
                }),
            })
            .await?;

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                    id: nanoid::nanoid!(),
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: app.name.clone(),
                    cron_expression: "0 * * * *".to_string(),
                    input: Some(mock_data_payload()),
                }),
            })
            .await?;

        let schedules = indexify_state
            .reader()
            .get_cron_schedules_for_app(TEST_NAMESPACE, &app.name)
            .await?;
        assert_eq!(schedules.len(), 2);

        // Delete one
        let to_delete = schedules[0].id.clone();
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::DeleteCronSchedule(DeleteCronScheduleRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: app.name.clone(),
                    schedule_id: to_delete,
                }),
            })
            .await?;

        let schedules = indexify_state
            .reader()
            .get_cron_schedules_for_app(TEST_NAMESPACE, &app.name)
            .await?;
        assert_eq!(schedules.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_app_removes_all_cron_schedules() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create app with two schedules
        let app = test_objects::mock_application();
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app.clone(),
                        upgrade_requests_to_current_version: false,
                        container_pools: vec![],
                    },
                )),
            })
            .await?;

        for expr in ["* * * * *", "0 * * * *"] {
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                        id: nanoid::nanoid!(),
                        namespace: TEST_NAMESPACE.to_string(),
                        application_name: app.name.clone(),
                        cron_expression: expr.to_string(),
                        input: None,
                    }),
                })
                .await?;
        }

        assert_eq!(
            indexify_state
                .reader()
                .get_all_cron_schedules()
                .await?
                .len(),
            2
        );

        // Tombstone the app
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: app.name.clone(),
                }),
            })
            .await?;
        test_srv.process_all_state_changes().await?;

        // All schedules should be removed
        assert_eq!(
            indexify_state
                .reader()
                .get_all_cron_schedules()
                .await?
                .len(),
            0,
            "all cron schedules should be removed after app deletion"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cron_channel_fires_on_create_schedule() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();
        let mut cron_rx = indexify_state
            .cron_events_rx
            .lock()
            .unwrap()
            .take()
            .unwrap();

        // Create app
        test_state_store::create_or_update_application(&indexify_state, "cron_test_app", 0).await;

        // No cron event from app creation
        assert!(cron_rx.try_recv().is_err());

        // Create cron schedule
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                    id: nanoid::nanoid!(),
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: "cron_test_app".to_string(),
                    cron_expression: "* * * * *".to_string(),
                    input: None,
                }),
            })
            .await?;

        // The cron channel should have received an Upserted event
        let event = cron_rx.try_recv();
        assert!(
            event.is_ok(),
            "cron channel should fire on CreateCronSchedule"
        );
        assert!(
            matches!(
                event.unwrap(),
                crate::state_store::CronEvent::Upserted { .. }
            ),
            "event should be Upserted"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_deploy_app_without_cron_no_event() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();
        let mut cron_rx = indexify_state
            .cron_events_rx
            .lock()
            .unwrap()
            .take()
            .unwrap();

        // Create app WITHOUT cron
        test_state_store::create_or_update_application(&indexify_state, "no_cron_test", 0).await;

        // The cron channel should NOT have received any event
        assert!(
            cron_rx.try_recv().is_err(),
            "cron channel should not fire for apps without cron_schedule"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_list_schedules_for_app() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create two apps with schedules
        let app1 = test_state_store::mock_application("cron_app_1", "1");
        let app2 = test_state_store::mock_application("cron_app_2", "1");

        for app in [&app1, &app2] {
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                        CreateOrUpdateApplicationRequest {
                            namespace: TEST_NAMESPACE.to_string(),
                            application: app.clone(),
                            upgrade_requests_to_current_version: false,
                            container_pools: vec![],
                        },
                    )),
                })
                .await?;
        }

        // Add 2 schedules to app1, 1 to app2
        for _ in 0..2 {
            indexify_state
                .write(StateMachineUpdateRequest {
                    payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                        id: nanoid::nanoid!(),
                        namespace: TEST_NAMESPACE.to_string(),
                        application_name: "cron_app_1".to_string(),
                        cron_expression: "* * * * *".to_string(),
                        input: None,
                    }),
                })
                .await?;
        }

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                    id: nanoid::nanoid!(),
                    namespace: TEST_NAMESPACE.to_string(),
                    application_name: "cron_app_2".to_string(),
                    cron_expression: "0 * * * *".to_string(),
                    input: None,
                }),
            })
            .await?;

        // get_cron_schedules_for_app should return only the app's schedules
        let app1_schedules = indexify_state
            .reader()
            .get_cron_schedules_for_app(TEST_NAMESPACE, "cron_app_1")
            .await?;
        assert_eq!(app1_schedules.len(), 2);

        let app2_schedules = indexify_state
            .reader()
            .get_cron_schedules_for_app(TEST_NAMESPACE, "cron_app_2")
            .await?;
        assert_eq!(app2_schedules.len(), 1);
        assert_eq!(app2_schedules[0].cron_expression, "0 * * * *");

        // Total should be 3
        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 3);

        Ok(())
    }
}
