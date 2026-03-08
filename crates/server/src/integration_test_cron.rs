#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        data_model::{
            CronScheduleEntry,
            test_objects::tests::{self as test_objects, TEST_NAMESPACE},
        },
        state_store::{
            requests::{
                CreateOrUpdateApplicationRequest,
                DeleteApplicationRequest,
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
            namespace: "ns1".to_string(),
            application_name: "my_app".to_string(),
            cron_expression: "* * * * *".to_string(),
            next_fire_time_ms: 1000,
            last_fired_at_ms: None,
            created_at: 0,
            enabled: true,
        };
        assert_eq!(entry.key(), "ns1|my_app");
        assert_eq!(CronScheduleEntry::key_from("ns1", "my_app"), "ns1|my_app");
    }

    #[test]
    fn test_cron_schedule_entry_serialization_roundtrip() {
        use crate::state_store::serializer::{StateStoreEncode, StateStoreEncoder};

        let entry = CronScheduleEntry {
            namespace: "default".to_string(),
            application_name: "test_app".to_string(),
            cron_expression: "*/5 * * * *".to_string(),
            next_fire_time_ms: 1700000000000,
            last_fired_at_ms: Some(1699999700000),
            created_at: 42,
            enabled: true,
        };
        let encoded = StateStoreEncoder::encode(&entry).unwrap();
        let decoded: CronScheduleEntry = StateStoreEncoder::decode(&encoded).unwrap();
        assert_eq!(decoded.namespace, entry.namespace);
        assert_eq!(decoded.application_name, entry.application_name);
        assert_eq!(decoded.cron_expression, entry.cron_expression);
        assert_eq!(decoded.next_fire_time_ms, entry.next_fire_time_ms);
        assert_eq!(decoded.last_fired_at_ms, entry.last_fired_at_ms);
        assert_eq!(decoded.created_at, entry.created_at);
        assert_eq!(decoded.enabled, entry.enabled);
    }

    #[test]
    fn test_compute_next_fire_time_every_minute() {
        // "* * * * *" = every minute
        // From epoch 0, the next occurrence should be at 60000ms (1 minute)
        let from_ms: u64 = 1700000000000; // some reference time
        let next = compute_next_fire_time("* * * * *", from_ms).unwrap();
        assert!(next > from_ms, "next fire time should be in the future");
        // Should be within 60 seconds
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
    async fn test_deploy_app_with_cron_creates_cf_entry() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Initially no cron schedules
        let schedules = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(schedules.len(), 0);

        // Create application with cron_schedule
        let mut app = test_objects::mock_application();
        app.cron_schedule = Some("*/5 * * * *".to_string());

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

        // Verify CF entry was created
        let schedule = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, &app.name)
            .await?;
        assert!(schedule.is_some(), "cron schedule CF entry should exist");
        let schedule = schedule.unwrap();
        assert_eq!(schedule.namespace, TEST_NAMESPACE);
        assert_eq!(schedule.application_name, app.name);
        assert_eq!(schedule.cron_expression, "*/5 * * * *");
        assert!(schedule.enabled);
        assert!(schedule.next_fire_time_ms > 0);
        assert!(schedule.last_fired_at_ms.is_none());

        // Verify get_all_cron_schedules returns it
        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_deploy_app_without_cron_has_no_cf_entry() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create application without cron_schedule (default)
        test_state_store::create_or_update_application(&indexify_state, "no_cron_app", 0).await;

        let schedule = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, "no_cron_app")
            .await?;
        assert!(schedule.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_update_app_to_remove_cron_deletes_cf_entry() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create app with cron
        let mut app = test_objects::mock_application();
        app.cron_schedule = Some("* * * * *".to_string());

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

        // Verify it exists
        let schedule = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, &app.name)
            .await?;
        assert!(schedule.is_some());

        // Update app to remove cron
        app.cron_schedule = None;
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

        // Verify CF entry is deleted
        let schedule = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, &app.name)
            .await?;
        assert!(schedule.is_none(), "cron schedule should be removed");

        Ok(())
    }

    #[tokio::test]
    async fn test_update_cron_expression_updates_cf_entry() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        let mut app = test_objects::mock_application();
        app.cron_schedule = Some("* * * * *".to_string());

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

        let schedule1 = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, &app.name)
            .await?
            .unwrap();
        assert_eq!(schedule1.cron_expression, "* * * * *");

        // Update to a different cron expression
        app.cron_schedule = Some("0 */2 * * *".to_string());
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

        let schedule2 = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, &app.name)
            .await?
            .unwrap();
        assert_eq!(schedule2.cron_expression, "0 */2 * * *");
        // next_fire_time should have changed since the expression is different
        assert_ne!(schedule1.next_fire_time_ms, schedule2.next_fire_time_ms);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_app_removes_cron_cf_entry() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create app with cron
        let mut app = test_objects::mock_application();
        app.cron_schedule = Some("*/10 * * * *".to_string());

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

        // Verify it exists
        assert!(
            indexify_state
                .reader()
                .get_cron_schedule(TEST_NAMESPACE, &app.name)
                .await?
                .is_some()
        );

        // Tombstone the app (goes through scheduler which issues
        // DeleteApplicationRequest)
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::TombstoneApplication(DeleteApplicationRequest {
                    namespace: TEST_NAMESPACE.to_string(),
                    name: app.name.clone(),
                }),
            })
            .await?;
        // Process the tombstone -> scheduler generates DeleteApplicationRequest
        test_srv.process_all_state_changes().await?;

        // Verify CF entry is removed
        let schedule = indexify_state
            .reader()
            .get_cron_schedule(TEST_NAMESPACE, &app.name)
            .await?;
        assert!(
            schedule.is_none(),
            "cron schedule should be removed after app deletion"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_cron_channel_fires_on_app_create() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();
        let mut cron_rx = indexify_state
            .cron_events_rx
            .lock()
            .unwrap()
            .take()
            .unwrap();

        // Create app with cron
        let mut app = test_objects::mock_application();
        app.cron_schedule = Some("* * * * *".to_string());

        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app,
                        upgrade_requests_to_current_version: false,
                        container_pools: vec![],
                    },
                )),
            })
            .await?;

        // The cron channel should have received an Upserted event
        let event = cron_rx.try_recv();
        assert!(
            event.is_ok(),
            "cron channel should fire on CreateOrUpdateApplication with cron"
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
    async fn test_cron_channel_does_not_fire_without_cron() -> Result<()> {
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
    async fn test_multiple_apps_with_cron_schedules() -> Result<()> {
        let test_srv = TestService::new().await?;
        let indexify_state = test_srv.service.indexify_state.clone();

        // Create two apps with different cron schedules
        let mut app1 = test_state_store::mock_application("cron_app_1", "1");
        app1.cron_schedule = Some("* * * * *".to_string());

        let mut app2 = test_state_store::mock_application("cron_app_2", "1");
        app2.cron_schedule = Some("0 * * * *".to_string());

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

        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 2);

        // Delete one
        app1.cron_schedule = None;
        indexify_state
            .write(StateMachineUpdateRequest {
                payload: RequestPayload::CreateOrUpdateApplication(Box::new(
                    CreateOrUpdateApplicationRequest {
                        namespace: TEST_NAMESPACE.to_string(),
                        application: app1,
                        upgrade_requests_to_current_version: false,
                        container_pools: vec![],
                    },
                )),
            })
            .await?;

        let all = indexify_state.reader().get_all_cron_schedules().await?;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].application_name, "cron_app_2");

        Ok(())
    }
}
