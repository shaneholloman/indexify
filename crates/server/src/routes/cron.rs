use anyhow::anyhow;
use axum::{
    Json,
    extract::{Path, State},
};
use base64::prelude::*;
use bytes::Bytes;
use chrono::Utc;
use futures::stream;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    data_model,
    http_objects::IndexifyAPIError,
    routes::routes_state::RouteState,
    state_store::requests::{
        CreateCronScheduleRequest,
        DeleteCronScheduleRequest,
        RequestPayload,
        StateMachineUpdateRequest,
    },
};

/// Maximum decoded input payload size (1 MiB).
const MAX_INPUT_SIZE: usize = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateCronScheduleHttpRequest {
    pub cron_expression: String,
    /// Optional base64-encoded input payload to pass to the application
    /// entrypoint on each cron invocation. Maximum 1 MiB decoded.
    #[serde(default)]
    pub input_base64: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateCronScheduleResponse {
    pub schedule_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CronScheduleInfo {
    pub id: String,
    pub application_name: String,
    pub cron_expression: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_payload: Option<data_model::DataPayload>,
    pub next_fire_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fired_at_ms: Option<u64>,
    pub created_at: u64,
    pub enabled: bool,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ListCronSchedulesResponse {
    pub schedules: Vec<CronScheduleInfo>,
}

/// Create a cron schedule for an application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}/cron-schedules",
    tag = "operations",
    request_body = CreateCronScheduleHttpRequest,
    responses(
        (status = 200, description = "cron schedule created", body = CreateCronScheduleResponse),
        (status = BAD_REQUEST, description = "invalid cron expression or limit exceeded"),
        (status = NOT_FOUND, description = "application not found"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn create_cron_schedule(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
    Json(body): Json<CreateCronScheduleHttpRequest>,
) -> Result<Json<CreateCronScheduleResponse>, IndexifyAPIError> {
    // Validate cron expression early
    let cron = croner::Cron::new(&body.cron_expression)
        .parse()
        .map_err(|e| {
            IndexifyAPIError::bad_request(&format!(
                "Invalid cron_expression '{}': {e}",
                body.cron_expression
            ))
        })?;

    // Enforce minimum interval between fires
    let min_interval_secs = state.config.cron.min_interval_secs;
    if min_interval_secs > 0 {
        let now = Utc::now();
        // Find two consecutive occurrences and check the gap
        if let Ok(first) = cron.find_next_occurrence(&now, false) &&
            let Ok(second) = cron.find_next_occurrence(&first, false)
        {
            let gap_secs = (second - first).num_seconds().unsigned_abs();
            if gap_secs < min_interval_secs {
                return Err(IndexifyAPIError::bad_request(&format!(
                    "Cron interval too short: {}s between fires, minimum is {}s",
                    gap_secs, min_interval_secs
                )));
            }
        }
    }

    // Verify the application exists
    let app = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    if app.is_none() {
        return Err(IndexifyAPIError::not_found("Application not found")
            .with_label("namespace", namespace)
            .with_label("application", application));
    }

    // Enforce per-app schedule limit
    let max_schedules = state.config.cron.max_schedules_per_app;
    let existing = state
        .indexify_state
        .reader()
        .get_cron_schedules_for_app(&namespace, &application)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    if existing.len() >= max_schedules as usize {
        return Err(IndexifyAPIError::bad_request(&format!(
            "Maximum number of cron schedules per application reached (limit: {})",
            max_schedules
        )));
    }

    let schedule_id = nanoid::nanoid!();

    // Decode and upload input to blob storage if provided
    let input_payload = if let Some(ref b64) = body.input_base64 {
        let decoded = BASE64_STANDARD
            .decode(b64)
            .map_err(|e| IndexifyAPIError::bad_request(&format!("Invalid base64 input: {e}")))?;
        if decoded.len() > MAX_INPUT_SIZE {
            return Err(IndexifyAPIError::bad_request(&format!(
                "Input payload too large: {} bytes, maximum is {} bytes",
                decoded.len(),
                MAX_INPUT_SIZE
            )));
        }

        let blob_key = format!("{namespace}/{application}/cron-schedules/{schedule_id}/input");
        let data_stream =
            stream::once(async move { Ok::<Bytes, anyhow::Error>(Bytes::from(decoded)) });
        let put_result = state
            .blob_storage
            .get_blob_store(&namespace)
            .put(&blob_key, Box::pin(data_stream))
            .await
            .map_err(|e| {
                IndexifyAPIError::internal_error(anyhow!("failed to upload cron input: {e}"))
            })?;

        Some(data_model::DataPayload {
            id: schedule_id.clone(),
            path: put_result.url,
            metadata_size: 0,
            offset: 0,
            size: put_result.size_bytes,
            sha256_hash: put_result.sha256_hash,
            encoding: "application/octet-stream".to_string(),
        })
    } else {
        None
    };

    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::CreateCronSchedule(CreateCronScheduleRequest {
                id: schedule_id.clone(),
                namespace,
                application_name: application,
                cron_expression: body.cron_expression,
                input: input_payload,
            }),
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    Ok(Json(CreateCronScheduleResponse { schedule_id }))
}

/// List cron schedules for an application
#[utoipa::path(
    get,
    path = "/v1/namespaces/{namespace}/applications/{application}/cron-schedules",
    tag = "operations",
    responses(
        (status = 200, description = "list of cron schedules", body = ListCronSchedulesResponse),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn list_cron_schedules(
    Path((namespace, application)): Path<(String, String)>,
    State(state): State<RouteState>,
) -> Result<Json<ListCronSchedulesResponse>, IndexifyAPIError> {
    let entries = state
        .indexify_state
        .reader()
        .get_cron_schedules_for_app(&namespace, &application)
        .await
        .map_err(IndexifyAPIError::internal_error)?;

    let schedules = entries
        .into_iter()
        .map(|e| CronScheduleInfo {
            id: e.id,
            application_name: e.application_name,
            cron_expression: e.cron_expression,
            input_payload: e.input,
            next_fire_time_ms: e.next_fire_time_ms,
            last_fired_at_ms: e.last_fired_at_ms,
            created_at: e.created_at,
            enabled: e.enabled,
        })
        .collect();

    Ok(Json(ListCronSchedulesResponse { schedules }))
}

/// Delete a cron schedule
#[utoipa::path(
    delete,
    path = "/v1/namespaces/{namespace}/applications/{application}/cron-schedules/{schedule_id}",
    tag = "operations",
    responses(
        (status = 200, description = "cron schedule deleted"),
        (status = BAD_REQUEST, description = "schedule not found"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn delete_cron_schedule(
    Path((namespace, application, schedule_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<(), IndexifyAPIError> {
    // Verify the schedule exists before attempting delete
    let existing = state
        .indexify_state
        .reader()
        .get_cron_schedule(&namespace, &application, &schedule_id)
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    if existing.is_none() {
        return Err(IndexifyAPIError::bad_request(&format!(
            "Cron schedule '{}' not found for application '{}/{}'",
            schedule_id, namespace, application
        )));
    }

    state
        .indexify_state
        .write(StateMachineUpdateRequest {
            payload: RequestPayload::DeleteCronSchedule(DeleteCronScheduleRequest {
                namespace,
                application_name: application,
                schedule_id,
            }),
        })
        .await
        .map_err(IndexifyAPIError::internal_error)?;
    Ok(())
}
