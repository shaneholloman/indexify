use std::{pin::Pin, sync::Arc, task::Poll, time::Duration};

use anyhow::anyhow;
use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, sse::Event},
};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use serde::Serialize;
use tokio::{
    sync::broadcast::{self, error::RecvError},
    time::interval,
};
use tracing::{debug, error, warn};

use super::routes_state::RouteState;
use crate::{
    data_model::{ApplicationState, RequestCtx},
    http_objects::IndexifyAPIError,
    invoke_helper,
    metrics::Increment,
    state_store::{
        IndexifyState,
        driver,
        request_events::{RequestStateChangeEvent, enrichment},
        requests::{InvokeApplicationRequest, RequestPayload, StateMachineUpdateRequest},
    },
};

/// We allow at max the length of a UUID4 with hyphens.
const MAX_REQUEST_ID_LENGTH: usize = 36;
/// Interval for checking if the request has finished when SSE events may have
/// been missed.
const REQUEST_HEALTH_CHECK_INTERVAL_SECS: u64 = 10;

struct SubscriptionGuard {
    indexify_state: Arc<IndexifyState>,
    namespace: String,
    application: String,
    request_id: String,
}

impl SubscriptionGuard {
    fn new(
        indexify_state: Arc<IndexifyState>,
        namespace: &str,
        application: &str,
        request_id: &str,
    ) -> Self {
        Self {
            indexify_state,
            namespace: namespace.to_string(),
            application: application.to_string(),
            request_id: request_id.to_string(),
        }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        let indexify_state = self.indexify_state.clone();
        let namespace = self.namespace.clone();
        let application = self.application.clone();
        let request_id = self.request_id.clone();

        tokio::spawn(async move {
            indexify_state
                .unsubscribe_request_events(&namespace, &application, &request_id)
                .await;
        });
    }
}

#[pin_project]
struct StreamWithGuard {
    #[pin]
    stream: Pin<Box<dyn Stream<Item = Result<Event, axum::Error>> + Send>>,
    _guard: SubscriptionGuard,
}

impl Stream for StreamWithGuard {
    type Item = Result<Event, axum::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

#[tracing::instrument(skip(rx, state))]
pub(crate) async fn create_request_progress_stream(
    mut rx: broadcast::Receiver<RequestStateChangeEvent>,
    state: RouteState,
    namespace: String,
    application: String,
    request_id: String,
) -> impl Stream<Item = Result<Event, axum::Error>> {
    async_stream::stream! {
        let reader = state.indexify_state.reader();
        let mut health_check_interval = interval(Duration::from_secs(REQUEST_HEALTH_CHECK_INTERVAL_SECS));

        // Check if the request is already finished before starting the stream
        match enrichment::check_for_finished(&reader, &state.blob_storage, &namespace, &application, &request_id).await {
            Ok(Some(event)) => {
                debug!("request already finished, sending event");
                yield Event::default().json_data(event);
                return;
            }
            Ok(None) => {
                debug!("request in progress, starting stream");
            }
            Err(error) => {
                error!(?error, "failed initial check, stopping stream");
                return;
            }
        }

        loop {
            tokio::select! {
                recv_result = rx.recv() => {
                    match recv_result {
                        Ok(event) if matches!(event, RequestStateChangeEvent::RequestFinished(_)) => {
                            debug!("received finished event");
                            // Enrich the event with output data if available
                            match enrichment::check_for_finished(&reader, &state.blob_storage, &namespace, &application, &request_id).await {
                                Ok(Some(finished_event)) => {
                                    yield Event::default().json_data(finished_event);
                                }
                                Ok(None) | Err(_) => {
                                    // Fall back to the original event
                                    yield Event::default().json_data(event);
                                }
                            }
                            break;
                        }
                        Ok(event) => {
                            debug!("received event: {:?}", event);
                            health_check_interval.reset();
                            yield Event::default().json_data(event);
                        }
                        Err(RecvError::Lagged(num)) => {
                            warn!("lagged behind by {} events, checking request state", num);
                            match enrichment::check_for_finished(&reader, &state.blob_storage, &namespace, &application, &request_id).await {
                                Ok(Some(event)) => {
                                    debug!("request finished during lag");
                                    yield Event::default().json_data(event);
                                    break;
                                }
                                Ok(None) => {
                                    debug!("request still in progress after lag");
                                    health_check_interval.reset();
                                }
                                Err(error) => {
                                    error!(?error, "check failed during lag, stopping stream");
                                    break;
                                }
                            }
                        }
                        Err(RecvError::Closed) => {
                            debug!("channel closed, checking final state");
                            if let Ok(Some(event)) = enrichment::check_for_finished(&reader, &state.blob_storage, &namespace, &application, &request_id).await {
                                yield Event::default().json_data(event);
                            }
                            break;
                        }
                    }
                }
                _ = health_check_interval.tick() => {
                    debug!("health check interval tick");
                    match enrichment::check_for_finished(&reader, &state.blob_storage, &namespace, &application, &request_id).await {
                        Ok(Some(event)) => {
                            debug!("request finished during health check");
                            yield Event::default().json_data(event);
                            break;
                        }
                        Ok(None) => {
                            // Request still in progress, continue waiting
                        }
                        Err(error) => {
                            error!(?error, "health check failed, stopping stream");
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Serialize)]
struct RequestIdV1 {
    request_id: String,
}

/// Make a request to application
#[utoipa::path(
    post,
    path = "/v1/namespaces/{namespace}/applications/{application}",
    request_body(content_type = "application/json", content = inline(serde_json::Value)),
    tag = "ingestion",
    responses(
        (status = 200, description = "request successful"),
        (status = 400, description = "bad request"),
        (status = INTERNAL_SERVER_ERROR, description = "internal server error")
    ),
)]
pub async fn invoke_application_with_object_v1(
    Path((namespace, application_name)): Path<(String, String)>,
    State(state): State<RouteState>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let _inc = Increment::inc(&state.metrics.requests, &[]);

    let request_id = match headers.get("Idempotency-Key").and_then(|v| v.to_str().ok()) {
        Some(id) => {
            if id.len() > MAX_REQUEST_ID_LENGTH {
                return Err(IndexifyAPIError::bad_request(&format!(
                    "Idempotency key for requests exceeds maximum length of {MAX_REQUEST_ID_LENGTH} characters"
                )));
            }
            if id.is_empty() {
                return Err(IndexifyAPIError::bad_request(
                    "Idempotency key for requests cannot be empty",
                ));
            }
            id.to_string()
        }
        None => nanoid::nanoid!(),
    };

    let accept_header = headers
        .get("Accept")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    let encoding = headers
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or("application/octet-stream".to_string());

    let application = state
        .indexify_state
        .reader()
        .get_application(&namespace, &application_name)
        .await
        .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to get application: {e}")))?
        .ok_or(
            IndexifyAPIError::not_found("application not found")
                .with_label("namespace", namespace.clone())
                .with_label("application", application_name.clone()),
        )?;

    if let ApplicationState::Disabled { reason } = &application.state {
        return Result::Err(IndexifyAPIError::conflict(reason));
    }

    if application.entrypoint.is_none() {
        return Err(IndexifyAPIError::bad_request(
            "application has no entrypoint - cannot invoke sandbox-only applications",
        ));
    }

    let payload_stream = body
        .into_data_stream()
        .map(|res| res.map_err(|err| anyhow::anyhow!(err)));

    let invocation = invoke_helper::build_invocation_request(
        &state.indexify_state,
        &state.blob_storage,
        &application,
        &request_id,
        Box::pin(payload_stream),
        &encoding,
    )
    .await
    .map_err(|e| IndexifyAPIError::internal_error(anyhow!("failed to build invocation: {e}")))?;

    let request_ctx = invocation.request_ctx;

    state
        .metrics
        .request_input_bytes
        .add(invocation.input_size, &[]);
    state.metrics.requests.add(1, &[]);

    let payload = RequestPayload::InvokeApplication(InvokeApplicationRequest {
        namespace: request_ctx.namespace.clone(),
        application_name: request_ctx.application_name.clone(),
        ctx: request_ctx.clone(),
    });
    state
        .indexify_state
        .write(StateMachineUpdateRequest { payload })
        .await
        .map_err(|e| {
            if let Some(driver_error) = e.downcast_ref::<driver::Error>() &&
                driver_error.is_request_already_exists()
            {
                IndexifyAPIError::conflict(&driver_error.to_string())
            } else {
                IndexifyAPIError::internal_error(anyhow!("failed to upload content: {e}"))
            }
        })?;

    if accept_header.contains("application/json") {
        return Ok(Json(RequestIdV1 {
            request_id: request_id.clone(),
        })
        .into_response());
    }
    if accept_header.contains("text/event-stream") {
        return return_sse_response(
            // cloning the state is cheap because all its fields are inside arcs
            state.clone(),
            request_ctx,
        )
        .await;
    }
    Err(IndexifyAPIError::bad_request(
        "accept header must be application/json or text/event-stream",
    ))
}

/// Stream progress of a request until it is completed
#[utoipa::path(
    get,
    path = "/namespaces/{namespace}/compute-graphs/{application}/requests/{request_id}/progress",
    tag = "operations",
    responses(
        (status = 200, description = "SSE events of a request"),
        (status = INTERNAL_SERVER_ERROR, description = "Internal Server Error")
    ),
)]
#[axum::debug_handler]
pub async fn progress_stream(
    Path((namespace, application, request_id)): Path<(String, String, String)>,
    State(state): State<RouteState>,
) -> Result<impl IntoResponse, IndexifyAPIError> {
    let ctx = state
        .indexify_state
        .reader()
        .request_ctx(&namespace, &application, &request_id)
        .await
        .map_err(|e| {
            IndexifyAPIError::internal_error(anyhow!("failed to get request context: {e}"))
        })?
        .ok_or(IndexifyAPIError::not_found("request not found"))?;
    return_sse_response(state, ctx).await
}

async fn return_sse_response(
    state: RouteState,
    ctx: RequestCtx,
) -> Result<axum::response::Response, IndexifyAPIError> {
    let rx = state
        .indexify_state
        .subscribe_request_events(&ctx.namespace, &ctx.application_name, &ctx.request_id)
        .await;

    let guard = SubscriptionGuard::new(
        state.indexify_state.clone(),
        &ctx.namespace,
        &ctx.application_name,
        &ctx.request_id,
    );

    let inner_stream = create_request_progress_stream(
        rx,
        state.clone(),
        ctx.namespace,
        ctx.application_name,
        ctx.request_id,
    )
    .await;

    let stream_with_guard = StreamWithGuard {
        stream: Box::pin(inner_stream),
        _guard: guard,
    };

    Ok(axum::response::Sse::new(stream_with_guard)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(Duration::from_secs(1)))
        .into_response())
}
