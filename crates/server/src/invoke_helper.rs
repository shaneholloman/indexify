use std::collections::HashMap;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use futures::Stream;

use crate::{
    blob_store::registry::BlobStorageRegistry,
    data_model::{self, Application, FunctionCallId, InputArgs, RequestCtx, RequestCtxBuilder},
    state_store::IndexifyState,
    utils::get_epoch_time_in_ms,
};

/// Result of building an invocation request.
pub struct InvocationResult {
    pub request_ctx: RequestCtx,
    /// Size of the uploaded input payload in bytes.
    pub input_size: u64,
}

/// Builds an `InvokeApplicationRequest` payload from a validated application
/// and an input payload stream.  Used by the HTTP invoke route.
///
/// The caller is responsible for:
///   1. Fetching and validating the application (exists, not tombstoned,
///      enabled, has entrypoint)
///   2. Writing the returned `RequestCtx` via `indexify_state.write()`
///   3. Handling errors in a caller-appropriate way (HTTP error vs log+skip)
pub async fn build_invocation_request(
    indexify_state: &IndexifyState,
    blob_storage: &BlobStorageRegistry,
    application: &Application,
    request_id: &str,
    payload_stream: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>,
    encoding: &str,
) -> Result<InvocationResult> {
    let namespace = &application.namespace;

    let entrypoint = application
        .entrypoint
        .as_ref()
        .ok_or_else(|| anyhow!("application has no entrypoint"))?;
    let entrypoint_fn_name = &entrypoint.function_name;
    let entrypoint_fn = application
        .functions
        .get(entrypoint_fn_name)
        .ok_or_else(|| {
            anyhow!(
                "entrypoint function '{}' not found in application",
                entrypoint_fn_name
            )
        })?;

    // Upload payload to blob store
    let payload_key = format!(
        "{}/input",
        data_model::DataPayload::request_key_prefix(namespace, &application.name, request_id)
    );

    let put_result = blob_storage
        .get_blob_store(namespace)
        .put(&payload_key, payload_stream)
        .await?;

    let input_size = put_result.size_bytes;
    let data_payload = data_model::DataPayload {
        id: request_id.to_string(),
        metadata_size: 0,
        path: put_result.url,
        size: put_result.size_bytes,
        sha256_hash: put_result.sha256_hash,
        offset: 0,
        encoding: encoding.to_string(),
    };

    // Create function call and function run
    let function_call_id = FunctionCallId(request_id.to_string());
    let fn_call = entrypoint_fn.create_function_call(
        function_call_id,
        vec![data_payload.clone()],
        Bytes::new(),
        None,
    );

    let app_version = indexify_state
        .reader()
        .get_application_version(namespace, &application.name, &application.version)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "application version not found for {}/{}@{}",
                namespace,
                application.name,
                application.version,
            )
        })?;

    let fn_run = app_version.create_function_run(
        &fn_call,
        vec![InputArgs {
            function_call_id: None,
            data_payload,
        }],
        request_id,
    )?;

    let fn_runs = HashMap::from([(fn_run.id.clone(), fn_run)]);
    let fn_calls = HashMap::from([(fn_call.function_call_id.clone(), fn_call)]);

    let request_ctx = RequestCtxBuilder::default()
        .namespace(namespace.clone())
        .application_name(application.name.clone())
        .application_version(application.version.clone())
        .request_id(request_id.to_string())
        .created_at(get_epoch_time_in_ms())
        .function_runs(fn_runs)
        .function_calls(fn_calls)
        .build()?;

    Ok(InvocationResult {
        request_ctx,
        input_size,
    })
}
