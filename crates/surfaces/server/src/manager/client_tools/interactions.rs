//! Private interaction writes. Plaintext never enters durable records or events.
use super::*;
use std::collections::BTreeSet;

impl ClientTools {
    pub(crate) async fn install_secret_state(
        &self,
        session: &microcrab_session::Session,
    ) -> Result<(), Error> {
        let session_id = session.process_control().session_id().to_owned();
        identifier(&session_id)?;
        let handle = session.state_handle();
        let runtime_id = handle.inspection()?.witness.runtime_instance_id;
        let mut state = self.state.lock().await;
        if state.secret_states.len() >= 128 && !state.secret_states.contains_key(&session_id) {
            return Err(invalid(
                "private interaction target retention limit reached",
            ));
        }
        state.secret_states.insert(session_id, (runtime_id, handle));
        Ok(())
    }
    pub async fn private_arguments(
        &self,
        socket: &str,
        invocation: &str,
        lease: &str,
    ) -> Result<Value, Error> {
        let state = self.state.lock().await;
        if !state
            .lease
            .as_ref()
            .is_some_and(|current| current.socket == socket && current.id == lease)
        {
            return Err(invalid(
                "current executor lease required for private arguments",
            ));
        }
        if !state
            .durable
            .calls
            .iter()
            .any(|call| call.invocation_id == invocation && call.status.unsettled())
        {
            return Err(invalid("invocation is absent or settled"));
        }
        let context = state
            .contexts
            .get(invocation)
            .ok_or_else(|| invalid("private invocation dispatch scope retired"))?;
        if context.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        state
            .private_arguments
            .get(invocation)
            .cloned()
            .ok_or_else(|| invalid("private invocation arguments unavailable"))
    }
    pub async fn secret_operation(&self, socket: &str, operation: &str) -> Result<Value, Error> {
        let state = self.state.lock().await;
        if !state.participants.contains_key(socket) {
            return Err(invalid("bound owner participant required"));
        }
        let row = state
            .durable
            .secret_operations
            .iter()
            .find(|row| row.operation_id == operation)
            .ok_or_else(|| invalid("secret operation not found"))?;
        let call = state
            .durable
            .calls
            .iter()
            .find(|call| call.invocation_id == row.invocation_id);
        Ok(
            serde_json::json!({"operation_id": row.operation_id, "invocation_id": row.invocation_id, "status":row.status,"call":call}),
        )
    }
    pub async fn secret(
        self: &Arc<Self>,
        socket: &str,
        request: SecretRequest,
    ) -> Result<CallRecord, Error> {
        let operation = uuid::Uuid::parse_str(&request.operation_id)
            .map_err(|_| invalid("canonical secret operation UUID required"))?;
        if operation.to_string() != request.operation_id {
            return Err(invalid("canonical secret operation UUID required"));
        }
        let mut state = self.state.lock().await;
        if !state.participants.contains_key(socket) {
            return Err(invalid("bound owner participant required"));
        }
        if let Some(previous) = state
            .durable
            .secret_operations
            .iter()
            .find(|row| row.operation_id == request.operation_id)
        {
            if previous.invocation_id != request.invocation_id {
                return Err(invalid("secret operation identity conflicts"));
            }
            return state
                .durable
                .calls
                .iter()
                .find(|call| {
                    call.invocation_id == request.invocation_id && !call.status.unsettled()
                })
                .cloned()
                .ok_or_else(|| {
                    invalid("secret operation is committing; observe its operation identity")
                });
        }
        if state.durable.secret_operations.len() >= MAX_CALLS {
            return Err(invalid("secret operation retention limit reached"));
        }
        let index = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == request.invocation_id)
            .ok_or_else(|| invalid("secret interaction not found"))?;
        let call = &state.durable.calls[index];
        if !call.status.unsettled() {
            return Ok(call.clone());
        }
        if state
            .durable
            .secret_operations
            .iter()
            .any(|row| row.invocation_id == request.invocation_id)
        {
            return Err(invalid(
                "secret interaction already claimed; observe its operation",
            ));
        }
        let capability = state
            .durable
            .catalogs
            .iter()
            .filter(|catalog| catalog.digest == call.catalog_digest)
            .flat_map(|catalog| &catalog.definitions)
            .find(|entry| entry.definition.name == call.tool)
            .and_then(|entry| entry.interaction.as_ref())
            .and_then(|interaction| interaction.secret_writes.as_ref())
            .ok_or_else(|| invalid("interaction has no private write capability"))?;
        let requested = call
            .arguments
            .pointer(&capability.keys_pointer)
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("secret interaction requires an exact key list"))?;
        if requested.is_empty() || requested.len() > capability.max_keys.min(8) {
            return Err(invalid("secret key list exceeds its bound"));
        }
        let mut keys = BTreeSet::new();
        for field in requested {
            let key = field
                .get("key")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("secret key required"))?;
            if !valid_secret_key(key) || !keys.insert(key.to_owned()) {
                return Err(invalid("secret keys must be valid and unique"));
            }
        }
        if request.values.len() != keys.len()
            || request.values.keys().any(|key| !keys.contains(key))
            || request
                .values
                .values()
                .any(|value| value.is_empty() || value.len() > 8192)
        {
            return Err(invalid(
                "provide every requested secret once within the value limit",
            ));
        }
        let context = state
            .contexts
            .get(&request.invocation_id)
            .cloned()
            .ok_or_else(|| invalid("secret interaction no longer live"))?;
        if context.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let runtime_id = context.state.inspection()?.witness.runtime_instance_id;
        let (registered_id, client_state) = state
            .secret_states
            .get(&call.session_id)
            .ok_or_else(|| invalid("exact Client private write state is not installed"))?;
        if *registered_id != runtime_id {
            return Err(invalid("private interaction runtime identity changed"));
        }
        let client_state = client_state.clone();
        let mut durable = state.durable.clone();
        durable.calls[index].responder_socket = Some(socket.into());
        durable.secret_operations.push(SecretOperation {
            operation_id: request.operation_id.clone(),
            invocation_id: request.invocation_id.clone(),
            status: "committing".into(),
        });
        self.store.commit(&durable)?;
        state.durable = durable;
        let bridge = self.clone();
        let (sender, receiver) = oneshot::channel();
        let invocation = request.invocation_id.clone();
        let worker = tokio::spawn(async move {
            let result = bridge
                .finish_secret(request, context, client_state, keys)
                .await;
            let _ = sender.send(result);
        });
        state.interaction_workers.insert(invocation, worker);
        drop(state);
        receiver
            .await
            .map_err(|_| invalid("secret write acknowledgement unknown; observe operation"))?
    }
    async fn finish_secret(
        &self,
        request: SecretRequest,
        context: ToolContext,
        client_state: microcrab_session::State,
        keys: BTreeSet<String>,
    ) -> Result<CallRecord, Error> {
        let prepared = client_state.prepare_secret_write(request.values).await;
        let mut state = self.state.lock().await;
        let index = state
            .durable
            .calls
            .iter()
            .position(|call| call.invocation_id == request.invocation_id)
            .ok_or_else(|| invalid("claimed secret interaction missing"))?;
        if !state.durable.calls[index].status.unsettled() {
            drop(prepared);
            let mut durable = state.durable.clone();
            if let Some(row) = durable
                .secret_operations
                .iter_mut()
                .find(|row| row.operation_id == request.operation_id)
            {
                row.status = "failed".into();
            }
            self.store.commit(&durable)?;
            state.durable = durable;
            return Ok(state.durable.calls[index].clone());
        }
        let outcome = if context.cancellation.is_cancelled() {
            drop(prepared);
            Err(Error::Cancelled)
        } else {
            prepared.and_then(|prepared| prepared.commit())
        };
        let (status, result, error) = match outcome {
            Ok(_) => (
                CallStatus::Completed,
                Some(
                    serde_json::json!({"status":"provided","secrets":keys.iter().map(|key| serde_json::json!({"key":key,"placeholder":format!("{{{{secret:{key}}}}}")})).collect::<Vec<_>>()}),
                ),
                None,
            ),
            Err(error) => (
                CallStatus::Failed,
                None,
                Some(
                    context
                        .redaction_handle()
                        .redact_text(&error.diagnostic_reason()),
                ),
            ),
        };
        // If this durable ACK fails the old committing receipt remains authoritative;
        // an observer never retries the already-owned private write.
        let previous = state.durable.clone();
        if let Some(row) = state
            .durable
            .secret_operations
            .iter_mut()
            .find(|row| row.operation_id == request.operation_id)
        {
            row.status = if status == CallStatus::Completed {
                "completed"
            } else {
                "failed"
            }
            .into();
        }
        match self.commit_finish(&mut state, index, status, result, error, false) {
            Ok(record) => Ok(record),
            Err(error) => {
                state.durable = previous;
                Err(error)
            }
        }
    }
}
fn valid_secret_key(key: &str) -> bool {
    let mut bytes = key.bytes();
    key.len() <= 128
        && bytes
            .next()
            .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}
