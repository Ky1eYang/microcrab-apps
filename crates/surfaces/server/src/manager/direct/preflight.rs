//! Authenticated pre-admission evidence. No endpoint cache substitutes for a fresh
//! descriptor witness, and a probe never authorizes execution.
use super::*;
use microcrab_host_protocol::{
    BindingScope, ErrorCode, InspectRequest, decode_frame_with_web_transport_features,
    encode_frame_with_web_transport_features,
};
use microcrab_storage::execution::ProjectBinding;

fn mismatch(
    reason: &'static str,
    pin: Option<&ProjectBinding>,
    candidate: Option<&HostDescriptor>,
) -> WireError {
    let mut error = WireError::new(
        "binding_mismatch",
        "Host identity or descriptor differs from the immutable Project pin",
    );
    error.details = Some(Box::new(json!({
        "reason":reason,"action_required":"use_compatible_host_or_different_workspace",
        "pairing_token_consumed":false,
        "pinned_digest":pin.map(|pin| pin.descriptor_digest.as_str()),
        "candidate_digest":candidate.map(|descriptor| descriptor.descriptor_digest.as_str())
    })));
    error
}
pub(super) fn rejection(error: microcrab_host_protocol::WireError, resume: bool) -> WireError {
    if error.code == ErrorCode::BindingMismatch {
        let mut wire = WireError::new("binding_mismatch", error.message);
        wire.details = Some(Box::new(json!({
            "action_required":"use_compatible_host_or_different_workspace",
            "pairing_token_consumed":false,"phase":"pre_auth"
        })));
        return wire;
    }
    let code = match error.code {
        ErrorCode::PairingDenied if resume => "resume_rejected",
        ErrorCode::PairingDenied => "pairing_denied",
        ErrorCode::WorkspaceDenied => "workspace_denied",
        ErrorCode::Timeout => "host_pairing_expired",
        ErrorCode::Unsupported => "host_preflight_unsupported",
        _ => "host_transport_unavailable",
    };
    let mut wire = WireError::new(code, error.message);
    wire.details = Some(Box::new(
        json!({"phase":"pre_auth","pairing_token_consumed":false}),
    ));
    wire
}
pub(super) fn inspect(handshake: &HostFrame) -> Result<(String, HostFrame), WireError> {
    let id = uuid::Uuid::new_v4().to_string();
    let request =
        InspectRequest::new(id.clone(), handshake).map_err(|error| rejection(error, false))?;
    Ok((id, HostFrame::Inspect { request }))
}
pub(super) fn accept(
    frame: HostFrame,
    expected: &str,
    pin: Option<&ProjectBinding>,
    handshake: &HostFrame,
) -> Result<HostDescriptor, WireError> {
    let HostFrame::Inspected { request_id, result } = frame else {
        return Err(failure(
            "Native Host does not support authenticated inspection; pairing was not sent",
        ));
    };
    if request_id != expected {
        return Err(failure(
            "Host inspection correlation differs; pairing was not sent",
        ));
    }
    let descriptor =
        result.map_err(|error| rejection(error, matches!(handshake, HostFrame::Resume { .. })))?;
    validate(&descriptor, pin)?;
    Ok(descriptor)
}
pub(super) fn validate(
    descriptor: &HostDescriptor,
    pin: Option<&ProjectBinding>,
) -> Result<(), WireError> {
    if descriptor.host_id.is_empty()
        || descriptor.workspace_id.is_empty()
        || descriptor.bridge_instance_id.is_empty()
        || descriptor.workspace_root.trim().is_empty()
        || descriptor.workspace_root.len() > 32768
    {
        return Err(WireError::new(
            "invalid_host",
            "Native Host identity or workspace metadata is invalid",
        ));
    }
    if descriptor.protocol_version != microcrab_host_protocol::PROTOCOL_VERSION
        || microcrab_host_protocol::descriptor_fingerprint(descriptor)
            .map_err(|_| failure("Host descriptor cannot be fingerprinted"))?
            != descriptor.descriptor_digest
    {
        return Err(failure(
            "Native Host descriptor fingerprint or protocol version differs",
        ));
    }
    if let Some(pin) = pin {
        let reason = if descriptor.host_id != pin.host_id {
            Some("host_identity_mismatch")
        } else if descriptor.workspace_id != pin.workspace_id
            || descriptor.workspace_root != pin.workspace_root
        {
            Some("workspace_identity_mismatch")
        } else if descriptor.descriptor_digest != pin.descriptor_digest {
            Some("host_descriptor_mismatch")
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(mismatch(reason, Some(pin), Some(descriptor)));
        }
    }
    Ok(())
}
pub(super) fn validate_hello(
    descriptor: &HostDescriptor,
    pin: Option<&ProjectBinding>,
    inspected: &HostDescriptor,
) -> Result<(), WireError> {
    let result = validate(descriptor, pin).and_then(|()| {
        if descriptor.host_id != inspected.host_id
            || descriptor.workspace_id != inspected.workspace_id
            || descriptor.workspace_root != inspected.workspace_root
            || descriptor.descriptor_digest != inspected.descriptor_digest
        {
            Err(mismatch(
                "inspected_descriptor_changed",
                pin,
                Some(descriptor),
            ))
        } else {
            Ok(())
        }
    });
    result.map_err(|mut error| {
        let details = error.details.get_or_insert_with(|| Box::new(json!({})));
        details["pairing_token_consumed"] = json!(true);
        details["phase"] = json!("post_auth");
        error
    })
}
pub(super) fn guard(
    handshake: &mut HostFrame,
    descriptor: &HostDescriptor,
) -> Result<(), WireError> {
    match handshake {
        HostFrame::Pair {
            expected_host_id,
            expected_workspace_id,
            expected_descriptor_digest,
            expected_workspace_root,
            ..
        }
        | HostFrame::Resume {
            expected_host_id,
            expected_workspace_id,
            expected_descriptor_digest,
            expected_workspace_root,
            ..
        } => {
            *expected_host_id = Some(descriptor.host_id.clone());
            *expected_workspace_id = Some(descriptor.workspace_id.clone());
            *expected_descriptor_digest = Some(descriptor.descriptor_digest.clone());
            *expected_workspace_root = Some(descriptor.workspace_root.clone());
            Ok(())
        }
        _ => Err(failure("Host authentication intent is invalid")),
    }
}
fn authenticated_project_scope(handshake: &HostFrame) -> Option<&BindingScope> {
    let (authentication, scope) = match handshake {
        HostFrame::Pair { token, scope, .. } => (token, scope),
        HostFrame::Resume {
            credential, scope, ..
        } => (credential, scope),
        _ => return None,
    };
    (!authentication.is_empty() && scope.is_project() && scope.valid()).then_some(scope)
}

pub(super) fn web_transport_offer(handshake: &HostFrame) -> WebTransportFeatures {
    authenticated_project_scope(handshake)
        .map(|_| WebTransportFeatures::V1)
        .unwrap_or_default()
}

/// Call only after the original authenticated Hello and preflight checks pass.
/// Keep the proof scoped to this attempt and the inspected Bridge instance;
/// the original carrier remains usable if the optional proof is unavailable.
pub(super) fn web_transport_proof(
    handshake: &HostFrame,
    offer: WebTransportFeatures,
    acknowledgment: WebTransportFeatures,
    inspected: &HostDescriptor,
    descriptor: &HostDescriptor,
    credential: &str,
) -> Option<bindings::WebTransportProof> {
    if !microcrab_host_protocol::web_transport_v1_negotiated(offer.version, acknowledgment.version)
        || descriptor.bridge_instance_id != inspected.bridge_instance_id
        || credential.is_empty()
    {
        return None;
    }
    Some(bindings::WebTransportProof {
        scope: authenticated_project_scope(handshake)?.clone(),
        ws_pull: offer.ws_pull && acknowledgment.ws_pull,
    })
}

pub(super) async fn relay_exchange(
    outbound: &mpsc::Sender<Vec<u8>>,
    inbound: &mut mpsc::Receiver<Vec<u8>>,
    frame: &HostFrame,
    offer: WebTransportFeatures,
) -> Result<(HostFrame, WebTransportFeatures), WireError> {
    let bytes = encode_frame_with_web_transport_features(frame, offer)
        .map_err(|_| failure("Cannot encode Host handshake"))?;
    let exchange = async {
        outbound
            .send(bytes)
            .await
            .map_err(|_| failure("Host relay handshake send failed"))?;
        let bytes = inbound
            .recv()
            .await
            .ok_or_else(|| failure("Host relay closed during handshake"))?;
        decode_frame_with_web_transport_features(&bytes)
            .map_err(|_| failure("Host relay returned an invalid handshake frame"))
    };
    tokio::time::timeout(Duration::from_secs(10), exchange)
        .await
        .map_err(|_| failure("Host relay handshake exceeded 10 seconds"))?
}
impl Manager {
    pub(super) async fn preflight_candidate(
        &self,
        principal: &Principal,
        params: &Connect,
        descriptor: &HostDescriptor,
    ) -> Result<Option<Value>, WireError> {
        let project = params.project_id.as_str();
        let _identity = self.identity_gate.lock().await;
        if principal.expires_at <= super::super::now() / 1000 {
            return Err(WireError::new(
                "unauthenticated",
                "Authentication expired before Host admission",
            ));
        }
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(WireError::new("closed", "Server is shutting down"));
        }
        let registered = self.editable_project(&principal.owner, project).await?;
        let pin = self
            .pins
            .project(project)
            .await
            .map_err(super::super::safe)?;
        validate(descriptor, pin.as_ref())?;
        if registered
            .host_id
            .as_ref()
            .is_some_and(|host| host != &descriptor.host_id)
            || registered
                .workspace_id
                .as_ref()
                .is_some_and(|workspace| workspace != &descriptor.workspace_id)
        {
            return Err(mismatch(
                "project_identity_mismatch",
                pin.as_ref(),
                Some(descriptor),
            ));
        }
        if let Some(existing) = self
            .canonical_project(&principal.owner, project, descriptor)
            .await?
        {
            return Ok(Some(
                json!({"status":"existing_project","existing_project_id":existing,
                "project_id":project,"connected":false,"ready":false,
                "pairing_token_consumed":false,"descriptor":descriptor}),
            ));
        }
        if pin.is_none() {
            self.validate_unbound_project(
                &principal.owner,
                project,
                Some(&registered),
                params.allow_legacy_binding,
            )
            .await
            .map_err(|mut error| {
                let details = error.details.get_or_insert_with(|| Box::new(json!({})));
                details["phase"] = json!("pre_auth");
                details["pairing_token_consumed"] = json!(false);
                error
            })?;
        }
        Ok(None)
    }
}
