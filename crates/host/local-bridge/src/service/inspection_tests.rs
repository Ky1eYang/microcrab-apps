//! Inspection authenticates availability without acquiring controller authority.
use super::*;
use tokio_util::sync::CancellationToken;

type AuthoritySnapshot = (
    Option<String>,
    Option<String>,
    Option<BindingScope>,
    Option<String>,
    u64,
    Instant,
    Instant,
    Instant,
);
async fn authority(bridge: &Bridge) -> AuthoritySnapshot {
    let auth = bridge.authority.lock().await;
    (
        auth.token.clone(),
        auth.credential.clone(),
        auth.scope.clone(),
        auth.bound_host_id.clone(),
        auth.generation,
        auth.lease_deadline,
        auth.credential_deadline,
        auth.pair_deadline,
    )
}
fn pair(token: &str, project: &str) -> WireFrame {
    WireFrame::Pair {
        token: token.into(),
        scope: BindingScope::project(
            "backend".into(),
            format!("binding-{project}"),
            project.into(),
        ),
        directory: None,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    }
}
fn guarded(mut frame: WireFrame, descriptor: &HostDescriptor) -> WireFrame {
    match &mut frame {
        WireFrame::Pair {
            expected_host_id,
            expected_workspace_id,
            expected_descriptor_digest,
            expected_workspace_root,
            ..
        }
        | WireFrame::Resume {
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
        }
        _ => panic!("authentication intent required"),
    }
    frame
}
async fn inspect(
    service: &BridgeService,
    id: &str,
    frame: &WireFrame,
) -> Result<HostDescriptor, WireError> {
    let reply = service
        .inspect(InspectRequest::new(id.into(), frame).unwrap())
        .await;
    let WireFrame::Inspected { request_id, result } = reply else {
        panic!("inspection reply required")
    };
    assert_eq!(request_id, id);
    result
}
#[tokio::test]
async fn repeated_inspection_and_guard_rejection_preserve_generated_pairing_capability() {
    let root = tempfile::tempdir().unwrap();
    let (service, token) = BridgeService::open(root.path(), &[], true, None).unwrap();
    let token = token.unwrap();
    let before = authority(&service.default).await;
    let intent = pair(&token, "a");
    let descriptor = inspect(&service, "inspect-1", &intent).await.unwrap();
    inspect(&service, "inspect-2", &intent).await.unwrap();
    assert_eq!(authority(&service.default).await, before);
    assert_eq!(
        service
            .pairing
            .as_ref()
            .unwrap()
            .lock()
            .await
            .token
            .as_deref(),
        Some(token.as_str())
    );
    let mut different = descriptor.clone();
    different.descriptor_digest = "a".repeat(64);
    let error = service
        .authenticate(guarded(intent.clone(), &different))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, ErrorCode::BindingMismatch);
    assert_eq!(authority(&service.default).await, before);
    assert_eq!(
        service
            .pairing
            .as_ref()
            .unwrap()
            .lock()
            .await
            .token
            .as_deref(),
        Some(token.as_str())
    );
    service
        .authenticate(guarded(intent, &descriptor))
        .await
        .unwrap();
    assert!(
        service
            .pairing
            .as_ref()
            .unwrap()
            .lock()
            .await
            .token
            .is_none()
    );
}
#[tokio::test]
async fn unauthorized_inspection_returns_no_descriptor_and_cannot_initialize_a_workspace() {
    let root = tempfile::tempdir().unwrap();
    let child = root.path().join("child");
    std::fs::create_dir(&child).unwrap();
    let (service, _) = BridgeService::open(root.path(), &[], true, Some("secret".into())).unwrap();
    let before = authority(&service.default).await;
    let mut wrong = pair("wrong", "a");
    if let WireFrame::Pair {
        directory,
        expected_host_id,
        ..
    } = &mut wrong
    {
        *directory = Some(child.to_string_lossy().into());
        *expected_host_id = Some("foreign-host".into());
    }
    assert_eq!(
        inspect(&service, "invalid-token", &wrong)
            .await
            .unwrap_err()
            .code,
        ErrorCode::PairingDenied
    );
    assert!(!child.join(".microcrab").exists());
    assert_eq!(authority(&service.default).await, before);
    let single = BridgeService::single(service.default.clone());
    assert_eq!(
        inspect(&single, "single-invalid", &wrong)
            .await
            .unwrap_err()
            .code,
        ErrorCode::PairingDenied
    );
    assert_eq!(authority(&service.default).await, before);
    let invalid_scope = WireFrame::Resume {
        credential: "wrong".into(),
        scope: BindingScope::project("backend".into(), "binding".into(), "a".into()),
        directory: Some("missing-directory".into()),
        expected_host_id: Some("foreign-host".into()),
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    };
    assert_eq!(
        inspect(&service, "invalid-resume", &invalid_scope)
            .await
            .unwrap_err()
            .code,
        ErrorCode::PairingDenied
    );
}
#[tokio::test]
async fn rejected_candidate_preserves_current_credential_scope_lease_and_real_rpc() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("sentinel.txt"), "still-live").unwrap();
    let (service, _) = BridgeService::open(root.path(), &[], true, Some("secret".into())).unwrap();
    let (_, hello) = service.authenticate(pair("secret", "b")).await.unwrap();
    let WireFrame::Hello {
        credential,
        lease_generation,
        ..
    } = hello
    else {
        panic!("Hello required")
    };
    let before = authority(&service.default).await;
    let candidate = pair("secret", "a");
    let mut descriptor = inspect(&service, "candidate-a", &candidate).await.unwrap();
    descriptor.descriptor_digest = "b".repeat(64);
    assert_eq!(
        service
            .authenticate(guarded(candidate, &descriptor))
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::BindingMismatch
    );
    assert_eq!(authority(&service.default).await, before);
    assert!(service.default.check_lease(&lease_generation).await.is_ok());
    let read = service
        .default
        .execute(
            "still-live-read".into(),
            lease_generation.clone(),
            5000,
            HostRequest::ReadFile {
                path: "sentinel.txt".into(),
                max_bytes: 1024,
            },
            CancellationToken::new(),
        )
        .await
        .0
        .unwrap();
    let reference: TransferRef = serde_json::from_value(read).unwrap();
    assert_eq!(reference.byte_length, 10);
    let resume = WireFrame::Resume {
        credential,
        scope: BindingScope::project("backend".into(), "binding-b".into(), "b".into()),
        directory: None,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    };
    inspect(&service, "old-credential-retained", &resume)
        .await
        .unwrap();
    assert_eq!(authority(&service.default).await, before);
}
#[tokio::test]
async fn descriptor_changes_between_inspection_and_resume_fail_before_authority_mutation() {
    let root = tempfile::tempdir().unwrap();
    let (mut bridge, _) =
        Bridge::open_with_options(root.path(), true, Some("secret".into())).unwrap();
    let hello = bridge.authenticate(pair("secret", "a")).await.unwrap();
    let WireFrame::Hello { credential, .. } = hello else {
        panic!("Hello required")
    };
    let resume = WireFrame::Resume {
        credential,
        scope: BindingScope::project("backend".into(), "binding-a".into(), "a".into()),
        directory: None,
        expected_host_id: None,
        expected_workspace_id: None,
        expected_descriptor_digest: None,
        expected_workspace_root: None,
    };
    let WireFrame::Inspected {
        result: Ok(descriptor),
        ..
    } = bridge
        .authenticate_mode(resume.clone(), Some("inspect-before-change".into()))
        .await
        .unwrap()
    else {
        panic!("inspection required")
    };
    let before = authority(&bridge).await;
    let mutable = Arc::get_mut(&mut bridge).unwrap();
    mutable
        .descriptor
        .supported_primitives
        .push("future.primitive".into());
    mutable.descriptor.descriptor_digest = descriptor_fingerprint(&mutable.descriptor).unwrap();
    assert_eq!(
        bridge
            .authenticate(guarded(resume, &descriptor))
            .await
            .unwrap_err()
            .code,
        ErrorCode::BindingMismatch
    );
    assert_eq!(authority(&bridge).await, before);
}
