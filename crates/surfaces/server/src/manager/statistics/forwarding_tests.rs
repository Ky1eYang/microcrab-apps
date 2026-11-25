use super::*;
use microcrab_auditor::{EmptyVariableView, HookPayload};
use microcrab_session::context::Context;
use std::sync::atomic::AtomicUsize;
struct Sink {
    records: Mutex<Vec<AuditBody>>,
    interventions: AtomicUsize,
}
impl Auditor for Sink {
    fn is_enabled(&self) -> bool {
        true
    }
    fn publish(&self, _: Moment, body: AuditBody) {
        self.records.lock().unwrap().push(body);
    }
    fn intervene<'a>(
        &'a self,
        moment: Moment,
        context: HookContext<'a>,
    ) -> BoxFuture<'a, Result<Injection, AuditorError>> {
        Box::pin(async move {
            assert_eq!(moment, Moment::RequestPrepared);
            assert_eq!(context.call_id.as_deref(), Some("actual-call"));
            assert_eq!(context.request_body, Some("original-body"));
            assert_eq!(context.max_request_bytes, Some(65536));
            assert!(context.variables.get("missing").is_none());
            self.interventions.fetch_add(1, Ordering::SeqCst);
            Ok(Injection {
                request_body: Some("replacement-body".into()),
                ..Default::default()
            })
        })
    }
}
#[tokio::test]
async fn borrowed_decorator_forwards_real_hook_and_invalidates_request_rewrite_provenance() {
    let sink = Sink {
        records: Mutex::new(vec![]),
        interventions: AtomicUsize::new(0),
    };
    let content = Context::new("frozen system".into(), vec![], 65536)
        .unwrap()
        .content(32)
        .unwrap();
    let selection = ModelSelection {
        provider_id: "p".into(),
        model: "m".into(),
        variant: None,
    };
    let scoped = FingerprintAuditor::new(&sink, &content, &selection).unwrap();
    scoped.publish(
        Moment::RequestPrepared,
        AuditBody {
            sent_request: Some("unaltered-wire".into()),
            ..Default::default()
        },
    );
    let result = scoped
        .intervene(
            Moment::RequestPrepared,
            HookContext {
                epoch: 0,
                subagent: None,
                turn: Some(1),
                round: Some(1),
                call_id: Some("actual-call".into()),
                request_body: Some("original-body"),
                max_request_bytes: Some(65536),
                variables: &EmptyVariableView,
                payload: HookPayload::None,
            },
        )
        .await
        .unwrap();
    assert_eq!(result.request_body.as_deref(), Some("replacement-body"));
    scoped.publish(
        Moment::RoundCompleted,
        AuditBody {
            call_id: Some("actual-call".into()),
            ..Default::default()
        },
    );
    let records = sink.records.lock().unwrap();
    assert_eq!(records[0].sent_request.as_deref(), Some("unaltered-wire"));
    assert!(records[0].context_fingerprint.is_some());
    assert!(records[1].context_fingerprint.is_none());
    assert!(records[1].context_target.is_none());
    assert_eq!(sink.interventions.load(Ordering::SeqCst), 1);
}
