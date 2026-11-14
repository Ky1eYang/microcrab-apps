//! Egress contracts with a deterministic slow sink; no OS timing assumptions.
use super::*;

#[tokio::test]
async fn blocked_writer_bounds_queues_and_sends_control_before_chunks() {
    let gate = Arc::new(Semaphore::new(0));
    let (observed, mut messages) = mpsc::unbounded_channel();
    let sink = Box::pin(futures_util::sink::unfold((), {
        let gate = gate.clone();
        move |(), message: Message| {
            let gate = gate.clone();
            let observed = observed.clone();
            async move {
                observed.send(message).map_err(|_| ())?;
                gate.acquire().await.map_err(|_| ())?.forget();
                Ok::<(), ()>(())
            }
        }
    }));
    let (control, incoming) = mpsc::channel(1);
    let (chunks, chunk_incoming) = mpsc::channel(1);
    control
        .send(WireFrame::Response {
            request_id: "request".into(),
            operation_id: "operation".into(),
            result: Ok(serde_json::json!({"transfer_id":"transfer"})),
        })
        .await
        .unwrap();
    chunks
        .send(WireFrame::TransferChunk {
            transfer_id: "transfer".into(),
            sequence: 0,
            data: vec![1],
            eof: true,
        })
        .await
        .unwrap();
    let (diagnostics, diagnostic_incoming) = mpsc::channel(1);
    diagnostics
        .try_send(WireFrame::Pong {
            lease_generation: "diagnostic-lane-fixture".into(),
        })
        .unwrap();
    assert!(
        diagnostics
            .try_send(WireFrame::Pong {
                lease_generation: "overflow".into()
            })
            .is_err()
    );
    let writer = tokio::spawn(write_frames(
        sink,
        incoming,
        chunk_incoming,
        diagnostic_incoming,
    ));
    let first = tokio::time::timeout(Duration::from_secs(2), messages.recv())
        .await
        .unwrap()
        .unwrap();
    let Message::Binary(first) = first else {
        panic!("protobuf frame")
    };
    assert!(matches!(
        decode_frame(&first).unwrap(),
        WireFrame::Response { .. }
    ));
    // The reader can queue a control reply while the writer is blocked, and
    // neither lane retains an unlimited backlog under pressure.
    control
        .try_send(WireFrame::Pong {
            lease_generation: "lease".into(),
        })
        .unwrap();
    assert!(matches!(
        control.try_send(WireFrame::Pong {
            lease_generation: "lease".into()
        }),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    assert!(matches!(
        chunks.try_send(WireFrame::TransferChunk {
            transfer_id: "next".into(),
            sequence: 0,
            data: vec![],
            eof: true
        }),
        Err(mpsc::error::TrySendError::Full(_))
    ));
    gate.add_permits(1);
    let second = tokio::time::timeout(Duration::from_secs(2), messages.recv())
        .await
        .unwrap()
        .unwrap();
    let Message::Binary(second) = second else {
        panic!("protobuf frame")
    };
    assert!(matches!(
        decode_frame(&second).unwrap(),
        WireFrame::Pong { .. }
    ));
    gate.add_permits(1);
    let third = tokio::time::timeout(Duration::from_secs(2), messages.recv())
        .await
        .unwrap()
        .unwrap();
    let Message::Binary(third) = third else {
        panic!("protobuf frame")
    };
    assert!(matches!(
        decode_frame(&third).unwrap(),
        WireFrame::TransferChunk {
            sequence: 0,
            eof: true,
            ..
        }
    ));
    gate.add_permits(1);
    let fourth = tokio::time::timeout(Duration::from_secs(2), messages.recv())
        .await
        .unwrap()
        .unwrap();
    let Message::Binary(fourth) = fourth else {
        panic!("protobuf frame")
    };
    assert!(matches!(
        decode_frame(&fourth).unwrap(),
        WireFrame::Pong { lease_generation } if lease_generation == "diagnostic-lane-fixture"
    ));
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
}
