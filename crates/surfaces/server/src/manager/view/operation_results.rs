//! Bounded operation receipts shared by live and detached snapshots.
use microcrab_storage::OperationRecord;
use serde_json::Value;

const PARTIAL_BUDGET: usize = 512 * 1024;

/// Call after canonical-Context filtering, supersession, and receipt pagination.
/// Reserve the selected retry first without changing the receipt page order.
pub(in crate::manager) fn project_operation_results(
    operations: Vec<OperationRecord>,
    superseded: &[String],
    retry_id: Option<&str>,
) -> Vec<OperationRecord> {
    let partial_size = |operation: &OperationRecord| {
        let result = operation.result.as_ref()?;
        (operation.status == "failed"
            && result.get("error").is_some()
            && result["after_message_id"].as_str().is_some()
            && !superseded.contains(&operation.id))
        .then_some(())?;
        let partial = result
            .get("partial_output")
            .filter(|value| value.is_object())?;
        serde_json::to_vec(partial).ok().map(|bytes| bytes.len())
    };
    let reserved = operations
        .iter()
        .position(|operation| Some(operation.id.as_str()) == retry_id)
        .and_then(|index| {
            partial_size(&operations[index])
                .filter(|size| *size <= PARTIAL_BUDGET)
                .map(|size| (index, size))
        });
    let mut remaining = PARTIAL_BUDGET - reserved.map_or(0, |(_, size)| size);
    operations
        .into_iter()
        .enumerate()
        .map(|(index, mut operation)| {
            let size = partial_size(&operation);
            operation.result = operation.result.as_ref().map(|result| {
                let mut projected = serde_json::Map::new();
                for field in [
                    "usage",
                    "stop_reason",
                    "rounds",
                    "position",
                    "after_message_id",
                    "retry_of_operation_id",
                    "retry_after_message_id",
                ] {
                    if let Some(value) = result.get(field) {
                        projected.insert(field.into(), value.clone());
                    }
                }
                if let Some(error) = result.get("error") {
                    projected.insert("error".into(), error.clone());
                    if let Some(ready) = result.get("retry_ready") {
                        projected.insert("retry_ready".into(), ready.clone());
                    }
                }
                if let Some(size) = size {
                    if reserved.is_some_and(|(reserved_index, _)| reserved_index == index) {
                        projected.insert("partial_output".into(), result["partial_output"].clone());
                    } else if size <= remaining {
                        remaining -= size;
                        projected.insert("partial_output".into(), result["partial_output"].clone());
                    } else {
                        projected.insert("partial_output_omitted".into(), Value::Bool(true));
                    }
                }
                Value::Object(projected)
            });
            operation
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn failure(id: &str, time: u64) -> OperationRecord {
        serde_json::from_value(json!({
            "version":1,"id":id,"owner":"owner","session_id":"session",
            "method":"turn.submit","payload_hash":"opaque","status":"failed",
            "revision":1,"created_at_ms":time,"updated_at_ms":time,
            "result":{"error":{"code":"transport"},"retry_ready":true,
                "retry_witness":"private","after_message_id":"anchor",
                "partial_output":{"text":"x".repeat(64 * 1024),"incomplete":true}},
        }))
        .unwrap()
    }

    #[test]
    fn selected_retry_last_reserves_partial_budget_without_reordering() {
        let mut stored: Vec<_> = (1..=40)
            .map(|time| failure(&format!("new-{time}"), time))
            .collect();
        stored.push(failure("selected", 0));
        let original = stored.clone();
        let (page, truncated) = super::super::operation_page(stored.clone(), Some("selected"));
        assert!(truncated);
        assert_eq!(page.last().unwrap().id, "selected");
        let ids: Vec<_> = page.iter().map(|operation| operation.id.clone()).collect();
        let projected = project_operation_results(page, &["new-40".into()], Some("selected"));
        assert_eq!(
            projected
                .iter()
                .map(|operation| operation.id.clone())
                .collect::<Vec<_>>(),
            ids
        );
        assert!(projected.last().unwrap().result.as_ref().unwrap()["partial_output"].is_object());
        assert!(
            projected[0]
                .result
                .as_ref()
                .unwrap()
                .get("partial_output")
                .is_none()
        );
        assert!(projected.iter().any(|operation| {
            operation.result.as_ref().unwrap()["partial_output_omitted"] == true
        }));
        let bytes: usize = projected
            .iter()
            .filter_map(|operation| operation.result.as_ref()?.get("partial_output"))
            .map(|partial| serde_json::to_vec(partial).unwrap().len())
            .sum();
        assert!(bytes <= PARTIAL_BUDGET);
        assert_eq!(stored, original);
    }
}
