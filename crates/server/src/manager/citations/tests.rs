use super::*;
use microcrab_session::context::ContentItem;
use serde_json::json;

#[test]
fn canonical_projection_grants_only_final_assistant_content() {
    let messages = crate::manager::view::messages(
        vec![
            ContentItem::User {
                source: None,
                parts: vec![microcrab_protocol::InputPart::Text {
                    text: "[user](user.txt)".into(),
                }],
            },
            ContentItem::Assistant {
                content: "[report](report.txt)".into(),
                tool_calls: vec![],
                provider_metadata: None,
            },
            ContentItem::Tool {
                call_id: "call".into(),
                content: "[tool](tool.txt)".into(),
            },
        ],
        0,
        "session",
    );
    let id = messages[1]["id"].as_str().unwrap();
    assert_eq!(select(&messages, id, "report.txt").unwrap(), "report.txt");
    for (message, href) in [(&messages[0], "user.txt"), (&messages[2], "tool.txt")] {
        assert_eq!(
            select(&messages, message["id"].as_str().unwrap(), href)
                .unwrap_err()
                .code,
            "permission_denied"
        );
    }
    // A streamed browser block / missing canonical id has no durable eligibility.
    assert!(select(&messages, "streamed-preview", "report.txt").is_err());
    assert!(select(&messages, id, "uncited.txt").is_err());
    let separate = json!([{"id":"assistant", "role":"assistant", "text":"normal", "reasoning":"[reason](reason.txt)", "tool_calls":[{"arguments":"[arg](arg.txt)"}]}]);
    for href in ["reason.txt", "arg.txt"] {
        assert!(select(separate.as_array().unwrap(), "assistant", href).is_err());
    }
}
#[test]
fn raster_preview_detects_bytes_and_bounds_dimensions_svg_downloads() {
    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut data = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        data.extend_from_slice(&width.to_be_bytes());
        data.extend_from_slice(&height.to_be_bytes());
        data.extend_from_slice(&[8, 2, 0, 0, 0, 0, 0, 0, 0]);
        data
    }
    assert_eq!(media_type(&png(1, 1)), "image/png");
    assert_eq!(media_type(&png(4097, 1)), "application/octet-stream");
    assert_eq!(media_type(&png(4096, 4096)), "application/octet-stream");
    assert_eq!(
        media_type(b"<svg onload='alert(1)'/>"),
        "application/octet-stream"
    );
    assert_eq!(media_type(b"not a png"), "application/octet-stream");
}
#[test]
fn host_failures_keep_closed_category_without_paths() {
    use microcrab_host_api::{ErrorCategory, HostError, ToolError};
    for (error, code) in [
        (ToolError::PathOutsideRoot, "permission_denied"),
        (ToolError::FileLimit, "request_too_large"),
        (ToolError::Cancelled, "cancelled"),
        (
            ToolError::Host(HostError::new(ErrorCategory::NotFound, "secret-path")),
            "not_found",
        ),
        (ToolError::unsupported("read_file"), "unsupported"),
    ] {
        let result = host_error(error);
        assert_eq!(result.code, code);
        assert!(!result.message.contains("secret-path"));
    }
}
