//! OpenAI-compatible Chat Completions request parser for stateless model relay.
//! Accepts standard Chat Completions parameters without leaking Session features.
use super::Failure;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use microcrab_protocol::{ImageSource, InputPart, ToolCall, ToolDefinition};
use microcrab_transport::{Content, ContentItem, OpenAiCompatibleRequestOptions};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;
use url::{Host, Url};

const MAX_IMAGE_DATA_BYTES: usize = 8 * 1024 * 1024;

#[derive(Deserialize)]
pub(super) struct Chat {
    pub model: Option<String>,
    messages: Vec<Message>,
    #[serde(default)]
    tools: Vec<FunctionTool>,
    #[serde(default)]
    pub stream: bool,
    pub stream_options: Option<StreamOptions>,
    max_tokens: Option<u32>,
    max_completion_tokens: Option<u32>,
    pub reasoning_effort: Option<String>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    n: Option<u32>,
}

#[derive(Deserialize)]
pub(super) struct StreamOptions {
    #[serde(default)]
    pub include_usage: bool,
}

#[derive(Deserialize)]
#[serde(tag = "role", rename_all = "snake_case")]
enum Message {
    System {
        content: ContentInput,
    },
    Developer {
        content: ContentInput,
    },
    User {
        content: ContentInput,
    },
    Assistant {
        content: Option<ContentInput>,
        #[serde(default)]
        tool_calls: Vec<FunctionCall>,
    },
    Tool {
        tool_call_id: String,
        content: ContentInput,
    },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ContentInput {
    String(String),
    Parts(Vec<MessagePart>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MessagePart {
    Text { text: String },
    ImageUrl { image_url: ImageUrlInput },
}

#[derive(Deserialize)]
struct ImageUrlInput {
    url: String,
}

impl ContentInput {
    fn take_text(self) -> Result<String, Failure> {
        match self {
            Self::String(text) => Ok(text),
            Self::Parts(parts) => {
                let mut text = String::new();
                for part in parts {
                    match part {
                        MessagePart::Text { text: chunk } => text.push_str(&chunk),
                        MessagePart::ImageUrl { .. } => {
                            return Err(Failure::bad(
                                "Image inputs are only supported in user messages",
                            ));
                        }
                    }
                }
                Ok(text)
            }
        }
    }

    fn into_user_parts(self, max_bytes: usize) -> Result<Vec<InputPart>, Failure> {
        match self {
            Self::String(text) => Ok(vec![InputPart::Text { text }]),
            Self::Parts(parts) => {
                let mut out = Vec::with_capacity(parts.len());
                for part in parts {
                    match part {
                        MessagePart::Text { text } => out.push(InputPart::Text { text }),
                        MessagePart::ImageUrl { image_url } => {
                            out.push(InputPart::Image {
                                source: parse_image_url(&image_url.url, max_bytes)?,
                            });
                        }
                    }
                }
                if out.is_empty() {
                    return Err(Failure::bad("User message parts cannot be empty"));
                }
                Ok(out)
            }
        }
    }
}

fn parse_image_url(raw: &str, max_bytes: usize) -> Result<ImageSource, Failure> {
    if let Some(rest) = raw.strip_prefix("data:") {
        let Some((header, data_b64)) = rest.split_once(',') else {
            return Err(Failure::bad("Invalid image data URL"));
        };
        let Some((mime, encoding)) = header.split_once(';') else {
            return Err(Failure::bad("Image data URL must specify base64 encoding"));
        };
        if !encoding.eq_ignore_ascii_case("base64") {
            return Err(Failure::bad("Image data URL must use base64 encoding"));
        }
        let media_type = match mime.trim().to_ascii_lowercase().as_str() {
            "image/png" => "image/png",
            "image/jpeg" | "image/jpg" => "image/jpeg",
            "image/webp" => "image/webp",
            "image/gif" => "image/gif",
            _ => return Err(Failure::bad("Unsupported image media type")),
        }
        .to_owned();
        let data = STANDARD
            .decode(data_b64.trim())
            .map_err(|_| Failure::bad("Invalid base64 image payload"))?;
        if data.is_empty() || data.len() > MAX_IMAGE_DATA_BYTES.min(max_bytes) {
            return Err(Failure::bad("Image payload is empty or exceeds size limit"));
        }
        return Ok(ImageSource::Data { media_type, data });
    }
    let url = Url::parse(raw).map_err(|_| Failure::bad("Invalid image_url"))?;
    let valid_https = url.scheme() == "https"
        && url.username().is_empty()
        && url.password().is_none()
        && matches!(url.host(), Some(Host::Domain(domain)) if {
            let host = domain.trim_end_matches('.');
            host.contains('.') && !host.ends_with(".localhost") && !host.ends_with(".local")
        });
    if !valid_https {
        return Err(Failure::bad(
            "image_url must use a public HTTPS domain or data:image base64 URI",
        ));
    }
    Ok(ImageSource::Url {
        url: url.to_string(),
    })
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FunctionTool {
    Function { function: ToolDefinitionInput },
}

#[derive(Deserialize)]
struct ToolDefinitionInput {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default = "empty_object")]
    parameters: Value,
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FunctionCall {
    Function { id: String, function: Call },
}

#[derive(Deserialize)]
struct Call {
    name: String,
    arguments: String,
}

fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

impl Chat {
    pub(super) fn request_options(&self) -> Result<OpenAiCompatibleRequestOptions, Failure> {
        if let Some(effort) = self.reasoning_effort.as_deref()
            && (effort.is_empty()
                || effort.len() > 64
                || effort == "default"
                || !effort
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')))
        {
            return Err(Failure::bad("Invalid reasoning_effort value"));
        }
        Ok(OpenAiCompatibleRequestOptions {
            reasoning_effort: self.reasoning_effort.clone(),
        })
    }

    pub(super) fn content(self, max_bytes: usize) -> Result<Content, Failure> {
        if self.messages.is_empty()
            || self.messages.len() > 256
            || self.tools.len() > 64
            || (self.stream_options.is_some() && !self.stream)
            || self.n.is_some_and(|n| n != 1)
            || self
                .temperature
                .is_some_and(|t| !t.is_finite() || !(0.0..=2.0).contains(&t))
            || self
                .top_p
                .is_some_and(|p| !p.is_finite() || !(0.0..=1.0).contains(&p))
        {
            return Err(Failure::bad(
                "Unsupported message, tool, sampling, stream or n options",
            ));
        }
        let max_output_tokens = self
            .max_completion_tokens
            .or(self.max_tokens)
            .unwrap_or(4096);
        if !(1..=32_768).contains(&max_output_tokens) {
            return Err(Failure::bad(
                "Output token limit must be between 1 and 32768",
            ));
        }
        let mut tools = Vec::new();
        let mut names = HashSet::new();
        for FunctionTool::Function { function } in self.tools {
            if !valid_name(&function.name)
                || !function.parameters.is_object()
                || !names.insert(function.name.clone())
            {
                return Err(Failure::bad(
                    "A unique function name and an object parameter schema are required",
                ));
            }
            tools.push(ToolDefinition {
                name: function.name,
                description: function.description,
                parameters: function.parameters,
            });
        }
        let mut system_parts = Vec::new();
        let mut items = Vec::new();
        let mut pending = HashSet::new();
        let mut used = HashSet::new();
        for message in self.messages {
            if !pending.is_empty() && !matches!(&message, Message::Tool { .. }) {
                return Err(Failure::bad(
                    "All pending tool calls require matching results",
                ));
            }
            match message {
                Message::System { content } | Message::Developer { content } => {
                    if !items.is_empty() {
                        return Err(Failure::bad(
                            "System and developer messages must precede conversation messages",
                        ));
                    }
                    let text = content.take_text()?;
                    if !text.is_empty() {
                        system_parts.push(text);
                    }
                }
                Message::User { content } => items.push(ContentItem::User {
                    parts: content.into_user_parts(max_bytes)?,
                    source: None,
                    envelope: None,
                }),
                Message::Assistant {
                    content,
                    tool_calls,
                } => {
                    let mut calls = Vec::new();
                    for FunctionCall::Function { id, function } in tool_calls {
                        if id.is_empty()
                            || id.len() > 256
                            || id.chars().any(char::is_control)
                            || !used.insert(id.clone())
                            || !names.contains(&function.name)
                        {
                            return Err(Failure::bad(
                                "Tool calls require unique IDs and a declared function",
                            ));
                        }
                        let arguments: Value =
                            serde_json::from_str(&function.arguments).map_err(|_| {
                                Failure::bad("Tool arguments must encode a JSON object")
                            })?;
                        if !arguments.is_object() {
                            return Err(Failure::bad("Tool arguments must encode a JSON object"));
                        }
                        pending.insert(id.clone());
                        calls.push(ToolCall {
                            id,
                            name: function.name,
                            arguments,
                            provider_metadata: None,
                        });
                    }
                    let content_text = match content {
                        Some(input) => input.take_text()?,
                        None => String::new(),
                    };
                    items.push(ContentItem::Assistant {
                        content: content_text,
                        tool_calls: calls,
                        provider_metadata: None,
                    });
                }
                Message::Tool {
                    tool_call_id,
                    content,
                } => {
                    if !pending.remove(&tool_call_id) {
                        return Err(Failure::bad("Tool result does not match a pending call"));
                    }
                    items.push(ContentItem::Tool {
                        call_id: tool_call_id,
                        content: content.take_text()?,
                        parts: Vec::new(),
                    });
                }
            }
        }
        if !pending.is_empty() || items.is_empty() {
            return Err(Failure::bad("Complete conversation messages are required"));
        }
        Ok(Content {
            epoch: 0,
            subagent: None,
            system: system_parts.join("\n\n"),
            items,
            tools,
            max_output_tokens,
            max_bytes,
            turn_id: None,
            round: None,
        })
    }
}
