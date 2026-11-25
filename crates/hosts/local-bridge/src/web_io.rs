//! Bridge-local I/O only: it does not select endpoints, interpret discovery bodies,
//! or grant authority. The caller owns scope/lease admission and socket publication.
use http_body_util::{BodyExt, Empty};
use hyper::{
    Request,
    body::{Body, Bytes},
    client::conn::http1,
    header,
};
use hyper_util::rt::TokioIo;
use std::{future::Future, net::IpAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::Instant,
};
use tokio_tungstenite::tungstenite::{self, http::Uri};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

const MAX_ENDPOINT_BYTES: usize = 4096;
const MAX_HTTP_BYTES: usize = 16 * 1024;
const MAX_SEND_BYTES: usize = 16 * 1024;
const MAX_RECEIVE_BYTES: usize = 256 * 1024;
const MAX_TIME: Duration = Duration::from_secs(10);

/// Redacted transport errors: no endpoint, handshake header, frame, body or OS text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Error {
    InvalidEndpoint,
    AuthorityMismatch,
    Limit,
    Cancelled,
    Timeout,
    Closed,
    Io,
    InvalidResponse,
    OutcomeUnknown,
}

type Result<T> = std::result::Result<T, Error>;

pub(super) struct Operation<'a> {
    deadline: Instant,
    lease: &'a CancellationToken,
    cancel: &'a CancellationToken,
    bridge_operation_id: Option<uuid::Uuid>,
}

impl<'a> Operation<'a> {
    /// Pass the original absolute deadline, not a fresh budget after queueing.
    pub(super) fn new(
        deadline: Instant,
        lease: &'a CancellationToken,
        cancel: &'a CancellationToken,
    ) -> Self {
        Self {
            deadline: deadline.min(Instant::now() + MAX_TIME),
            lease,
            cancel,
            bridge_operation_id: None,
        }
    }

    pub(super) fn with_bridge_operation_id(mut self, id: Option<uuid::Uuid>) -> Self {
        self.bridge_operation_id = id;
        self
    }

    fn check(&self) -> Result<()> {
        if self.lease.is_cancelled() || self.cancel.is_cancelled() {
            Err(Error::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(Error::Timeout)
        } else {
            Ok(())
        }
    }

    async fn run<T>(&self, io: impl Future<Output = Result<T>>) -> Result<T> {
        // A log-to-tracing bridge would otherwise collect tungstenite's raw
        // request, message and close-reason logs at verbose log levels. Scope
        // suppression to I/O future polls, never the caller or other routes.
        async {
            self.check()?;
            tokio::select! {
                biased;
                _ = self.lease.cancelled() => Err(Error::Cancelled),
                _ = self.cancel.cancelled() => Err(Error::Cancelled),
                _ = tokio::time::sleep_until(self.deadline) => Err(Error::Timeout),
                result = io => result,
            }
        }
        .with_subscriber(tracing::Dispatch::none())
        .await
    }
}

trait SocketIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> SocketIo for T {}

enum Kind {
    Http,
    WebSocket,
}

struct Endpoint<'a> {
    uri: Uri,
    // The original spelling is kept for protocol authority. No URL
    // normalizer may rewrite the request path/query, port, or host.
    authority: &'a str,
    host: &'a str,
    port: u16,
    tls: bool,
}

impl<'a> Endpoint<'a> {
    fn parse(raw: &'a str, kind: Kind) -> Result<Self> {
        if raw.len() > MAX_ENDPOINT_BYTES {
            return Err(Error::Limit);
        }
        if raw
            .bytes()
            .any(|b| !(0x21..=0x7e).contains(&b) || matches!(b, b'\\' | b'#'))
        {
            return Err(Error::InvalidEndpoint);
        }
        let (scheme, rest) = raw.split_once("://").ok_or(Error::InvalidEndpoint)?;
        let tls = match (kind, scheme) {
            (Kind::Http, "https") | (Kind::WebSocket, "wss") => true,
            (Kind::Http, "http") | (Kind::WebSocket, "ws") => false,
            _ => return Err(Error::InvalidEndpoint),
        };
        let authority = rest
            .split(['/', '?'])
            .next()
            .ok_or(Error::InvalidEndpoint)?;
        let (host, port) = if let Some(bracketed) = authority.strip_prefix('[') {
            let (host, tail) = bracketed.split_once(']').ok_or(Error::InvalidEndpoint)?;
            host.parse::<std::net::Ipv6Addr>()
                .map_err(|_| Error::InvalidEndpoint)?;
            (
                host,
                tail.strip_prefix(':')
                    .or_else(|| tail.is_empty().then_some(""))
                    .ok_or(Error::InvalidEndpoint)?,
            )
        } else {
            let (host, port) = authority.split_once(':').unwrap_or((authority, ""));
            if host.is_empty()
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-'))
            {
                return Err(Error::InvalidEndpoint);
            }
            // Reject ambiguous legacy numeric host forms rather than letting
            // system resolution quietly reinterpret them as another spelling.
            if host.parse::<IpAddr>().is_err()
                && !matches!(url::Host::parse(host), Ok(url::Host::Domain(_)))
            {
                return Err(Error::InvalidEndpoint);
            }
            (host, port)
        };
        let port = if port.is_empty() && !authority.ends_with(':') {
            if tls { 443 } else { 80 }
        } else {
            if !port.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::InvalidEndpoint);
            }
            port.parse::<u16>()
                .ok()
                .filter(|&p| p != 0)
                .ok_or(Error::InvalidEndpoint)?
        };
        // HTTP origin-form needs a '/' for an empty URL path, including
        // query-only URLs. Do not rewrite an explicitly supplied path/query.
        let target = &rest[authority.len()..];
        let uri: Uri = if target.starts_with('?') {
            format!("{scheme}://{authority}/{target}").parse()
        } else {
            raw.parse()
        }
        .map_err(|_| Error::InvalidEndpoint)?;
        if uri.authority().map(|a| a.as_str()) != Some(authority) {
            return Err(Error::InvalidEndpoint);
        }
        Ok(Self {
            uri,
            authority,
            host,
            port,
            tls,
        })
    }

    async fn dial(&self) -> Result<Box<dyn SocketIo>> {
        let name = self
            .tls
            .then(|| rustls::pki_types::ServerName::try_from(self.host.to_owned()))
            .transpose()
            .map_err(|_| Error::InvalidEndpoint)?;
        let stream = TcpStream::connect((self.host, self.port))
            .await
            .map_err(|_| Error::Io)?;
        let Some(name) = name else {
            return Ok(Box::new(stream));
        };
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Io)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let stream = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(name, stream)
            .await
            .map_err(|_| Error::Io)?;
        Ok(Box::new(stream))
    }
}

/// Defense-in-depth for a caller that already authorized discovery. This
/// does not interpret the body, grant permission, or dial either URL.
pub(super) fn same_discovery_authority(http: &str, ws: &str) -> Result<()> {
    let http = Endpoint::parse(http, Kind::Http)?;
    let ws = Endpoint::parse(ws, Kind::WebSocket)?;
    let same_host = match (http.host.parse::<IpAddr>(), ws.host.parse::<IpAddr>()) {
        (Ok(http), Ok(ws)) => http == ws,
        (Err(_), Err(_)) => http.host.eq_ignore_ascii_case(ws.host),
        _ => false,
    };
    if !same_host || http.port != ws.port || (http.tls && !ws.tls) {
        return Err(Error::AuthorityMismatch);
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct HttpResponse {
    pub(super) status: u16,
    pub(super) body: String,
}

fn limit(bytes: usize, cap: usize) -> Result<()> {
    if bytes > cap {
        Err(Error::Limit)
    } else {
        Ok(())
    }
}

pub(super) async fn http_get(
    raw: &str,
    max_bytes: usize,
    op: &Operation<'_>,
) -> Result<HttpResponse> {
    op.check()?;
    limit(max_bytes, MAX_HTTP_BYTES)?;
    let endpoint = Endpoint::parse(raw, Kind::Http)?;
    let request = Request::get(
        endpoint
            .uri
            .path_and_query()
            .ok_or(Error::InvalidEndpoint)?
            .as_str(),
    )
    .header(header::HOST, endpoint.authority)
    .header(header::CONNECTION, "close")
    .header(header::ACCEPT_ENCODING, "identity")
    .body(Empty::<Bytes>::new())
    .map_err(|_| Error::InvalidEndpoint)?;
    op.run(async {
        let stream = endpoint.dial().await?;
        op.check()?;
        let (mut sender, driver) = http1::Builder::new()
            .max_buf_size(16 * 1024)
            .max_headers(64)
            .handshake(TokioIo::new(stream))
            .await
            .map_err(|_| Error::Io)?;
        let response = async {
            let response = sender.send_request(request).await.map_err(|_| Error::Io)?;
            let status = response.status().as_u16();
            let mut body = response.into_body();
            if body.size_hint().lower() > max_bytes as u64 {
                return Err(Error::Limit);
            }
            let mut bytes = Vec::new();
            while let Some(frame) = body.frame().await {
                op.check()?;
                if let Ok(data) = frame.map_err(|_| Error::Io)?.into_data() {
                    limit(bytes.len() + data.len(), max_bytes)?;
                    bytes.extend_from_slice(&data);
                }
            }
            op.check()?;
            let body = String::from_utf8(bytes).map_err(|_| Error::InvalidResponse)?;
            Ok(HttpResponse { status, body })
        };
        tokio::pin!(response, driver);
        // Drive both in the operation task; no detached HTTP task can retain
        // the connection if the caller, lease, or deadline drops this future.
        tokio::select! {
            biased;
            result = &mut response => result,
            result = &mut driver => {
                result.map_err(|_| Error::Io)?;
                response.await
            }
        }
    })
    .await
}

pub(super) use text_socket::TextSocket;
mod text_socket;

fn ws_error(error: tungstenite::Error) -> Error {
    use tungstenite::{Error::*, error::ProtocolError};
    match error {
        ConnectionClosed
        | AlreadyClosed
        | Protocol(ProtocolError::ResetWithoutClosingHandshake) => Error::Closed,
        Capacity(_) | AttackAttempt => Error::Limit,
        Protocol(_) | Utf8(_) | Http(_) | HttpFormat(_) => Error::InvalidResponse,
        _ => Error::Io,
    }
}

#[cfg(test)]
mod tests;
