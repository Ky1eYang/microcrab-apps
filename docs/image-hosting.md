# Embedder-only provider image hosting

The Server does not expose image hosting through HTTP, WebSocket, startup JSONC,
provider TOML, or editable owner provider catalogs. No uploader ships in the
standalone Server. Normal Session and `view_image` traffic uses inline canonical Data.

A trusted Rust embedder holding `Application::manager` (or a separately constructed
`Manager`) may call `Manager::prepare_image_hosting_route(ProviderImageHosting)`.
The policy names the exact authenticated owner, operator-configured provider ID,
model and variant, expected provider protocol, and complete expected request URL
(including resource suffix and route query). It also supplies an opaque non-secret
upload authority label, dedicated HTTPS hostname allowlist, maximum hint lifetime,
and `Arc<dyn ImageUrlResolver>`. The Server resolves its authoritative catalog using
its normal model route constructor, and rejects a claimed protocol or endpoint that
does not match. User-editable `user:` routes are not eligible. The prepared route
owns one immutable scoped Core client; it is never installed into live Sessions or
retargeted. Discard it when the approved route or authority changes.

Given an immutable canonical `Content`, the embedder calls
`PreparedImageHostingRoute::authorize(authenticated_owner, content, positions)` only
with trusted fresh attachment provenance and explicit permission to upload the
exact images. Owner and provenance must come from trusted caller state, not a
request parameter, tool name, or retained-history scan. The resulting single-use
Rust grant is bound to the exact prepared route and canonical request. Consume it
with that handle's `stream` or `complete`. The default route has no uploader and
never issues such a grant.

The Core media contract governs allowlist validation, expiry, revocation, request
budget, and provider fallback. For an eligible recognized remote-fetch rejection,
Core invalidates the rejected hint and retries once before any streamed event,
subject to the total attempt budget. Explicit hosting attempts, including the inline
retry, produce metadata-only audit records and bypass request-body hooks. Ordinary
Server audit behavior and authenticated browser attachment/image routes are unchanged.
External object and provider retention are not controlled by local Session deletion.
