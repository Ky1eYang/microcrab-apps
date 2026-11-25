# microcrab-apps

Application hosts for the [microcrab core runtime](https://github.com/Ky1eYang/microcrab-core).
The Rust workspace contains an authenticated HTTP/WebSocket server and a local
Host bridge.

## Layout

- [`crates/surfaces/server`](crates/surfaces/server) (`microcrab-server`):
  authentication, project/session APIs, streaming, persistence assembly, and
  static web asset serving.
- [`crates/hosts/local-bridge`](crates/hosts/local-bridge) (`microcrab-local-bridge`):
  local filesystem/process access over authenticated WebSocket and gRPC carriers.
- [`crates/bootstrap`](crates/bootstrap) (`microcrab-app-bootstrap`): pure Session
  assembly helpers; see [architecture](docs/architecture.md) for what stays in Server.
- [`dev`](dev): source-only integration and browser acceptance tooling.

Core packages are Git dependencies from the same repository and revision. The
committed lockfile pins their resolved source.

## Development

Use a recent stable Rust toolchain supporting edition 2024 and your platform's
native build tools. Windows builds expect `rust-lld.exe` on `PATH` and the Windows
SDK for SQLite.

```sh
git clone git@github.com:Ky1eYang/microcrab-apps.git
cd microcrab-apps
cargo fetch --locked
cargo fmt --all -- --check
cargo test --workspace --all-features --locked --offline -- --test-threads=1
```

Runtime acceptance also requires prepared Agent components and, for browser flows,
the separately built [web frontend](https://github.com/Ky1eYang/microcrab-frontend).
Build components in the core repository and supply their paths explicitly. Deploy
frontend build output into `public/` or set `frontend_dir` in the server config.

## Run

Copy the annotated [startup template](config.example.jsonc) to
`.microcrab/config.jsonc`. Set your actual public origin, external OIDC client,
provider model/credentials and prepared Agent paths before starting:

```sh
cargo run --locked --offline -p microcrab-server
cargo run --locked --offline -p microcrab-local-bridge -- --help
```

The template uses external OIDC and does not require the `dev` feature. For local
loopback development only, the separate [development example](crates/surfaces/server/config.example.jsonc)
uses the built-in issuer and requires `--features dev`.

All relative paths resolve from the server's startup working directory, not from
the configuration file's directory. `--config PATH` selects an explicit JSONC
file. Legacy `.microcrab/config.json` is considered only when the default JSONC
file is absent. The optional external TOML provider catalog is a distinct advanced
input; it is not the Server startup configuration. Each native Project requires
authenticated Host attachment.

Rust embedders can [prepare an opt-in provider image hosting route](docs/image-hosting.md)
with an injected uploader; ordinary Server model traffic remains inline.

Bridge token mode has no pairing-token time limit. `--require-token` generates a
code that remains valid until the first successful pairing (across all workspaces
served by that Bridge) or until the process stops. `--token TOKEN` uses an exact,
reusable token for that process. Descriptor preflight and rejected pair attempts
do not consume a generated token. Connection handshake, resume-credential and
controller-lease timeouts still apply.

Native Host attachment requires a Bridge that supports authenticated descriptor
preflight. Keep the Bridge and Server compatible. Each Project pins its Host
identity and capabilities. For a mismatch, connect a compatible Host or use a
different workspace. Selecting the same Host workspace follows duplicate
Project detection. Deleting a Project also deletes its Sessions.

Use `project.host_watch` on the authenticated global `/api/v1/ws` to observe the
selected Project. Send `project_id` and a fresh UUID `subscription_id`; the result
contains that ID, `sequence: 0`, and `summary`. Later `project_host_event` frames
for that ID carry the next sequence and a replacement summary; an omitted summary
ends the observation without closing the socket. A second watch replaces the first;
`project.host_unwatch` removes only the matching subscription. A gap or reconnect
requires a new baseline. The watch does not own a Host route or relay.

`GET /api/v1/projects/{project_id}/host-state` remains an owner-scoped, on-demand
read of the published route for startup/recovery and explicit diagnostics; do not
poll it when the global watcher is available. `carrier_mode` is `server_direct_grpc`, `server_direct_outbound`,
`browser_relay`, `embedded`, or `detached`. For a connected native route,
`descriptor` supplies Host/workspace identity, root, platform, tool capabilities
and supported primitives; otherwise it is `null`. A detached route can retain
its last `connection_generation` for diagnostics. Top-level `ready` includes the
Server admission gate; nested `readiness` is RemoteHost evidence. The response
provides no browser relay release witness or execution grant. Whether a tab owns
the live relay depends on that tab's own exact relay generation.

Automatic saved-address resume can set `only_if_unattached: true` on gRPC connect,
WebSocket outbound start, or browser relay attach without supplying new pairing
or legacy binding authority. If a connected route won the race, the Server returns
`host_route_observation_changed` (HTTP 409 on HTTP routes); read Host state again
instead of retrying or falling back. Explicit Connect retains its existing behavior.
A gRPC connect or WebSocket outbound start that reuses the already-connected
Server-direct route returns `observed_existing: true` with its current generation.
This is an observation, not a new attachment or a relay release witness.

`GET /api/v1/sessions/{session_id}/executions` reads retained execution metadata
without opening a Session. The response includes `items`, `next_cursor`, and a
`witness` with `session_id`, `history_lineage`, `history_epoch`, `rows_revision`,
and `dormant` (the page came from persisted history, not a live runtime). Compare
the witness to the selected Session and history before displaying it. To continue
paging on the same observation, pass `witness.rows_revision` as the opaque
`expected_revision`. A changed selection or retained row returns HTTP 409 with
`execution_history_changed`; discard the old pages and start with cursor zero
without a precondition. Requests without a precondition retain numeric cursor
paging, but do not guarantee consistency between pages.

The Server may include an optional `presentation` hint on selected tool calls
and results, live tool events, native execution rows, and approval requests.
A trusted runtime observation can identify `coding-file`, `message-receipt`, or
`view-image`; retained framework image carriers can also establish `view-image`
for historical calls. Unproven tools have no hint, and an observation without an
execution reservation can label only its own live event. This field is for display
only: it does not change arguments, tool output, retained audit evidence, execution
identity, or permission checks.

### External Speaker replies

In **Project → External Messaging**, explicitly enable **Allow external messaging**
for the bound Project. Projects start off, including upgrades with retained sender
keys; projectless Sessions cannot opt in. Disabled Projects reject new submissions
and all new receipt or reply observations, and keys cannot be revealed while off.
Disabling keeps keys, admitted inputs, and committed reply bytes. Re-enabling
resumes access only for still-effective credential incarnations; remove or rotate
keys for permanent revocation. A flag change is rejected without writing if an
affected Turn or activation is busy. Configuration acknowledgement is separate
from Session catalog application.

Only an enabled Project's eligible root Session advertises `reply_external_dialogue`.
Sessions with a live `reply_external_speaker` catalog cannot reopen; start a new
Session. A retained continuation whose catalog no longer matches the Project needs
explicit settlement or recovery, not replay under a different catalog.

Submit once to `POST /api/v1/sessions/{session_id}/inbox` with a speaker Bearer,
canonical UUID `Idempotency-Key`, and JSON `{ "body": "…" }`. Use the
acknowledged eight-hex `dialogue_id` to observe replies at either:

- `GET /api/v1/sessions/{session_id}/inbox` with `X-MicroCrab-Dialogue-Id`;
- `GET /api/v1/sessions/{session_id}/inbox/dialogues/{dialogue_id}/reply`.

Both require the exact, still-effective initiating speaker credential. An owner
cookie is insufficient. A replacement credential, even with identical bearer
bytes, cannot read earlier replies. A currently valid speaker whose credential
does not match the dialogue receives the same 404 as a missing dialogue. The GET
never starts a Session or resends input. For browser requests, only exact
`auth.allowed_origins` are accepted and granted route-local CORS; no wildcard
origin or ambient browser credentials are allowed. Native clients may omit Origin.

`Accept: text/event-stream` selects SSE. Each event has `dialogue_id` and uses
these decimal SSE IDs:

| Event | SSE ID | Other payload fields |
| --- | --- | --- |
| `queued` | `0` | `status: "queued"` |
| `consumed` | `1` | `status: "consumed"` |
| `chunk` | `index + 2` | zero-based `index`, `message`, `is_final`, `at_ms` (Unix milliseconds) |
| `done` | `total_chunks + 2` | `status`, `reason`, `total_chunks`, `settled_turn` |

`done` reflects the durable Turn terminal, not the `is_final` chunk flag. Only
explicit reply-tool messages appear; normal assistant output and reasoning never
enter this feed. Resume by explicitly supplying the last received SSE ID in
`Last-Event-ID`; the Server returns only later events. A GET without a cursor
starts reading at ID 0 (including retained events), without replaying mutations.
A stream stops on terminal settlement, authorization loss, deletion, shutdown,
or the five-minute observation deadline. EOF without `done` does not establish a
Turn outcome. Disconnecting cancels only the read.

Without `Accept: text/event-stream`, GET returns an immediate JSON snapshot with
`dialogue_id`, `speaker_identity`, `status`, `settled_turn`, `chunks`,
`aggregated_reply`, and `reason`; it does not wait for settlement. `settled_turn`
and `reason` are null before settlement and `aggregated_reply` is null with no
chunks. Terminal status/reason pairs are `completed`/`turn_settled`,
`completed`/`no_reply_invoked`, `cancelled`/`cancelled`, and
`interrupted`/`interrupted`. Legacy inbox receipts without an alias remain
observable as operation receipts but gain no reply association.

See [Chat Host flavors](docs/chat-host-flavors.md) for the browser conversation
binding and attachment contract.

Optional acceptance tooling uses `npm install` and
`python -m pip install -r dev/requirements.txt`. Its browser fixtures require
separately prepared runtime assets; `npm run test:poll` runs the standalone polling
utility tests.

## Contributing

Open a focused issue or pull request and include relevant verification. Preserve
authenticated owner isolation, permission boundaries and exact operation receipts.
Never commit credentials, private runtime state or generated assets.

## License

[MIT](LICENSE).
