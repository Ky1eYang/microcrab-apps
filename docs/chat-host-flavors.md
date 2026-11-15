# Chat host flavor implementation record

Status: implemented source; native acceptance requires root-coordinated validation.

Client owns `HostFlavor` (`chat_only` / `browser_sandbox`) in an immutable
`conversation-flavors/<conversation UUID>.json` sidecar alongside the existing
execution identity directory. This works for both file and SQLite Session stores;
Core execution pin formats and historical bytes are unchanged. Each sidecar binds
the complete ConversationGrant, including owner, backend, binding, browser,
workspace and conversation identity.

`conversation.binding` accepts optional `host_flavor` and returns the existing
flattened conversation scope plus `host_flavor`. New frontends must send it
explicitly. Absent flavor retains the historical Sandbox default for older
clients. Existing browser grants without a sidecar classify as Sandbox; an
unrelated projectless Session does not. The sidecar is committed before publishing
the Core grant or returning preparation authority. A restart between those writes
reuses its exact staged grant. Complete staged bytes are synced and hard-linked
without replacement; failed/conflicting publication never rewrites old pins.
Disk publication and per-attach disk resolution run in blocking workers.

`conversation.attach` accepts optional flavor as a consistency witness. The
persisted flavor is authoritative. Chat Only requires an inert browser descriptor
with `/chat`, empty advertised Host capabilities/primitives and no virtual shell.
Attach replies expose flavor both at top level and in readiness. Session metadata
projections and managed snapshot refreshes preserve flavor for actual browser
conversations. Client tools, plugin permissions and network policy are unchanged.

Tests added:

- Exact Frontend golden bytes, typed Rust serialization and SHA-256 fingerprint.
- Manager flavor admission before attach, execution-descriptor rejection,
  immutable identity, projection classification and restart.
- Staged-sidecar crash recovery, independent-writer conflict, exact owner/workspace
  fencing and legacy grant-only classification.
- Explicit ignored maintained-component/provider acceptance test:
  `chat_only_maintained_components_preserve_client_tools_in_provider_payload`.
  Set `MICROCRAB_TEST_CODING_COMPONENT` and `MICROCRAB_TEST_UNIVERSAL_COMPONENT`
  to root-built maintained component paths, then invoke the root Cargo runner for
  Client server tests with all features and that filter, forwarding `--ignored`.
  Both Coding and Universal remain in the catalog with empty Host tools. The test
  creates a fresh Session for each agent and checks its actual loopback provider
  payload, retaining question/todo/provider client tools while excluding Host tools.
  Universal's shell-free mode carries no shell permission or execution instructions;
  native Hosts retain the actual linked shell tool. When multiple components are
  available, Session creation selects the exact `component` digest returned by
  `agent.catalog`, together with the agent and mode names.

Run the maintained-component gate from the superproject root after preparing both
component paths above:

```sh
cargo xtask integration setup
cargo xtask integration metadata
cargo xtask integration test -p microcrab-server --lib --all-features --filter chat_only_maintained_components_preserve_client_tools_in_provider_payload --ignored
```

Verification: scoped rustfmt and `git diff --check` passed. Explicit root
`integration setup` and `integration metadata` verified all 13 local Core package
paths and both Client snapshot workspace packages. The ignored Server lib test
above passed (1 passed, 0 failed, 494 filtered out), using the maintained Coding
component and the rebuilt Universal component at explicit absolute paths. This
proves catalog admission and fresh Session/provider payloads for both agents.
No live service restart, deployment, push or tag was performed; browser route
acceptance and existing immutable Session pins remain separate.

Limits: the flavor/grant index is loaded at Manager startup to support synchronous
metadata projection, so memory/startup work scales with retained conversation
history. Existing Core pins remain the execution and owner authorities. The added
sidecar follows the same application-owned data-directory trust boundary, rejects
symlink/reparse sidecar paths, and does not promise protection against an
administrator concurrently replacing ancestors of that trusted directory.
