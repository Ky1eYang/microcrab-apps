# Chat Host flavor contract

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

## Verification

The Server library tests cover descriptor serialization and fingerprints, flavor
admission and projection, immutable identity, staged-sidecar recovery and conflicts,
owner/workspace fencing, and classification of grants without a sidecar.

The maintained-component gate checks catalog admission and the actual loopback
provider payload for fresh Coding and Universal Sessions. Chat Only keeps question,
todo and provider client tools while excluding Host tools. Universal's shell-free
mode has no shell permission or execution instructions. The gate also checks that a
native Host retains the linked shell tool. When multiple components are available,
Session creation selects the exact `component` digest from `agent.catalog` together
with the agent and mode names.

From the superproject root, set `MICROCRAB_TEST_CODING_COMPONENT` and
`MICROCRAB_TEST_UNIVERSAL_COMPONENT` to the prepared maintained component paths,
then run:

```sh
cargo xtask integration setup
cargo xtask integration metadata
cargo xtask integration test -p microcrab-server --lib --all-features --filter chat_only_maintained_components_preserve_client_tools_in_provider_payload --ignored
```

## Limits

The flavor/grant index is loaded at Manager startup to support synchronous
metadata projection, so memory/startup work scales with retained conversation
history. Existing Core pins remain the execution and owner authorities. The added
sidecar follows the same application-owned data-directory trust boundary, rejects
symlink/reparse sidecar paths, and does not promise protection against an
administrator concurrently replacing ancestors of that trusted directory.
