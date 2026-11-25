# Client crate boundaries

Client packages the existing Server surface and standalone Local Bridge Host
carrier over Core. The layout does not change the package names, binary names,
startup paths, or HTTP/WebSocket contracts.

- [Server](../crates/surfaces/server) owns deployment configuration and auth,
  bound Host selection, route and approval authority, and Session persistence,
  publication, initialization, and recovery. In particular, a managed Session
  is published before effectful initialization so its authorized executor can
  receive approval requests.
- [Local Bridge](../crates/hosts/local-bridge) is the separately runnable Host
  carrier. Its WebSocket/gRPC transports and local execution policy are unchanged.
- [Bootstrap](../crates/bootstrap) plans Agent-dependent native builtin registrations,
  reads and hashes one bounded component snapshot against a caller-supplied Host,
  and prepares unstarted Sessions via distinct Core create, restore and fork
  paths. The caller selects artifacts and checks pins, schedules blocking I/O,
  binds and authorizes Hosts, wires persistence, and owns publication and start.
  Bootstrap is assembly wiring, not permission or authority to change a frozen
  tool catalog. It is not yet a versioned external embedding contract.

Conversation Host flavor and project carrier mode remain different Server
concepts. [Chat Only](chat-host-flavors.md) has a real conversation-bound Host,
not an absent Host. Browser Sandbox retains its existing grant and Host. The
project Host-state `embedded` and `detached` values describe the Server's existing
route observations, not new Host variants or claims about browser ownership.
