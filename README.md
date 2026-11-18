# microcrab-client

Application hosts for the [microcrab core runtime](https://github.com/Ky1eYang/microcrab-core).
The Rust workspace contains an authenticated HTTP/WebSocket server and a local
Host bridge.

## Layout

- [`crates/server`](crates/server): authentication, project/session APIs, streaming
  transport, persistence assembly and static web asset serving.
- [`crates/host/local-bridge`](crates/host/local-bridge): local filesystem/process
  access over authenticated WebSocket and gRPC carriers.
- [`dev`](dev): source-only integration and browser acceptance tooling.

Core packages are Git dependencies from the same repository and revision. The
committed lockfile pins their resolved source.

## Development

Use a recent stable Rust toolchain supporting edition 2024 and your platform's
native build tools. Windows builds expect `rust-lld.exe` on `PATH` and the Windows
SDK for SQLite.

```sh
git clone git@github.com:Ky1eYang/microcrab-client.git
cd microcrab-client
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
loopback development only, the separate [development example](crates/server/config.example.jsonc)
uses the built-in issuer and requires `--features dev`.

All relative paths resolve from the server's startup working directory, not from
the configuration file's directory. `--config PATH` selects an explicit JSONC
file. Legacy `.microcrab/config.json` is considered only when the default JSONC
file is absent. The optional external TOML provider catalog is a distinct advanced
input; it is not the Server startup configuration. Each native Project requires
authenticated Host attachment.

Native Host attachment requires a Bridge that supports authenticated descriptor
preflight. Keep the Bridge and Server compatible. Each Project pins its Host
identity and capabilities. For a mismatch, connect a compatible Host or use a
different workspace. Selecting the same Host workspace follows duplicate
Project detection. Deleting a Project also deletes its Sessions.

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
