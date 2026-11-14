# microcrab-client

Application hosts for the [microcrab core runtime](https://github.com/Ky1eYang/microcrab-core).
The current Rust workspace contains an authenticated HTTP/WebSocket server and a
local Host bridge. Future desktop and terminal applications belong in this layer.

## Layout

- [`crates/server`](crates/server): authentication, project/session APIs, streaming
  transport, persistence assembly and static web asset serving.
- [`crates/host/local-bridge`](crates/host/local-bridge): local filesystem/process
  access over authenticated WebSocket and gRPC carriers.
- [`dev`](dev): source-only integration and browser acceptance tooling.

Core packages are Git dependencies from the same repository and revision. The
committed lockfile pins their resolved source. APIs may change before v1.0.

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

Copy the [server configuration example](crates/server/config.example.jsonc) to
`.microcrab/config.jsonc` and replace credential placeholders privately. The
example uses the development OIDC issuer and requires the `dev` feature:

```sh
cargo run --locked --offline -p microcrab-server --features dev
cargo run --locked --offline -p microcrab-local-bridge -- --help
```

For a deployed server, configure your own OIDC provider and disable `dev_oidc`.
Relative configuration paths resolve from the server's startup working directory.
Each native Project requires authenticated Host attachment.

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
