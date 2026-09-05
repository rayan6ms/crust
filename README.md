# Crust

Crust is a Rust-native Lavalink v4 compatible audio node. It provides the
Lavalink REST and WebSocket API, sessions, players, filters, statistics,
RoutePlanner, configuration, operations, and a bounded Discord voice boundary.

Crust uses [Mantle](https://github.com/rayan6ms/mantle) for source loading,
track compatibility, media decoding, audio processing, and Opus production.
It uses [Oto](https://github.com/rayan6ms/oto) for Discord Voice Gateway, RTP,
DAVE, and 20 ms network pacing. Both dependencies use exact Git revisions,
and `Cargo.lock` records the resolved dependency graph.

This is a source preview, not a tagged 1.0 release. Linux x86_64 is the
validated platform; other platforms are not currently claimed as supported.

## Build

Install Rust 1.97.1 (pinned by `rust-toolchain.toml`), Git, C and C++ compilers,
CMake, Clang/libclang, and platform build tools. Native media dependencies
are built from source; the first build needs network access.

The default server build enables Mantle and Oto. Set a strong, unique
`LAVALINK_SERVER_PASSWORD` or `CRUST_PASSWORD` before starting; do not use
the compatibility default password on a deployed node:

```sh
cargo build --locked --release --bin crust-server
# Supply LAVALINK_SERVER_PASSWORD through your environment or secret manager.
./target/release/crust-server --config application.yml.example
```

An example configuration is provided in `application.yml.example`. The server
listens on `127.0.0.1:2333` by default. The `Containerfile` and systemd example
show rootless deployment patterns. Build the image from the repository root
with `podman build -t localhost/crust:dev .`. Inside a container, bind the
server to `0.0.0.0`; restrict the host-published port as appropriate. Keep
credentials out of images and Git. Place any remotely exposed node behind
appropriate access controls and TLS termination. Health probes and enabled
Prometheus metrics are unauthenticated.

## Compatibility and development

The public API targets Lavalink 4.2.2. YouTube support is provided by Mantle;
Crust does not load Lavalink Java or Kotlin plugins. Important differences:

- Session resume is restricted to the original user.
- RoutePlanner address freeing accepts IP literals, not DNS names.
- `/crust/v1/info` reports actual Crust/Mantle/Oto build identity; standard
  version fields identify the Lavalink compatibility target.
- Mantle's generic load-error boundary cannot distinguish every search-specific
  rate-limit failure for RoutePlanner health classification.
- Native process telemetry uses Linux probes; other platforms report zero
  when those probes are unavailable.

Run the checked-in tests and quality checks with:

```sh
cargo test --locked --workspace --all-targets
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo deny --locked check # requires cargo-deny
```

The root workspace patch selects Oto's maintained OpenMLS crypto adaptation.
Keep it alongside the Oto pin: Cargo does not inherit dependency-workspace
patches. An unmaintained build-time procedural macro dependency is currently
acknowledged in `deny.toml`; runtime vulnerability checks remain enforced.

## License

Crust is dual licensed under the MIT License or Apache License, Version 2.0.
See `LICENSE-MIT` and `LICENSE-APACHE`. Dependencies retain their own licenses;
the build dependency inventory is available through `cargo metadata --locked`.
See `NOTICE` for dependency and redistribution notes. Media codec patent
rights and third-party service terms are separate from these software licenses.
