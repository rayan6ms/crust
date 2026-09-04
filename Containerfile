# Build from the Projects directory so the sibling Oto workspace dependency is
# available to Cargo:
#   podman build -f crust/Containerfile -t localhost/crust:dev .
FROM docker.io/library/rust:1.97-bookworm AS build

WORKDIR /workspace
COPY crust/ crust/
COPY oto/ oto/
RUN cargo build --manifest-path crust/Cargo.toml --release --bin crust-server

FROM docker.io/library/debian:bookworm-slim

# Runs as an unprivileged user when launched by rootless Podman.
RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin crust
COPY --from=build /workspace/target/release/crust-server /usr/local/bin/crust-server
USER crust
WORKDIR /home/crust
EXPOSE 2333

# Mount application.yml and provide CRUST_PASSWORD (or the Lavalink password
# environment variable) through the runtime secret mechanism. No credential is
# baked into this image or printed by the entrypoint.
ENTRYPOINT ["/usr/local/bin/crust-server"]
