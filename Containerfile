# Build from the repository root: podman build -t localhost/crust:dev .
FROM docker.io/library/rust:1.97-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang libclang-dev
WORKDIR /workspace
COPY . .
RUN CARGO_BUILD_JOBS=2 cargo build --locked --release --bin crust-server

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin crust
COPY --from=build /workspace/target/release/crust-server /usr/local/bin/crust-server
USER crust
WORKDIR /home/crust
EXPOSE 2333
# Mount configuration and inject credentials at runtime; never bake in secrets.
ENTRYPOINT ["/usr/local/bin/crust-server"]
