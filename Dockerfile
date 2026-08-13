# Tributary agent image.
#
# Exists so Catchment can run the agent as a container beside the database
# (C1, the calibration). Its whole point is "test the working tree, never a
# published tag", so this builds from source in this repository; a released
# image would be testing something other than the tree under review.
FROM rust:1-slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p tributary

# trixie matches the glibc of the rust:1-slim builder. TimeLakeDB's image
# learned this the expensive way — a bookworm runtime stage started failing
# with `GLIBC_2.38 not found` when the builder image moved forward — so the
# same pairing is used here rather than rediscovered.
FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/tributary /usr/local/bin/tributary

# Non-root, for the same reason TimeLakeDB is (SECURITY.md exposure 4). An
# agent tails files and writes a checkpoint; it has no business owning
# anything else on the filesystem. The two directories it does own are the
# state dir and the log directory it is pointed at.
RUN groupadd --system --gid 1000 tributary \
    && useradd --system --uid 1000 --gid 1000 --home-dir /var/lib/tributary tributary \
    && mkdir -p /var/lib/tributary/state /var/log/tributary \
    && chown -R tributary:tributary /var/lib/tributary /var/log/tributary
USER tributary:tributary

# The corpus lives on a container-local volume, never a host bind mount.
# On Docker Desktop an open fd loses its file across a rename, which is
# exactly the rotation case L1 exists to prove, and checkpoint fsyncs on a
# host mount measure the mount rather than the agent (Catchment ROADMAP C1).
VOLUME ["/var/lib/tributary/state", "/var/log/tributary"]

# No CMD default for --config on purpose: a config is the one thing the
# agent cannot invent, and starting with a guessed source is worse than
# refusing. `tributary` already exits 2 with usage when it is missing.
ENTRYPOINT ["tributary"]
