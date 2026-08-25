#!/bin/sh
# Build the .deb and .rpm release artifacts for the Tributary agent.
#
# Everything runs in containers: this needs Docker and nothing else — no Rust
# toolchain, no nfpm, no rpmbuild, no dpkg-dev. That is the same constraint
# the rest of this program builds under, and it means the CI runner and a
# laptop produce the artifact the same way.
#
# WHY THE BUILD IMAGE IS OLD
#
# A dynamically linked binary inherits the glibc of the machine that linked it
# and refuses to start on anything older. Linked on a current image the agent
# demands a very new GLIBC that RHEL 9 (2.34), Debian 12 (2.36) and Ubuntu
# 22.04 (2.35) do not have — the package would install cleanly and then fail
# with a symbol-lookup error, which is the worst way to learn this. Building on
# Debian 11 puts the floor at glibc 2.31 and covers RHEL/Rocky 9+, Debian 11+
# and Ubuntu 20.04+. Raise BUILD_IMAGE only with that trade-off in mind.
#
#   usage: packaging/build.sh [VERSION] [--skip-build]
#
# VERSION defaults to `git describe`, else the workspace version. A leading
# "v" is stripped; a tag like v0.1.0-alpha becomes the semver 0.1.0-alpha,
# which nfpm renders per format (deb 0.1.0~alpha, rpm 0.1.0~alpha).

set -eu

REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

BUILD_IMAGE=${BUILD_IMAGE:-debian:11}
NFPM_IMAGE=${NFPM_IMAGE:-goreleaser/nfpm:latest}
CARGO_REGISTRY_VOLUME=${CARGO_REGISTRY_VOLUME:-rk-cargo-registry}
TARGET_VOLUME=${TARGET_VOLUME:-tb-portable-target}

# Stated in the package metadata as a hard dependency, so "too old" is a
# refused install rather than a crash. Keep in step with BUILD_IMAGE.
PKG_GLIBC_MIN=${PKG_GLIBC_MIN:-2.31}
PKG_ARCH=${PKG_ARCH:-amd64}

SKIP_BUILD=0
VERSION=""
for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=1 ;;
        -h|--help) sed -n '1,30p' "$0"; exit 0 ;;
        *) VERSION=$arg ;;
    esac
done

if [ -z "$VERSION" ]; then
    VERSION=$(git describe --tags --abbrev=0 2>/dev/null || true)
fi
if [ -z "$VERSION" ]; then
    # No tag: fall back to the workspace version, which is what an untagged
    # local build should be called.
    VERSION=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
fi
PKG_VERSION=${VERSION#v}

echo "building tributary $PKG_VERSION ($PKG_ARCH, glibc >= $PKG_GLIBC_MIN)"

mkdir -p dist

if [ "$SKIP_BUILD" -eq 0 ]; then
    echo "==> compiling the agent on $BUILD_IMAGE"
    docker run --rm \
        -v "$REPO_ROOT":/w \
        -v "$CARGO_REGISTRY_VOLUME":/usr/local/cargo/registry \
        -v "$TARGET_VOLUME":/target \
        -w /w \
        -e CARGO_TARGET_DIR=/target \
        -e CARGO_HOME=/usr/local/cargo \
        -e PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
        "$BUILD_IMAGE" sh -c '
            set -e
            apt-get update -qq >/dev/null
            apt-get install -y -qq curl build-essential cmake pkg-config >/dev/null
            command -v cargo >/dev/null || \
                curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
                    | sh -s -- -y --default-toolchain stable --profile minimal --no-modify-path >/dev/null
            cargo build --release -p tributary
            # Symbols are a large fraction of the artifact and nothing debugs
            # from a package build; a separate debuginfo package is future work.
            strip /target/release/tributary
            cp /target/release/tributary /w/dist/tributary
        '
else
    echo "==> --skip-build: reusing dist/tributary"
    [ -f dist/tributary ] || { echo "dist/tributary missing"; exit 1; }
fi

# Fail loudly if the binary needs a newer glibc than we promise. Without this
# the floor is a comment; with it, raising BUILD_IMAGE by accident breaks the
# build instead of breaking somebody's host.
echo "==> verifying the glibc floor"
FLOOR=$(docker run --rm -v "$REPO_ROOT":/w -w /w "$BUILD_IMAGE" sh -c '
    apt-get update -qq >/dev/null 2>&1
    apt-get install -y -qq binutils >/dev/null 2>&1
    objdump -T dist/tributary | grep -o "GLIBC_[0-9.]*" | sort -V | tail -1
' | tr -d '\r')
echo "    binary requires at most ${FLOOR:-unknown}"
case "$FLOOR" in
    GLIBC_*)
        want=$(printf 'GLIBC_%s' "$PKG_GLIBC_MIN")
        highest=$(printf '%s\n%s\n' "$FLOOR" "$want" | sort -V | tail -1)
        if [ "$highest" != "$want" ]; then
            echo "ERROR: binary needs $FLOOR but the package promises $want." >&2
            echo "       Lower BUILD_IMAGE, or raise PKG_GLIBC_MIN and say so in the docs." >&2
            exit 1
        fi
        ;;
    *) echo "WARNING: could not read the glibc floor; not verified" >&2 ;;
esac

for fmt in deb rpm; do
    echo "==> packaging $fmt"
    docker run --rm \
        -v "$REPO_ROOT":/w -w /w \
        -e PKG_VERSION="$PKG_VERSION" \
        -e PKG_ARCH="$PKG_ARCH" \
        -e PKG_GLIBC_MIN="$PKG_GLIBC_MIN" \
        "$NFPM_IMAGE" pkg -f packaging/nfpm.yaml -p "$fmt" -t dist/
done

echo "==> checksums"
docker run --rm -v "$REPO_ROOT":/w -w /w/dist "$BUILD_IMAGE" sh -c '
    rm -f SHA256SUMS
    sha256sum *.deb *.rpm > SHA256SUMS
    cat SHA256SUMS
'

echo
echo "artifacts in dist/:"
ls -1 dist/*.deb dist/*.rpm dist/SHA256SUMS 2>/dev/null || true
