#!/bin/sh
# Install the built packages on the distros a release actually targets, and
# prove the result works: files land, the service account is created, the
# operator's config survives an upgrade, removal keeps the queue, and the
# binary starts and answers /healthz on that distro's glibc.
#
# Run after packaging/build.sh:
#
#   packaging/verify.sh                     # every target
#   packaging/verify.sh rockylinux:9        # just one
#
# This is not ceremony. The TimeLakeDB package it is modelled on caught, on
# its first run, an `apt remove` that deleted the data directory and a
# scriptlet that failed every install on Amazon Linux 2023 (no shadow-utils) —
# neither visible from the spec. The same shape of bug is possible here.
#
# The same script runs the host loop and the in-container checks, so CI and a
# laptop cannot drift.

set -eu

# ---------------------------------------------------------------- in-container
if [ "${1:-}" = "--in-container" ]; then
    fails=0
    ck() { # ck "<description>" <command...>
        d=$1
        shift
        if "$@" >/dev/null 2>&1; then
            echo "  ok    $d"
        else
            echo "  FAIL  $d"
            fails=$((fails + 1))
        fi
    }

    . /etc/os-release 2>/dev/null || true
    echo "  ${PRETTY_NAME:-unknown} / glibc $(ldd --version 2>&1 | head -1 | sed 's/.* //')"

    if command -v apt-get >/dev/null 2>&1; then
        FMT=deb
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq >/dev/null 2>&1
        command -v curl >/dev/null 2>&1 || apt-get install -y -qq curl >/dev/null 2>&1
        apt-get install -y -qq /dist/*.deb >/dev/null 2>&1
    else
        FMT=rpm
        # AL2023 ships curl-minimal; asking for `curl` there starts a package
        # conflict, so only install it when the command is genuinely absent.
        command -v curl >/dev/null 2>&1 || dnf install -y -q curl >/dev/null 2>&1
        dnf install -y -q /dist/*.rpm >/dev/null 2>&1
    fi

    ck "binary installed"  test -x /usr/bin/tributary
    ck "unit installed"    test -f /usr/lib/systemd/system/tributary.service
    ck "config installed"  test -f /etc/tributary/config.toml
    ck "env installed"     test -f /etc/tributary/tributary.env
    ck "service account"   id tributary

    # The queue directory must NOT be package-owned (else removal deletes an
    # unshipped spool); postinstall creates it instead.
    ck "queue dir created" test -d /var/lib/tributary

    # Docs are asserted in the PAYLOAD, not on disk: the Ubuntu and AL2023
    # images configure their package manager to discard /usr/share/doc/*, so a
    # filesystem check there measures the image, not the package.
    if [ "$FMT" = deb ]; then
        ck "docs in package" sh -c 'dpkg -c /dist/*.deb | grep -q usr/share/doc/tributary/README.md'
    else
        ck "docs in package" sh -c 'rpm -qlp /dist/*.rpm | grep -q /usr/share/doc/tributary/README.md'
    fi

    # The one listener this agent opens is the telemetry endpoint, and it
    # carries no auth — so the packaged default must be loopback, never an
    # all-interfaces bind that a scraper reach also exposes file paths and
    # volumes to anyone on the network.
    ck "telemetry default is loopback" \
        grep -q '^addr = "127.0.0.1:9109"$' /etc/tributary/config.toml

    # The real question: does this binary run on THIS distro's glibc? The
    # shipped config has no [[source]] and refuses to start by design, so write
    # a minimal working one — tail a temp file, serve telemetry, ship nowhere
    # (it spools and keeps running) — and prove /healthz answers.
    mkdir -p /tmp/tb/state
    printf '{"ts":1,"level":"info","message":"smoke"}\n' > /tmp/tb/app.log
    cat > /tmp/tb/config.toml <<'CFG'
[output]
url = "http://127.0.0.1:59999"
database = "logs"
batch_lines = 10
[telemetry]
addr = "127.0.0.1:19109"
[[source]]
name = "smoke"
path = "/tmp/tb/app.log"
table = "smoke"
parser = "json"
timestamp = { field = "ts", format = "unix_ms", resolution = "ms" }
[source.fields]
message = "string"
CFG
    /usr/bin/tributary --config /tmp/tb/config.toml --state-dir /tmp/tb/state \
        >/tmp/tb/run.log 2>&1 &
    agent=$!
    served=1
    i=0
    while [ $i -lt 30 ]; do
        if curl -sf http://127.0.0.1:19109/healthz >/tmp/tb/health 2>/dev/null; then
            served=0
            break
        fi
        i=$((i + 1))
        sleep 1
    done
    if [ $served -eq 0 ]; then
        echo "  ok    serves /healthz on this glibc"
    else
        echo "  FAIL  never served /healthz; agent log:"
        sed 's/^/        /' /tmp/tb/run.log
        fails=$((fails + 1))
    fi
    kill $agent 2>/dev/null || true

    # An upgrade must not clobber the operator's configuration.
    echo "# operator edit" >> /etc/tributary/config.toml
    if [ "$FMT" = deb ]; then
        apt-get install -y -qq --reinstall /dist/*.deb >/dev/null 2>&1
    else
        dnf reinstall -y -q /dist/*.rpm >/dev/null 2>&1
    fi
    ck "config survives upgrade" grep -q "operator edit" /etc/tributary/config.toml

    # Uninstalling the agent must never destroy the queue.
    mkdir -p /var/lib/tributary/queue
    echo "unshipped line" > /var/lib/tributary/queue/000000000001.lp
    if [ "$FMT" = deb ]; then
        apt-get remove -y -qq tributary >/dev/null 2>&1
    else
        dnf remove -y -q tributary >/dev/null 2>&1
    fi
    ck "binary removed"        test ! -f /usr/bin/tributary
    ck "queue directory kept"  test -d /var/lib/tributary
    ck "queued data kept"      test -f /var/lib/tributary/queue/000000000001.lp
    ck "service account kept"  id tributary

    if [ "$fails" -ne 0 ]; then
        echo "  $fails CHECK(S) FAILED ($FMT)"
        exit 1
    fi
    echo "  all checks passed ($FMT)"
    exit 0
fi

# ------------------------------------------------------------------- host loop
REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
cd "$REPO_ROOT"

[ -n "$(ls dist/*.deb dist/*.rpm 2>/dev/null)" ] || {
    echo "no packages in dist/ — run packaging/build.sh first" >&2
    exit 1
}

# Two package managers, and the oldest and newest glibc we claim to support.
# AL2023 is not decoration: it is the EC2 default and the only one of these
# that ships without shadow-utils.
TARGETS=${*:-"debian:12 ubuntu:22.04 rockylinux:9 amazonlinux:2023"}

rc=0
for image in $TARGETS; do
    echo "===== $image ====="
    if docker run --rm \
        -v "$REPO_ROOT/dist":/dist:ro \
        -v "$REPO_ROOT/packaging/verify.sh":/verify.sh:ro \
        "$image" sh /verify.sh --in-container
    then :; else
        echo "  ^ FAILED on $image"
        rc=1
    fi
    echo
done

if [ $rc -ne 0 ]; then
    echo "package verification FAILED"
    exit 1
fi
echo "package verification passed on every target"
