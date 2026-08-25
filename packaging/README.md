# Packaging

The `.deb` and `.rpm` attached to each GitHub Release, and how to build them.

| file | what it is |
|---|---|
| `nfpm.yaml` | the package spec — **one** file, both formats |
| `build.sh` | builds the binary and both packages, entirely in containers |
| `verify.sh` | installs and runs them on every target distro |
| `tributary.service` | the systemd unit |
| `config.toml` | the pipeline config, installed to `/etc/tributary/` |
| `tributary.env` | token + log level, installed to `/etc/tributary/` |
| `scripts/` | pre/post install and remove scriptlets |

## Verified on

Every one of these installs the package, starts the agent and gets a healthy
`/healthz` — checked by `packaging/verify.sh`, which the release workflow runs.

| distro | glibc | format |
|---|---|---|
| Debian 12 (bookworm) | 2.36 | deb |
| Ubuntu 22.04 LTS | 2.35 | deb |
| Rocky Linux 9 | 2.34 | rpm |
| Amazon Linux 2023 | 2.34 | rpm |

Newer releases of each (Debian 13, Ubuntu 24.04, RHEL 10) are covered by the
same floor. Amazon Linux **2** is not: its glibc is 2.26, below the floor.

## Build

```sh
packaging/build.sh                 # version from `git describe`, else Cargo.toml
packaging/build.sh 0.1.0-alpha     # explicit
packaging/build.sh --skip-build    # repackage an existing dist/tributary
```

Output lands in `dist/`: the two packages plus `SHA256SUMS`.

Then check them:

```sh
packaging/verify.sh                 # all four target distros
packaging/verify.sh rockylinux:9    # just one
```

Docker is the only prerequisite — no Rust toolchain, no `nfpm`, no `rpmbuild`,
no `dpkg-dev`. That is the same constraint the rest of this program builds
under, and it means a laptop and the CI runner produce the artifact the same
way.

On Windows, run these from WSL, or prefix with `MSYS_NO_PATHCONV=1` under Git
Bash — MSYS rewrites the container-side paths handed to `docker -v` and `sh
/verify.sh`, so without it the mounts silently point at the wrong place.

## One spec, two formats

`nfpm.yaml` is read twice, once per format. The alternative — `cargo-deb`
metadata plus a `cargo-generate-rpm` block — means two descriptions of the
same package that drift apart, and the drift is always found by whichever user
got the less-tested one. The places the formats genuinely differ live in one
`overrides:` block: `libc6 (>= 2.31)` against `glibc >= 2.31`, and `passwd`
against `shadow-utils` for `useradd`.

## The glibc floor

**Packages require glibc 2.31 or newer**: RHEL/Rocky/Alma 9+, Debian 11+,
Ubuntu 20.04+. RHEL 8 (glibc 2.28) is not covered.

This is a build-time property, not a runtime one. A dynamically linked binary
inherits the glibc of the machine that linked it and refuses to start on
anything older, so the floor is decided by `BUILD_IMAGE` — currently
`debian:11`. Linked on a current image instead, the agent demands a very new
`GLIBC_*` that RHEL 9, Debian 12 and Ubuntu 22.04 do not have: the package
would install cleanly and then fail at startup with a symbol-lookup error,
which is the worst possible way to learn about it.

`build.sh` reads the built binary's highest required `GLIBC_*` symbol and
fails the build if it exceeds what the metadata promises. Without that check
the floor is a comment; with it, raising `BUILD_IMAGE` by accident breaks the
build rather than somebody's host.

## What the package does, and does not, do

It installs:

```
/usr/bin/tributary                          the agent
/usr/lib/systemd/system/tributary.service   the unit
/etc/tributary/config.toml                  pipeline config; edits survive upgrades
/etc/tributary/tributary.env                token + log level (0640, holds a secret)
/var/lib/tributary                          the durable queue, owned by the tributary user
/usr/share/doc/tributary/                   README, LICENSE
```

It creates a `tributary` system account with no shell and points the unit at
it. The unit locks down the write side hard — `ProtectSystem=strict` with
`/var/lib/tributary` as the only writable path, no new privileges, a
`@system-service` syscall filter — while leaving reads open, because a tail
agent's entire job is reading files it does not own.

**It does not start the service, and that is deliberate.** Two reasons. The
shipped `config.toml` has no `[[source]]`, so the agent refuses to start until
it is told what to tail and where to ship. And the `tributary` user cannot
read root-only logs by default: on Debian/Ubuntu you add it to `adm`, and the
postinstall message says so where you will see it, rather than the package
silently granting a service account read over every system log.

Uninstalling does not delete `/var/lib/tributary` or the `tributary` user —
the queue can hold accepted-but-unshipped lines, and throwing those away on
`apt remove` is the exact silent loss this agent exists to prevent. `apt
purge` additionally removes the two config files and says plainly that the
queue is still there.

## Why verify.sh exists

Because reading the spec does not tell you what a package manager will do. It
installs on all four target distros and asserts the things a spec cannot: that
`apt remove` keeps the queue directory (a package-owned empty dir would be
deleted — the trap the TimeLakeDB package hit with its data directory), that
Amazon Linux 2023's missing `shadow-utils` is pulled in before the `useradd`
scriptlet runs, that an operator edit to `config.toml` survives an upgrade,
and that the binary actually starts and serves `/healthz` on that distro's
glibc.

## Releasing

`.github/workflows/release.yml` runs on a `v*` tag: it calls `build.sh`,
installs and smoke-tests the packages with `verify.sh`, and attaches both plus
`SHA256SUMS` to the Release. A tag containing `-` (`v0.1.0-alpha`) is marked
as a pre-release.

It deliberately does not re-run the test suite: `ci.yml` proves the tree, and
duplicating the billed private-repo minutes per tag makes cutting a release
something people avoid doing. Tag a commit whose CI was green.
