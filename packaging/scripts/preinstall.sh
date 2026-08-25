#!/bin/sh
# Create the service account before any file lands, so the packaged
# directories can be owned correctly on first install.
#
# Idempotent on purpose: this runs again on every upgrade, and on rpm it runs
# with different arguments than on dpkg, so it must not care which.
set -e

GROUP=tributary
USER=tributary
HOME_DIR=/var/lib/tributary

# The package depends on shadow-utils (rpm) / passwd (deb) so these exist by
# the time this runs. Checked anyway: on Amazon Linux 2023, which ships
# without shadow-utils, a missing useradd fails the whole transaction with
# "Error in PREIN scriptlet" and no indication of what was actually wrong.
for tool in getent groupadd useradd; do
    command -v "$tool" >/dev/null 2>&1 || {
        echo "tributary: $tool not found — install shadow-utils (rpm) or passwd (deb)" >&2
        exit 1
    }
done

# Not every distro puts nologin in the same place, and AL2023's minimal image
# has neither /usr/sbin/nologin nor /sbin/nologin. The account must have no
# usable shell; which unusable shell is not important.
NOLOGIN=/bin/false
for candidate in /usr/sbin/nologin /sbin/nologin /usr/bin/nologin; do
    if [ -x "$candidate" ]; then
        NOLOGIN=$candidate
        break
    fi
done

if ! getent group "$GROUP" >/dev/null 2>&1; then
    groupadd --system "$GROUP"
fi

if ! getent passwd "$USER" >/dev/null 2>&1; then
    useradd --system \
        --gid "$GROUP" \
        --home-dir "$HOME_DIR" \
        --no-create-home \
        --shell "$NOLOGIN" \
        --comment "Tributary service account" \
        "$USER"
fi

# Deliberately NOT added to any log-reading group (adm etc.) here. Granting a
# service account read over every system log is the operator's call, taken
# after they decide which files to tail — postinstall.sh says so in the place
# they will see it. Auto-joining `adm` would be a silent privilege grant.

exit 0
