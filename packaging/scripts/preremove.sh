#!/bin/sh
# Runs before files are removed. Stop the service, but only on a real removal
# — on an upgrade the postinstall restarts it, and stopping here would turn
# every upgrade into an outage twice as long as it needs to be, during which
# logs pile up unshipped.
#
# The two package managers say "this is an upgrade" differently:
#   dpkg : $1 = "upgrade"      (vs "remove")
#   rpm  : $1 = "1"            (vs "0")
set -e

case "$1" in
    upgrade|1) exit 0 ;;
esac

if command -v systemctl >/dev/null 2>&1; then
    systemctl stop tributary >/dev/null 2>&1 || true
    systemctl disable tributary >/dev/null 2>&1 || true
fi

exit 0
