#!/bin/sh
# Runs after files are removed.
#
# What this deliberately does NOT do: delete /var/lib/tributary, or remove the
# tributary user. That directory is the durable queue — it can hold log lines
# that were accepted and spooled but not yet shipped, and uninstalling the
# agent must never throw those away. An operator who wants them gone removes
# them explicitly; leaving the account means the files keep a valid owner
# until they do.
set -e

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

# dpkg passes "purge" when the admin asked for the config to go too. Even then
# the queue stays; only the config directory we created is cleaned up.
if [ "$1" = "purge" ]; then
    rm -f /etc/tributary/config.toml /etc/tributary/tributary.env
    rmdir /etc/tributary 2>/dev/null || true
    cat <<'EOF'
Tributary configuration removed. The queue at /var/lib/tributary was kept,
along with the `tributary` user that owns it — it may still hold unshipped
log lines. Remove them by hand if you mean to discard that spool:

    rm -rf /var/lib/tributary && userdel tributary

EOF
fi

exit 0
