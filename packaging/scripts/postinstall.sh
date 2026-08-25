#!/bin/sh
# Runs after files land, on both a fresh install and an upgrade.
#
# Deliberately does NOT enable or start the service. As shipped the agent has
# no [[source]] and will refuse to start anyway, but more to the point: which
# files to tail, which server to ship to, and whether the account may read
# those files are all the operator's decisions, taken after reading the
# config. The message below says so where they will actually see it.
set -e

STATE_DIR=/var/lib/tributary
CONF_DIR=/etc/tributary

# The state directory holds the durable queue; it is the only path the service
# writes. systemd's StateDirectory= also creates it, but a correct owner
# before first start means `tributary` run by hand behaves the same as the
# unit.
mkdir -p "$STATE_DIR"
chown -R tributary:tributary "$STATE_DIR"
chmod 0750 "$STATE_DIR"

# The config may reference credentials (a token file), and tributary.env may
# hold the token itself, so the directory is readable by the service account
# and root, nobody else.
if [ -d "$CONF_DIR" ]; then
    chown -R root:tributary "$CONF_DIR"
    chmod 0750 "$CONF_DIR"
    [ -f "$CONF_DIR/config.toml" ] && chmod 0640 "$CONF_DIR/config.toml"
    [ -f "$CONF_DIR/tributary.env" ] && chmod 0640 "$CONF_DIR/tributary.env"
fi

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    # On an upgrade, restart only if the operator had it running.
    if systemctl is-active --quiet tributary 2>/dev/null; then
        systemctl restart tributary >/dev/null 2>&1 || true
    else
        cat <<'EOF'

Tributary is installed but NOT started.

  1. Point it at your server and your logs:
       /etc/tributary/config.toml
     As shipped it has no [[source]] and will refuse to start. Set the output
     url and add one [[source]] per log file.

  2. If your TimeLakeDB server requires a token, set it in:
       /etc/tributary/tributary.env    (TRIBUTARY_TOKEN=...)

  3. Let the service account READ your log files. The agent runs as the
     unprivileged `tributary` user, which cannot read root-only logs by
     default. On Debian/Ubuntu:
       usermod -aG adm tributary
     Elsewhere, grant read on the paths you tail (group membership or ACLs).

  4. Start it:
       systemctl enable --now tributary

  5. Check it (if you kept [telemetry] on):
       curl -s http://127.0.0.1:9109/healthz

EOF
    fi
fi

exit 0
