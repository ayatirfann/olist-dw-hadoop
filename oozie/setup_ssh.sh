#!/usr/bin/env bash
# =====================================================================
# ONE-TIME SETUP: let Oozie run the pipeline scripts over SSH  (sandbox, as root)
#
# Oozie's SSH action logs in from the Oozie server (Linux user "oozie")
# to OOZIE_SSH_HOST and runs a command there. This gives the "oozie" user
# a key pair and authorises its public key for that login - the same
# key-based login you set up from the Mac to the sandbox.
#
# Usage:  /opt/olist/app/oozie/setup_ssh.sh
# =====================================================================
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$APP_DIR/conf/olist.conf"

OOZIE_HOME_DIR="$(getent passwd oozie | cut -d: -f6)"
TARGET_USER="${OOZIE_SSH_HOST%@*}"
TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6)"

# 1. key pair for the oozie user (kept if it already exists)
mkdir -p "$OOZIE_HOME_DIR/.ssh"
[ -f "$OOZIE_HOME_DIR/.ssh/id_rsa" ] || ssh-keygen -q -t rsa -N "" -f "$OOZIE_HOME_DIR/.ssh/id_rsa"
chown -R oozie "$OOZIE_HOME_DIR/.ssh"
chmod 700 "$OOZIE_HOME_DIR/.ssh"; chmod 600 "$OOZIE_HOME_DIR/.ssh/id_rsa"

# 2. authorise it for the target user (added only once)
mkdir -p "$TARGET_HOME/.ssh"; touch "$TARGET_HOME/.ssh/authorized_keys"
grep -qF "$(cat "$OOZIE_HOME_DIR/.ssh/id_rsa.pub")" "$TARGET_HOME/.ssh/authorized_keys" \
  || cat "$OOZIE_HOME_DIR/.ssh/id_rsa.pub" >> "$TARGET_HOME/.ssh/authorized_keys"
chmod 700 "$TARGET_HOME/.ssh"; chmod 600 "$TARGET_HOME/.ssh/authorized_keys"

# 3. test the login as the oozie user (also records the host key)
su -s /bin/bash oozie -c "ssh -o StrictHostKeyChecking=no -o BatchMode=yes $OOZIE_SSH_HOST 'echo [setup] oozie can log in as \$(whoami)@\$(hostname)'"
