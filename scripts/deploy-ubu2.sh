#!/usr/bin/env bash
#
# Deploy DatabaseSync to ubu2 (/opt/services/DatabaseSync).
#
# WHY THIS EXISTS (AIM #1821): before this script the deploy was hand-copied, with no record of
# it on-host or in the repo. That is how /opt/services/DatabaseSync/profiles/ ended up
# world-readable with three production credentials in it (server_event #207) — a copy at the
# default umask silently widened the modes and nothing noticed. The mode enforcement below is
# the point of the script, not a nicety.
#
# Run from the workstation, from anywhere:
#     ./scripts/deploy-ubu2.sh            # build + deploy binaries, enforce modes
#     ./scripts/deploy-ubu2.sh --modes    # enforce modes only, no build/deploy
#
# Rule #123: the build happens ON ubu2, never on the workstation or the NFS mount.
# The mirror is refreshed first so ubu2 builds the current source, not up-to-an-hour-old source.
#
# PROFILES ARE NEVER DEPLOYED BY THIS SCRIPT. They are per-host configuration, they differ
# between win2 (01-06, SQL Server) and ubu2 (07-13, PostgreSQL), and shipping the build's copy
# would overwrite the live set. Update them deliberately and by hand.

set -euo pipefail

HOST=claude@ubu2.digsol.us
DEPLOY=/opt/services/DatabaseSync
UNIT=database-sync
MODES_ONLY=false

[[ "${1:-}" == "--modes" ]] && MODES_ONLY=true

if [[ "$MODES_ONLY" == false ]]; then
    echo "==> Refreshing the ubu2 mirror from devlocal"
    "$HOME/sync-devlocal.sh" >/dev/null
    echo "    mirror synced"

    echo "==> Building on ubu2 and deploying binaries"
    ssh "$HOST" 'bash -s' <<'REMOTE'
set -euo pipefail
SRC=/mnt/devshare/ClaudeProjects/DatabaseSync
BUILD=/tmp/databasesync-build
DEPLOY=/opt/services/DatabaseSync

rm -rf "$BUILD"; mkdir -p "$BUILD"
# Exclude profiles/ from the staged source so no credential-bearing file can reach the build
# output, and '._*' because macOS AppleDouble sidecars hard-fail the compile (error CS2015).
rsync -a --exclude=bin --exclude=obj --exclude='._*' --exclude='.DS_Store' \
      --exclude='profiles/' --exclude='profiles.Development/' --exclude='profiles.Production/' \
      --exclude='secrets.env' \
      "$SRC/" "$BUILD/"

cd "$BUILD"
COMMIT=$(git -C "$SRC" rev-parse HEAD 2>/dev/null || echo unknown)
dotnet publish DatabaseSync.csproj -c Release -p:SourceRevisionId="$COMMIT" \
       -o "$BUILD/out" --nologo

sudo systemctl stop database-sync
# --exclude protects live per-host state from being clobbered by build output.
sudo rsync -a --delete \
     --exclude='profiles/' --exclude='profiles.Development/' --exclude='profiles.Production/' \
     --exclude='secrets.env' --exclude='.database-sync.lock' \
     "$BUILD/out/" "$DEPLOY/"
echo "    deployed commit $COMMIT"
REMOTE
fi

echo "==> Enforcing permissions on credential-bearing files"
ssh "$HOST" 'sudo bash -s' <<'REMOTE'
set -euo pipefail
DEPLOY=/opt/services/DatabaseSync

# The service runs as root (the unit declares no User=), so root-owned 0600 is both sufficient
# and correct. Verified in server_event #207 by restarting and confirming all profiles loaded.
if [[ -d "$DEPLOY/profiles" ]]; then
    chown -R root:root "$DEPLOY/profiles"
    chmod 750 "$DEPLOY/profiles"
    find "$DEPLOY/profiles" -type f -exec chmod 600 {} +
fi

if [[ -f "$DEPLOY/secrets.env" ]]; then
    chown root:root "$DEPLOY/secrets.env"
    chmod 600 "$DEPLOY/secrets.env"
fi

# Globs must be expanded INSIDE sudo: the calling user cannot traverse a 750 directory, and a
# glob that expands as them silently yields nothing (AIM #1507 / server_event #207).
echo "    profiles dir: $(stat -c '%a %U:%G' "$DEPLOY/profiles" 2>/dev/null || echo 'absent')"
echo "    profile files:"
find "$DEPLOY/profiles" -type f -printf '      %m %u:%g %f\n' 2>/dev/null | sort || true
[[ -f "$DEPLOY/secrets.env" ]] && echo "    secrets.env:  $(stat -c '%a %U:%G' "$DEPLOY/secrets.env")"

# Prove the boundary rather than assuming it. github-runner runs every self-hosted CI job on
# this host and needs no privilege to read a world-readable file.
if id github-runner &>/dev/null; then
    if sudo -u github-runner test -r "$DEPLOY/profiles" 2>/dev/null; then
        echo "    !! github-runner CAN read the profiles directory - INVESTIGATE"; exit 1
    fi
    echo "    github-runner: access DENIED (verified)"
fi
REMOTE

echo "==> Starting and verifying"
ssh "$HOST" "sudo systemctl start $UNIT && sleep 4 && systemctl is-active $UNIT && curl -s -m 5 http://localhost:5123/health"
echo
echo "Deploy complete."
