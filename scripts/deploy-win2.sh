#!/usr/bin/env bash
#
# Deploy DatabaseSync to win2 (C:\Services\DatabaseSync).
#
# WHY THIS EXISTS (hardened 2026-08-10). The win2 deploy was hand-run every time, so it
# never gained the protections scripts/deploy-ubu2.sh got in #1821. Two things went wrong:
#
#   1. The ad-hoc backup was a full `Copy-Item -Recurse` of the service directory, which
#      DUPLICATED secrets.env each time. Two extra copies of three live LMPro credentials
#      were created in a single day before anyone noticed.
#   2. #1821 declared win2 "0 literals" but only ever checked the ACTIVE profiles/ dir.
#      A canary over the whole tree on 2026-08-10 found 90 LITERAL credentials in 57 files:
#      profiles-backup-20260725/ (12), profiles.Development/ (4) and publish/ (74) - the
#      last being the same stale-publish-output mechanism #1821 closed on ubu2 but never
#      cleaned here. All removed; this script now FAILS the deploy if they come back.
#
# Run from the workstation, from anywhere:
#     ./scripts/deploy-win2.sh            # build on ubu2 + deploy binaries + verify
#     ./scripts/deploy-win2.sh --canary   # credential canary only, no build/deploy
#
# Rule #123: the build happens ON UBU2, never on the workstation. win-x64 self-contained
# cross-compiles fine from Linux.
#
# PROFILES ARE NEVER DEPLOYED BY THIS SCRIPT. win2 owns 01-06 (SQL Server); ubu2 owns
# 07-13 (PostgreSQL). Shipping the build's copy would cross-contaminate the two hosts.
# secrets.env is likewise never copied - it is per-host and lives only on the target.
#
# NOTE ON STYLE: every Windows-side step is written to a .ps1, copied over and executed.
# Inlining PowerShell into `ssh "... powershell -Command \"...\""` is a quoting minefield
# - it silently produced an empty result that read as a PASS during the #1975 work. Do not
# 'simplify' this back into inline one-liners.

set -euo pipefail

HOST=claude@win2.digsol.us
DEPLOY='C:/Services/DatabaseSync'
SRC=/mnt/devshare/ClaudeProjects/DatabaseSync
BUILD=/tmp/databasesync-win2-build
STAGE="${TMPDIR:-/tmp}/databasesync-win2-stage"
PSTMP="${TMPDIR:-/tmp}/databasesync-win2-ps"
CANARY_ONLY=false

[[ "${1:-}" == "--canary" ]] && CANARY_ONLY=true
mkdir -p "$PSTMP"

# Run a here-doc PowerShell script on win2. Returns the script's exit code.
run_ps() {
    local name="$1"
    cat > "$PSTMP/$name.ps1"
    scp -q "$PSTMP/$name.ps1" "$HOST:C:/Windows/Temp/$name.ps1"
    local rc=0
    ssh "$HOST" "powershell -ExecutionPolicy Bypass -File C:\\Windows\\Temp\\$name.ps1" || rc=$?
    ssh "$HOST" "del C:\\Windows\\Temp\\$name.ps1" >/dev/null 2>&1 || true
    return $rc
}

# --- Credential canary -------------------------------------------------------------
# Reports LITERAL credentials only: a ${NAME} placeholder is not a secret. Emits counts
# and PATHS, never values - a canary that prints the secret it found is its own leak.
canary() {
    run_ps canary <<'PS1'
$lit = @(); $ph = 0
Get-ChildItem -Recurse C:\Services -File -Filter *.json -ErrorAction SilentlyContinue |
  Where-Object { $_.Length -lt 2MB } | ForEach-Object {
    $t = Get-Content $_.FullName -Raw -ErrorAction SilentlyContinue
    foreach ($m in [regex]::Matches($t, 'Password=([^;"]*)')) {
      if ($m.Groups[1].Value -match '^\$\{[A-Za-z0-9_]+\}$') { $ph++ } else { $lit += $_.FullName }
    }
  }
Write-Output ("    placeholders=" + $ph)
# Any non-JSON file carrying a credential, other than the live secrets.env, is also a hit.
$other = Get-ChildItem -Recurse C:\Services -File -ErrorAction SilentlyContinue |
  Where-Object { $_.Extension -ne '.json' -and $_.FullName -ne 'C:\Services\DatabaseSync\secrets.env' -and $_.Length -lt 2MB } |
  Select-String -Pattern 'Password=' -List -ErrorAction SilentlyContinue
if ($lit -or $other) {
  Write-Output "    CREDENTIAL FILES FOUND:"
  ($lit | Select-Object -Unique) | ForEach-Object { Write-Output ("      " + $_) }
  $other | ForEach-Object { Write-Output ("      " + $_.Path) }
  exit 1
}
Write-Output "    no credentials outside the live profiles/ and secrets.env (verified)"
exit 0
PS1
}

if [[ "$CANARY_ONLY" == true ]]; then
    echo "==> Credential canary on win2"
    canary
    exit $?
fi

# --- Refuse to stop the service mid-sync -------------------------------------------
# win2 moves ~91M rows a night; killing a run mid-flight wastes hours. An unreachable
# host must read as BUSY, never as safe - the same failing-safe-looking-green shape that
# produced a false PASS during #1975.
echo "==> Checking win2 is idle"
STATUS=$(ssh "$HOST" 'curl -s -m 20 http://localhost:5123/status' 2>/dev/null || true)
if ! grep -q 'profileName' <<<"$STATUS"; then
    echo "    ERROR: could not read /status - refusing to deploy blind" >&2
    exit 1
fi
if grep -q '"isRunning":true' <<<"$STATUS"; then
    echo "    ERROR: a sync is running. Wait for it to finish." >&2
    exit 1
fi
echo "    idle"

echo "==> Refreshing the ubu2 mirror from devlocal"
"$HOME/sync-devlocal.sh" >/dev/null
echo "    mirror synced"

echo "==> Building win-x64 on ubu2 (rule #123)"
ssh claude@ubu2.digsol.us 'bash -s' <<REMOTE
set -euo pipefail
rm -rf "$BUILD"; mkdir -p "$BUILD"
rsync -a --exclude=bin --exclude=obj --exclude='._*' --exclude='.DS_Store' \
      --exclude='profiles/' --exclude='profiles.Development/' --exclude='profiles.Production/' \
      --exclude='secrets.env' "$SRC/" "$BUILD/"
cd "$BUILD"
COMMIT=\$(git -C "$SRC" rev-parse HEAD 2>/dev/null || echo unknown)
echo "    building \$COMMIT"
dotnet publish DatabaseSync.csproj -c Release -r win-x64 --self-contained true \
       -p:SourceRevisionId="\$COMMIT" -o "$BUILD/out" --nologo | tail -2
# The csproj carries CopyToPublishDirectory=Never for profiles (#1821), but VERIFY rather
# than trust: a regression there ships credentials to the target.
STRAY=\$(find "$BUILD/out" -name '*.json' -print0 | xargs -0 grep -l 'Password=' 2>/dev/null | wc -l)
[ "\$STRAY" -eq 0 ] || { echo "    ERROR: \$STRAY credential file(s) in publish output" >&2; exit 1; }
[ ! -d "$BUILD/out/profiles" ] || { echo "    ERROR: profiles/ present in publish output" >&2; exit 1; }
echo "    publish output clean (0 credential files, no profiles dir)"
REMOTE

echo "==> Staging artifacts to the workstation"
rm -rf "$STAGE"; mkdir -p "$STAGE"
rsync -a claude@ubu2.digsol.us:"$BUILD/out/" "$STAGE/"
echo "    $(find "$STAGE" -maxdepth 1 -type f -name '*.*' | wc -l) flat files staged"

echo "==> Stopping the service"
ssh "$HOST" 'sc stop DatabaseSync' >/dev/null 2>&1 || true
sleep 10

# --- Backup, WITHOUT copying secrets.env -------------------------------------------
# The whole point of scripting this. An ad-hoc Copy-Item -Recurse duplicates the live
# credentials into a second, longer-lived location. The binaries are the only part of a
# rollback worth keeping, and they are reproducible from git anyway.
echo "==> Backing up binaries (secrets.env and profiles/ deliberately excluded)"
run_ps backup <<'PS1'
$b = 'C:\Services\DatabaseSync.bak'
if (Test-Path $b) { Remove-Item -Recurse -Force $b }
New-Item -ItemType Directory -Path $b | Out-Null
Get-ChildItem 'C:\Services\DatabaseSync' -File |
  Where-Object { $_.Name -ne 'secrets.env' } | Copy-Item -Destination $b
$n = (Get-ChildItem $b -File | Measure-Object).Count
$leak = Test-Path (Join-Path $b 'secrets.env')
Write-Output ("    backed up $n files; secrets.env copied = $leak")
if ($leak) { Write-Output '    ERROR: backup captured secrets.env'; exit 1 }
PS1

echo "==> Copying binaries (flat files only - never scp -r)"
# scp -r would carry the build's profiles/ and cross-contaminate win2 with PG profiles.
( cd "$STAGE" && scp -q *.* "$HOST:$DEPLOY/" )
echo "    copied"

echo "==> Starting and verifying"
ssh "$HOST" 'sc start DatabaseSync' >/dev/null 2>&1
sleep 20
ssh "$HOST" 'sc query DatabaseSync | findstr STATE'
ssh "$HOST" 'curl -s -m 20 http://localhost:5123/health'; echo

echo "==> Post-deploy checks"
run_ps postcheck <<'PS1'
$p = Get-ChildItem 'C:\Services\DatabaseSync\profiles' -Filter *.json -Name
Write-Output ("    profiles on host: " + ($p -join ', '))
$pg = @($p | Where-Object { $_ -match '^(0[7-9]|1[0-3])-' })
if ($pg.Count -gt 0) { Write-Output '    ERROR: PostgreSQL profiles leaked onto win2'; exit 1 }
if ($p.Count -ne 6) { Write-Output "    ERROR: expected 6 profiles, found $($p.Count)"; exit 1 }
Write-Output ("    secrets.env present: " + (Test-Path 'C:\Services\DatabaseSync\secrets.env'))
PS1

echo "==> Credential canary"
canary

echo "Deploy complete."
