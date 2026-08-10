# CLAUDE.md - Database Sync Service Project Documentation

> **Note:** For comprehensive configuration examples and troubleshooting, see [QUICKSTART.md](../QUICKSTART.md) in the parent folder.

## Quick Start

### System Instructions

If necessary reference `../dev_docs_common/PostgreSQLConnectionInfo.txt` (in this `ClaudeProjects/` tree) to connect to db

**On conversation start:** Offer to run a profile completeness audit (see [Profile Completeness Audit](#profile-completeness-audit) below). Tables get added/removed/renamed in prod as apps evolve, so profiles drift over time.

### Run Locally
```bash
dotnet run
```

The dashboard opens automatically at `http://localhost:5123/dashboard`

**LAN access (ubu2):** `http://ubu2.digsol.us:5123/dashboard` — UFW rule allows port 5123 from `10.10.2.0/24`

### HTTP API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/status` | Status of all profiles |
| GET | `/status/{profile}` | Status of specific profile |
| GET | `/profiles` | List profile names |
| POST | `/sync/{profile}` | Trigger sync for profile |
| POST | `/sync/{profile}?fullRefresh=true` | Trigger full refresh sync |
| POST | `/sync` | Trigger all profiles |
| GET | `/history/{profile}` | Sync history (JSON) |
| GET | `/dashboard` | HTML Dashboard |
| GET | `/dashboard/{profile}` | Profile Dashboard |
| POST | `/admin/generate-profile/{profile}` | Generate complete sync profiles for all schemas |

### Trigger Sync via API
```bash
# Trigger specific profile
curl -X POST http://localhost:5123/sync/Production

# Trigger with full refresh
curl -X POST "http://localhost:5123/sync/Production?fullRefresh=true"

# Check status
curl http://localhost:5123/status
```

### Deploy as Service

**Windows (Recommended)**:

The application has native Windows Service support built-in.

```cmd
# 1. Publish as self-contained executable
dotnet publish -c Release -r win-x64 --self-contained true -o C:\Services\DatabaseSync

# 2. Copy your appsettings.json to the publish folder
copy appsettings.json C:\Services\DatabaseSync\

# 3. Install as Windows Service (run as Administrator)
sc create DatabaseSync binPath="C:\Services\DatabaseSync\DatabaseSync.exe" start=auto DisplayName="Database Sync Service"

# 4. Configure automatic restart on failure
sc failure DatabaseSync reset=86400 actions=restart/60000/restart/60000/restart/60000

# 5. Start the service
sc start DatabaseSync
```

**Windows Service Management**:
```cmd
sc stop DatabaseSync      # Stop the service
sc start DatabaseSync     # Start the service
sc query DatabaseSync     # Check status
sc delete DatabaseSync    # Uninstall (stop first)
```

**Linux (systemd)**:
```bash
sudo cp database-sync.service /etc/systemd/system/
sudo systemctl enable database-sync
sudo systemctl start database-sync
```

### Deployment Environments

| Server | Role | SSH | Service Path | Profiles |
|--------|------|-----|-------------|----------|
| **win2** (win2.digsol.us) | SQL Server syncs | `ssh claude@win2.digsol.us` | `C:\Services\DatabaseSync` | LMP_Main, LMP_Archive, LMP_Account (profiles 01-06) |
| **ubu2** (ubu2.digsol.us) | PostgreSQL syncs | `ssh claude@ubu2.digsol.us` | `/opt/services/DatabaseSync` | emp `core`, `emp`, `nxs`, `wgo` schemas (profiles 07-10); acx all schemas (profile 11); rmp `rmp` schema (profile 12); emp `faf` schema (profile 13) |

**Important**: Only deploy SQL Server profiles to win2 and PostgreSQL profiles to ubu2. Mixing causes errors (e.g., PG profiles on win2 fail trying to create `_sync_history` with restricted permissions).

**Profile naming convention**: Profiles are numbered with a **ZERO-PADDED two-digit prefix** (`01-LMP_Main` … `07-CORE`, `08-EMP`, `09-NXS`, `10-WGO`, `11-ACX`, `12-RMP`, `13-FAF`) so that string sorting produces the correct execution order. CORE must sync first since emp, nxs, and wgo schemas have foreign keys to core tables. ACX and RMP each sync independently (separate databases).

> 🔴 **The padding is load-bearing — an unpadded set silently runs in the WRONG ORDER.** Both sort sites compare `ProfileName`/filename as **strings** (`Services/ProfileLoader.cs:77` for load order, `Program.cs:309` for `POST /sync`), and `"10" < "7"` lexically. From the day WGO was renumbered to 10 until 2026-07-25 the live order was `10-WGO, 11-ACX, 12-RMP, 7-CORE, 8-EMP, 9-NXS` — **CORE ran fourth, after WGO**, exactly inverting the documented cross-schema FK guarantee. This CLAUDE.md asserted the opposite the whole time.
>
> It never surfaced as an error because `DisableTriggersDuringLoad` suppresses the FK enforcement that would have made mis-ordering loud, and a full cycle converges anyway once CORE eventually runs — the same "the flag masks an ordering problem" theme as AIM #1497. Do not rely on that: remove the flag, or have CORE fail mid-cycle, and it becomes real.
>
> **When renaming a profile, set `ProfileId` to the OLD name.** `_sync_history` is keyed on `EffectiveProfileId`, which prefers `ProfileId` and falls back to `ProfileName` (`Configuration/SyncServiceConfig.cs:238`). Without the pin, a rename orphans all prior history and every Incremental table silently reverts to a full first sync. All nine renamed profiles carry the pin; verified post-rename that history still resolves (5,849 rows for CORE back to 2026-03-03, and win2's `01-LMP_Main` still returns its runs).

**FAF schema sync** (profile `13-FAF-prodpgsql-devpgsql`, added 2026-07-25): mirrors the `faf` schema of the `emp` database prod→dev (29 tables, daily 05:00, `DisableTriggersDuringLoad` + `AuditReferentialIntegrityAfterSync` + `MaxDeletePercent: 25`). Runs **last** because every faf table has cross-schema FKs into `core` (users/events/talent/venues) — 35 of them — so `07-CORE` must complete first. This is the schema that had no profile at all until now, which is what made it the sole victim of the AIM #1497 orphan bug.

> 🔴 **Every table is `DeleteMode: None` — this profile is deliberately NOT a full-parity mirror, and that is not an oversight.** `faf` is the one schema where dev holds a large, actively-used test corpus that does not exist in prod. Measured 2026-07-25 before building the profile: **~285 dev-only rows** — 194 extra `tickets`, 53 `user_artist_affinities` (prod has **zero**), plus dev-only `orders`, `carts`, `order_items`, `payment_transactions`, `refunds`, `promo_codes`. A `DeleteMode: Sync` profile would delete every one of them on its first run, including the #1287 resale fixtures — precisely the harm #1497 was opened to stop, at ~100× the scale. Do not "fix" this profile by turning deletes on without first reconciling the dev corpus.
>
> **The unavoidable trade-off: upsert-only means PK collisions instead.** Dev allocated its test rows from dev sequences, so their IDs sit inside the range prod has not reached yet — prod `orders` max is 6 while dev's is 41; `carts` 15 vs 77; `payment_transactions` 6 vs 35. As prod grows into that range, the upsert (keyed on PK) silently overwrites the dev row. **This is not theoretical — the first run overwrote 4 dev-only rows (3 `faf.carts`, 1 `faf.refunds`).** Everything else was disjoint: `tickets` 318 + 124 = 442, `orders` 17 + 6 = 23, `ticket_types` 2 + 8 = 10, all exact. (`event_announcements` shows 13,201 "updates" but those were already prod-aligned IDs being refreshed to prod's 16,967 — not collisions.)
>
> **There is no configuration that avoids both losses.** Full parity destroys the dev corpus; upsert-only lets prod progressively overwrite it. The only clean exit is a deliberate reconciliation: either accept prod as authoritative and reseed dev (then switch to `DeleteMode: Sync` and the collision problem disappears because IDs align), or move the dev test corpus into an ID range prod will never reach (e.g. bump dev sequences to 1,000,000+) and keep upsert-only. **Until one of those happens, expect slow attrition of FAF dev test rows.**
>
> `faf.seller_payout_accounts` exists in **dev only** (dev-ahead, from #1287) and is correctly absent from the profile, which is built from the prod table list.

**Priorities (P1→P5) came from a real topological sort, and the FK graph has a CYCLE.** `tickets → order_items → resale_listings → tickets`. A naive topological sort — including the built-in Profile Generator's — cannot resolve it. Break it at the **nullable** edges: `order_items.resale_listing_id` and `cart_items.resale_listing_id` are nullable, while `resale_listings.ticket_id` is `NOT NULL`, so `resale_listings` genuinely must follow `tickets`. With those two edges dropped the graph is acyclic:

| Priority | Tables |
|---|---|
| P1 | carts, event_announcements, event_interest, event_price_history, presales, promo_codes, show_requests, ticket_types, user_artist_affinities, user_music_connections, user_notification_preferences, user_payment_methods, user_tracked_events, venue_sections |
| P2 | announcement_recipients, cart_analytics, cart_items, orders, presale_signups, ticket_type_sections, user_notifications, venue_seats, waitlist_entries |
| P3 | order_items, payment_transactions |
| P4 | refunds, tickets |
| P5 | resale_listings, ticket_transfers |

**When adding any profile, check for FK cycles first** — a nullable FK is the correct place to break one, because a NULL-able child row can be inserted before its parent exists. All 29 tables have single-column PKs, no `GENERATED ALWAYS` columns, and no USER-DEFINED types, so none of the pgvector/enum/tsvector special handling applies here.

**RMP database sync** (profile `12-RMP-prodpgsql-devpgsql`, done 2026-06-22): mirrors the `rmp` schema of the `rmp` database prod→dev (39 tables; FullRefresh + DeleteMode Sync + `DisableTriggersDuringLoad`, daily 05:00). Excluded: `public.spatial_ref_sys` (PostGIS system table), `schemaversions` (DbUp migration tracking — per-environment), and `refresh_tokens`/`password_reset_tokens` (auto-skipped by Rule #132).

> 🔴 **`rmp.platform_admin_actions` → `rmp.audit_log` (2026-08-09, AIM #1946). The chase-don't-delete rule paid off a second time.** The profile carried `rmp.platform_admin_actions`, which does not exist in prod, so every run logged `⊘ … not found - skipped` and reported `38/39 successful, 1 skipped`. It was **not** dropped: RMP migration `0037_retire_platform_admin_actions.sql` (RMP #1483) copied every historical row into the unified `rmp.audit_log` (#1468) and then dropped the table, so the correct fix was to **repoint, not remove** — exactly the `wgo.curator_*` precedent from the 2026-07-25 audit. `audit_log` sits at Priority 3 (parents `rmp.tenants` P1, `rmp.users` P2; no inbound FKs), PK `id` is `GENERATED … AS IDENTITY` (covered by `OVERRIDING SYSTEM VALUE`). Verified: 39/39 tables, 0 skipped, and `audit_log` md5-identical to prod at 272 rows.
>
> **`audit_log.ip_address` is the fleet's first `inet` column and it needed no code change.** `inet` is a base type, not USER-DEFINED, so it gets no `::text` cast; Npgsql returns `System.Net.IPAddress`, which falls through to the COPY writer's final `ToString()` branch and emits valid `inet` literal text. Confirmed empirically by the md5 above rather than by reading the branch — do the same for the next new type. (`jsonb` was already proven across 15 other columns in this profile.)
>
> **Drift audit re-run against prod 2026-08-09**: the only stale entry was the one above, and the only uncovered prod table was `rmp.audit_log` itself. The other four "missing" tables are all deliberate documented exclusions (`spatial_ref_sys`, `schemaversions`, and the two Rule-#132 token tables), so profile 12 is now at full intended coverage — still 39 tables. PostGIS `geography` columns (`communities.geo_boundary`, `listings.geo_point`) sync via the USER-DEFINED `::text` cast (verified md5-identical); generated `listings.description_tsv` is auto-excluded and recomputed on the target. **One-time dev-side setup performed** (target `rmpdev` is a restricted user): `GRANT SET ON PARAMETER session_replication_role TO rmpdev`; `GRANT UPDATE ON ALL SEQUENCES IN SCHEMA rmp TO rmpdev` + matching `ALTER DEFAULT PRIVILEGES FOR ROLE claude`; and `_sync_history` pre-created in the **public** schema (NOT `rmp`) — the rmp database's `search_path = "rmp, public"` means an unqualified create lands in `rmp`, but `PostgreSqlSyncHistoryRepository.InitializeAsync` only existence-checks `public`, so it must live in `public` or the service tries (and fails) to CREATE it as the restricted user.

### Remote Deployment via SSH (win2)

> ✅ **Use `./scripts/deploy-win2.sh`.** It builds on ubu2 (rule #123), **refuses to stop the service while a sync is running** (and treats an unreachable host as BUSY, never as safe), backs up **binaries only — never `secrets.env`**, copies flat files only, then verifies the profile set and runs a **credential canary that fails the deploy**. `--canary` runs the canary alone.
>
> 🔴 **It exists because hand-running this deploy caused two real problems (2026-08-10).** (1) The ad-hoc `Copy-Item -Recurse` backup **duplicated `secrets.env`** every time — two extra copies of three live LMPro credentials in a single day. (2) A canary over the whole tree found **90 LITERAL credentials in 57 files** that #1821 never saw, because that audit only ever checked the *active* `profiles/` directory: `profiles-backup-20260725/` (12, pre-rename), `profiles.Development/` (4, PG profiles that should never have been on win2 at all) and `publish/` (74 — the *same* stale-publish-output mechanism #1821 closed on ubu2 but never cleaned here). All removed; the canary now fails the deploy if any return.
>
> **Every Windows-side step is written to a `.ps1`, copied over and executed** rather than inlined into `ssh "… powershell -Command \"…\""`. That quoting is a minefield — during #1975 an inline form silently produced an *empty* result that compared equal to another empty result and read as a **PASS**. Do not "simplify" it back to one-liners.
>
> The manual steps below are kept for reference and for debugging the script.

Build locally, deploy remotely. The `claude` user on win2 has admin privileges for service management.

```bash
# 1. Publish from the project directory (runs on local Linux machine)
dotnet publish DatabaseSync.csproj -c Release -r win-x64 --self-contained true -o /tmp/DatabaseSync-publish

# 2. Stop the service
ssh claude@win2.digsol.us "sc stop DatabaseSync"

# 3. Copy published files to win2 (flat files only, excludes profiles/ directory)
scp /tmp/DatabaseSync-publish/*.* claude@win2.digsol.us:"C:/Services/DatabaseSync/"

# 4. Copy profile configs (SQL Server profiles only, 01-06)
#    These carry ${PLACEHOLDER}s, not passwords. win2 resolves them from
#    C:\Services\DatabaseSync\secrets.env, which holds the 3 DBSYNC_LMP_* keys and is
#    NEVER overwritten by a deploy. If that file is missing the service will refuse to
#    start (fatal by design) rather than run with unresolved credentials.
scp profiles/0[1-6]*.json claude@win2.digsol.us:"C:/Services/DatabaseSync/profiles/"

# 5. Start the service
ssh claude@win2.digsol.us "sc start DatabaseSync"

# 6. Verify
ssh claude@win2.digsol.us "sc query DatabaseSync"
ssh claude@win2.digsol.us "curl -s http://localhost:5123/health"
```

**Important**: Step 3 uses `scp *.* ` (flat files only) instead of `scp -r` to prevent the `profiles/` directory (which contains all profiles including PG profiles 07-13) from being copied to win2. Never use `scp -r` for the full publish directory — it will deploy PG profiles to win2, causing the dashboard to show PostgreSQL schemas that belong on ubu2. Note: `rsync` is not available on win2's PATH, so use `scp` for file transfer.

**Logs on win2**: `D:\Logs\DatabaseSync\sync-YYYYMMDD.log`

**Checking logs remotely**:
```bash
# Tail recent log entries
ssh claude@win2.digsol.us "powershell -command \"Get-Content 'D:\Logs\DatabaseSync\sync-20260303.log' -Tail 30\""

# Search for errors
ssh claude@win2.digsol.us "powershell -command \"Select-String -Path 'D:\Logs\DatabaseSync\sync-20260303.log' -Pattern 'ERR|fail'\""
```

### Remote Deployment via SSH (ubu2)

> ✅ **Use `./scripts/deploy-ubu2.sh`.** It does everything below *plus* enforces `600 root:root`
> on `profiles/` and `secrets.env`, proves `github-runner` is denied, and runs the credential
> canary. The manual steps are kept for reference and for debugging the script.
>
> 🔴 Two corrections to the older manual steps below, both of which caused real exposure:
> - The build must NOT run on the workstation (rule #123) — it runs on ubu2.
> - `--exclude 'profiles/'` is **unanchored** and matches that directory name at any depth, so
>   `--delete` skips nested copies. Use `--exclude '/profiles/'`. An unanchored pattern is how 36
>   credential files survived in `/opt/services/DatabaseSync/publish/`.
> - `chown -R www-data` is **wrong** for profiles. The unit declares no `User=`, so the service
>   runs as root; profiles must be `600 root:root` (server_event #207).

```bash
# 1. Build ON UBU2 (rule #123 - never on the workstation or the NFS mount)
~/sync-devlocal.sh          # mirror is hourly; sync so ubu2 builds current source
ssh claude@ubu2.digsol.us 'rm -rf /tmp/dbsync && \
  rsync -a --exclude=bin --exclude=obj --exclude="._*" --exclude="profiles/" --exclude="secrets.env" \
        /mnt/devshare/ClaudeProjects/DatabaseSync/ /tmp/dbsync/ && \
  cd /tmp/dbsync && dotnet publish DatabaseSync.csproj -c Release -o /tmp/dbsync/out --nologo'

# 2. Stop, deploy binaries (ANCHORED excludes), start
ssh claude@ubu2.digsol.us "sudo systemctl stop database-sync && \
  sudo rsync -a --delete --exclude='/profiles/' --exclude='/secrets.env' \
       --exclude='/.database-sync.lock' /tmp/dbsync/out/ /opt/services/DatabaseSync/"

# 3. Re-assert modes - a copy at the default umask silently re-widens them
ssh claude@ubu2.digsol.us "sudo chown -R root:root /opt/services/DatabaseSync/profiles && \
  sudo chmod 750 /opt/services/DatabaseSync/profiles && \
  sudo find /opt/services/DatabaseSync/profiles -type f -exec chmod 600 {} + && \
  sudo chmod 600 /opt/services/DatabaseSync/secrets.env"

# 4. Profiles only when changed (PG profiles only, 07-13). They carry ${PLACEHOLDER}s,
#    not passwords - the values live in secrets.env, which is NEVER copied from the workstation.
scp profiles/0[7-9]*.json profiles/1[0-3]*.json claude@ubu2.digsol.us:/tmp/
ssh claude@ubu2.digsol.us "sudo cp /tmp/*.json /opt/services/DatabaseSync/profiles/ && \
  sudo chown root:root /opt/services/DatabaseSync/profiles/*.json && \
  sudo chmod 600 /opt/services/DatabaseSync/profiles/*.json && rm -f /tmp/*.json"

# 5. Start and verify
ssh claude@ubu2.digsol.us "sudo systemctl start database-sync"
ssh claude@ubu2.digsol.us "sudo systemctl is-active database-sync"
ssh claude@ubu2.digsol.us "curl -s http://localhost:5123/health"
# Confirm resolution fired - "N of N profile(s) use placeholders, 0 unresolved"
ssh claude@ubu2.digsol.us "journalctl -u database-sync --since '-2min' -q | grep 'Secret resolution'"
```

**Note**: The `rsync --exclude 'profiles/'` pattern prevents the build output's profiles directory (which may contain SQL Server profiles from the source tree) from overwriting the server's PG-only profiles. Always update profiles separately in step 5.

**Logs on ubu2**: `/var/log/services/DatabaseSync/sync-YYYYMMDD.log`

> 🔴 **This path was WRONG in practice for five months, and the failure was invisible (AIM #1946, fixed 2026-08-09).** `appsettings.json` sets `"LogPath": "D:/Logs/DatabaseSync"` — correct for win2, meaningless on Linux. `Path.IsPathRooted("D:/…")` returns **false** on Linux, so the path was joined onto the app base directory and Serilog silently created a directory literally named `D:`. Logs went to `/opt/services/DatabaseSync/D:/Logs/DatabaseSync/` while the directory documented here sat frozen at `sync-20260304.log`. Anyone following this runbook during an incident read a five-month-old file and would reasonably conclude the service had not run — which nearly happened, since the first greps for a skip reason returned nothing.
>
> **The fix is a guard in `Program.cs`, not a config override.** `appsettings.Production.json` cannot work: **both hosts run `DOTNET_ENVIRONMENT=Production`**, so a Linux path there would break win2. Instead, a drive-letter prefix on a non-Windows OS now falls back to `/var/log/services/DatabaseSync` and prints why at startup. Override per host with `SyncService__LogPath` (the early config builder now reads environment variables, which it previously did not — only the main builder did). The stray `D:` tree was removed by the deploy's `--delete`.
>
> **journalctl carried the same output the whole time**, which is exactly why nothing looked broken: `journalctl -u database-sync` is the reliable cross-check when a log path is in doubt.

**Checking logs remotely**:
```bash
# Tail recent log entries
ssh claude@ubu2.digsol.us "tail -30 /var/log/services/DatabaseSync/sync-20260303.log"

# Search for errors
ssh claude@ubu2.digsol.us "grep -E 'ERR|fail' /var/log/services/DatabaseSync/sync-20260303.log"
```

**Service management on ubu2**:
```bash
sudo systemctl stop database-sync     # Stop
sudo systemctl start database-sync    # Start
sudo systemctl restart database-sync  # Restart
sudo systemctl status database-sync   # Status
journalctl -u database-sync -f        # Follow live logs
```

---

## Profile Completeness Audit

Sync profiles drift over time as application schemas evolve — tables get added, renamed, or dropped in production. Run this audit periodically to keep profiles in sync with reality.

### Process

1. **Read each PG profile file** (`profiles/7-*.json` through `profiles/12-*.json`) to get the list of configured source tables per profile.

2. **Query the production database** for all actual user tables per schema:
   ```sql
   -- For emp database (core, emp, nxs, wgo schemas):
   SELECT table_schema || '.' || table_name
   FROM information_schema.tables
   WHERE table_type = 'BASE TABLE'
     AND table_schema IN ('core', 'emp', 'nxs', 'wgo')
   ORDER BY table_schema, table_name;

   -- For acx database (all non-system schemas):
   SELECT table_schema || '.' || table_name
   FROM information_schema.tables
   WHERE table_type = 'BASE TABLE'
     AND table_schema NOT IN ('information_schema', 'pg_catalog')
   ORDER BY table_schema, table_name;
   ```
   Use connection strings from the profile files themselves.

3. **Compare** each profile's table list against the prod query results:
   - **Missing tables**: Exist in prod but not in the profile (excluding restricted tables: `db_environment`, `_sync_history`, and EF migration tables like `__EFMigrationsHistory`)
   - **Stale tables**: In the profile but no longer exist in prod (these get skipped with a warning during sync but add noise)

4. **Assign priorities** for new tables using FK dependencies:
   ```sql
   -- Query FK relationships for new tables:
   SELECT
       ns.nspname || '.' || cl.relname AS child_table,
       nsr.nspname || '.' || clr.relname AS parent_table
   FROM pg_constraint c
   JOIN pg_class cl ON c.conrelid = cl.oid
   JOIN pg_namespace ns ON cl.relnamespace = ns.oid
   JOIN pg_class clr ON c.confrelid = clr.oid
   JOIN pg_namespace nsr ON clr.relnamespace = nsr.oid
   WHERE c.contype = 'f'
     AND ns.nspname || '.' || cl.relname IN (/* new tables */)
   ORDER BY 1, 2;
   ```
   - Tables with no FKs or FKs only to Priority 1 tables → same priority as peers
   - Tables with FKs to higher-priority tables → one level below their parent
   - Cross-schema FKs (e.g., `wgo.promotions` → `core.users`) are handled by profile execution order, not within-profile priority

5. **Update profiles**: Add missing tables, remove stale entries, deploy to ubu2, and verify table counts via the health API.

6. **Check every new table has a PRIMARY KEY before adding it.** The upsert requires one; a PK-less table fails the whole table with `No primary key found - cannot perform upsert` and drops the profile to `success=false`. Caught the hard way 2026-07-25: `wgo.trending_chip_counts` was added from the missing-tables list and immediately failed, because it has no PK. Query it up front:
   ```sql
   SELECT t.table_schema||'.'||t.table_name,
          COALESCE((SELECT string_agg(kcu.column_name,',')
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema
                    WHERE tc.table_schema=t.table_schema AND tc.table_name=t.table_name
                      AND tc.constraint_type='PRIMARY KEY'), '(NONE)') AS pk
   FROM information_schema.tables t WHERE t.table_schema||'.'||t.table_name IN (/* candidates */);
   ```

7. **Grant sequence UPDATE for any newly-covered schema** — see the sequence-permissions note under Database User Conventions. Adding tables from a schema the sync user has never written to means its sequences were never granted.

### Audit results — 2026-07-25 (run against prod, AIM #1497 follow-up)

Compared all six PG profiles against `prodpgsql`. **9 tables were missing, 3 were stale.**

Added (priority from FK depth within each profile; cross-schema FKs are handled by profile order, not priority):

| Profile | Table | Priority | Parents |
|---|---|---|---|
| 07-CORE | `core.user_push_subscriptions` | 2 | core.users (P1) |
| 07-CORE | `core.curator_activity` | 3 | core.curators (P2) |
| 07-CORE | `core.curator_favorite_genres_tags` | 4 | core.curator_favorite_genres (P3) |
| 07-CORE | `core.curator_favorite_talent_tags` | 4 | core.curator_favorite_talent (P3) |
| 07-CORE | `core.curator_favorite_events_tags` | 6 | core.curator_favorite_events (P5) |
| 08-EMP | `emp.comp_issuances` | 1 | core.events/users only (cross-schema) |
| 08-EMP | `emp.refund_authorizations` | 1 | core.events/users only (cross-schema) |
| 09-NXS | `nxs.event_setlists` | 4 | nxs.setlists (P3) |

Removed as stale — these did not vanish, they **moved to the `core` schema** and were restructured: `wgo.curator_activity` → `core.curator_activity`, and `wgo.curator_genres` / `wgo.curator_tags` → the three `core.curator_favorite_*_tags` tables. A profile entry that logs `source table not found` is worth chasing rather than deleting; the table is often alive under a new name.

Rejected: `wgo.trending_chip_counts` — exists in prod but has **no primary key** (see step 6).

**Dev-only tables are not drift.** Seven `emp.guest_*`/`emp.comp_*` tables show up when the comparison is run against *dev* but are absent from prod, so they are correctly uncovered. Always compare against **prod**, which is the source of truth for what a mirror should carry.

Post-audit table counts: 07-CORE 42, 08-EMP 5, 09-NXS 15, 10-WGO 38, 11-ACX 55, 12-RMP 39.

### Forced full refresh is capped by row count (AIM #1966)

**`ForceFullRefresh` is all-or-nothing per day-schedule** — it overrides *every* table's `Mode`, regardless of size. That is how a 137M-row archive table came to be fully re-staged weekly.

**The failure it fixes:** `tbl_Archive_Track` (137,048,815 rows) failed **every Sunday**, on both LMP_Archive profiles, with `SqlException 9002: The transaction log for database 'tempdb' is full due to 'ACTIVE_TRANSACTION'`. It burned 2h42m and 1h07m before dying with **0 rows synced**. Measured across 20 daily logs: 0 failures on all 17 weekdays, failures on all 3 Sundays, **escalating 0 → 4 → 8 tempdb errors** (2026-07-26 failed differently — an execution timeout — which is the same "too big for this strategy" problem at an earlier stage, not the same error).

**Both targets failed identically** (`stgsql` on AWS and win2), which is what ruled out per-host tempdb sizing: two independently-administered instances failing the same way points at the operation, not the box. Resizing tempdb would have moved the failure, not fixed it.

- **The guard is a row THRESHOLD, deliberately not a per-table opt-out flag.** A table that grows past 3M is exempted automatically; a hand-maintained flag list rots silently, which is the exact drift pattern that let this run unnoticed for weeks.
- **The estimate is metadata, never `COUNT(*)`** — `sys.partitions` (SQL Server) / `pg_class.reltuples` (PG). Counting 137M rows to decide "is this table too big to scan" would be self-defeating. `GetEstimatedRowCountAsync` returns **null** when unknown, and callers must treat null as "change nothing" — never as zero.
- **The decision runs BEFORE the "Syncing {Table} (Mode: …)" log line**, so the log never claims `FullRefresh (forced)` for a table that was actually exempted. Getting that order wrong makes the logs lie about the very thing you are debugging.
- **Proven with a two-sided control, per the standing rule here** — a guard that never fires and a guard that always fires both look "green". On `nxs.setlist_songs` (6,114 rows): at the default 3M threshold it correctly did **not** fire (`FullRefresh (forced)`); at a threshold of 100 it did (`keeping configured mode Incremental`), and rows processed fell 6,736 → 1,943.

**Ten table entries were ALSO configured `Mode: FullRefresh` outright** despite exceeding 3M rows — the guard does not touch those, since it only suppresses a *forced* refresh. They were switched to `Incremental`:

| Profile | Table | Rows |
|---|---|---|
| 01/02 | `tbl_Account_Contact` | 3,556,332 |
| 05/06 | `tbl_OrderToInvoice` | 9,877,742 |
| 05/06 | `tbl_OrderInvoice` | 9,877,725 |
| 05/06 | `tbl_OrderToSettle` | 7,135,115 |
| 05/06 | `tbl_OrderSettlement` | 6,798,595 |

> 🔴 **They all carried `TimestampColumn: EntryDT` AND `FallbackTimestampColumn: EntryDT` — the same creation-date column twice.** Flipping them to Incremental as-configured would have silently missed every in-place update, because `EntryDT` does not change when a row is edited. All five have a `LastEditDT`, so they now use the documented `COALESCE(LastEditDT, EntryDT)` pattern. **Check for a modification column before converting any table to Incremental** — a redundant fallback is a warning sign that nobody checked.

> ⚠ **`max_source_timestamp` was NULL across 240+ prior runs** (a FullRefresh never records one), so `LookbackHours` is **load-bearing on the first incremental run**: with no history and no lookback, the code falls back to "sync all data" and reproduces the full scan you were escaping. `LookbackHours: 2160` (90 days) was sized from measured edit volume — ~3× the 30-day figure.

> ✅ **The first-run double-lookback noted here is FIXED (AIM #1975).** It was real — `2160h → 180 days`, and `168h → 336h` on a second table — because the guard inferred "have I already applied this?" from a timestamp comparison that is *always* true: the no-history branch sets `lastSyncTime = UtcNow - Lookback`, and the guard then re-evaluates `UtcNow - Lookback` milliseconds later, which is fractionally larger. Provenance is now tracked with an explicit flag instead of re-derived from the value. Verified: both log lines now show the same timestamp.

**Result on profile 05** (same profile, same forced-full-refresh trigger, before → after): **34,342,123 rows in 1h 23m 44s → 2,912,645 rows in 11m 29s**, 17/17 tables both times.

### 🔴 `_sync_history` overstated success — a skipped table wrote `success=true` (AIM #1946)

**This is the trap that makes a drift audit read as clean.** A missing source/target table sets `Success = true; Skipped = true` (`SyncOrchestrator.cs`, the two `goto RecordHistory` paths) so it correctly does **not** count as a failure. But `_sync_history` had no way to express "skipped", so the row landed as a plain success. The run summary said `38/39 tables, 1 skipped` while history for the same `run_id` said **39 rows, 39 success, 0 failed** — history contradicted the summary and read as "everything synced". Because the skip was invisible in history, **history could not be used to date when the skip started**, which is the one thing you want it for.

That matters more than it sounds: this file tells you to use `_sync_history` for freshness checks precisely because `systemctl is-active` is not a health check here. A freshness tool that overstates success is worse than no tool.

**Fixed by adding a `skipped BOOLEAN NOT NULL DEFAULT FALSE` column** rather than suppressing the row — a row recording "we deliberately did nothing" is useful; a row claiming success is not. Present in both repositories' `CREATE TABLE`, in the idempotent `ADD COLUMN` migration each runs at startup, in the INSERT, and in both read queries. The dashboard renders `SKIP`, and the 24h success rate now **excludes** skipped rows from both sides of the ratio instead of scoring them as wins.

- **Each repository PROBES for the column instead of assuming it.** `_sync_history` is frequently pre-created by an admin user while the sync user holds DML-only rights (it is `postgres`-owned on all three PG targets), so the `ALTER TABLE` is denied with `42501`. Emitting an INSERT naming a column the table lacks would fail with `42703` and take down **all** history recording — far worse than the bug being fixed — so the INSERT and SELECTs adapt to what is actually there, and a missing column logs a WARNING with the exact DDL to run.
- **The column was added manually as `claude` on `emp`, `acx`, and `rmp`** (devpgsql) for that reason. SQL Server targets self-migrate on next start, since those sync users do hold DDL rights.
- **Backfilled 559 historical rows** (`emp` 524, `rmp` 35) matching `error_message LIKE '%not found - skipped'`. All 12 distinct tables were **already-resolved** drift — the three `wgo.curator_*` rows stop dead at 2026-07-25 (when that audit removed them) and the `7-CORE` ones at 2026-04-14 — confirming `rmp.platform_admin_actions` was the fleet's only live skip.
- **Proven with a controlled probe, not just observed green** (the standing rule here): a deliberately non-existent `rmp.__probe_1946_missing` was added to profile 12, synced, and recorded `success=t, skipped=t` while the other 39 recorded `skipped=f` — history read 39 real successes + 1 skipped, matching the summary's 39/40. Pre-fix the same run would have read 40/40/0. Probe entry and its history row were then removed.

### ✅ `_sync_history.rows_deleted` was ALWAYS 0 on PG→PG two-phase profiles — FIXED (AIM #1964)

Third and last member of the "history misstates the run" family, after the two closed by #1946.

**The measurement:** `max(rows_deleted) = 0` across **all 25,372 history rows of all 7 PG profiles**, while the same runs demonstrably deleted rows (profile 12's summary reported 14 deletes and `rmp.audit_log` went 286 → 272). The contrast that proved the diagnosis: the **SQL Server** targets showed `max rows_deleted = 3,775,721`, because they delete *inline* within each table's sync.

**Mechanism:** PG→PG with any `DeleteMode: Sync` switches to two-phase sync. Phase 1 runs `SyncTableAsync(skipDelete: true)` and writes the history row — correctly 0, since deletes were skipped. Phase 2 then deletes and only did `existingResult.RowsDeleted = deletedCount`, mutating the **in-memory** `SyncResult`. That is precisely why the run summary and dashboard were always right while history was always wrong.

**Fix:** `ISyncHistoryRepository.UpdateDeleteCountAsync(runId, sourceTable, rowsDeleted)`, called from the phase-2 success branch, keyed on `(run_id, source_table)` which is unique within a run. It **must never throw** — failing to annotate history is not a reason to fail a sync that already moved data — so both implementations swallow and log, and log a WARNING if they match 0 rows.

- **Rejected: deferring the phase-1 write until after phase 2.** It yields cleaner data but means a mid-run crash loses *all* of that run's history instead of everything up to the crash — a real loss of diagnostic value on the exact runs you most need to diagnose.
- **Rejected: a second delete-phase history row.** Doubles the rows and every existing consumer would have to learn to fold them.
- **Proven with a controlled probe against a PREDICTED number, not merely "non-zero":** 7 dev-only rows were planted in `rmp.audit_log` (cloned from valid rows — `id` is `GENERATED ALWAYS`, so it needs `OVERRIDING SYSTEM VALUE`, and there are `CHECK` constraints on `action`). The parity delete removed exactly 7, the summary said 7, and `_sync_history.rows_deleted` recorded **7** — against `max = 0` across every prior run.

> **The SQL Server path needed no change** and the new method is a no-op there: the phase-2 call site sits inside the PG→PG-only two-phase branch, so it is never reached on a SQL Server target.

### Tables and Data That Cannot or Should Not Sync

**Auto-excluded by DatabaseSync** (filtered regardless of profile config):

| Table/Pattern | Reason |
|---------------|--------|
| `db_environment` | Environment-specific settings that must differ per env |
| `_sync_history` | Managed by the DatabaseSync service itself |
| `*refresh_tokens` | Session credentials — syncing leaks prod sessions to dev (AIM Rule #132) |
| `*password_reset_tokens` | Short-lived tokens with prod-domain URLs (AIM Rule #132) |
| `*email_verification_tokens` | Short-lived tokens with prod-domain URLs (AIM Rule #132) |
| `*verification_tokens` | Environment-specific verification records (AIM Rule #132) |
| `*sessions` | Active user session state, environment-specific (AIM Rule #132) |

> **Note:** The token/session exclusion uses suffix matching. Tables like `shared.user_token_budgets` (which tracks API token quotas, not auth tokens) are NOT matched. If a table name ends with a restricted suffix but contains business data, remove it from the suffix list and add a code comment explaining the exception.

**Exclude from profiles manually:**

| Table/Pattern | Reason |
|---------------|--------|
| `__EFMigrationsHistory` | EF Core migration tracking — per-environment state |
| Tables with no primary key (e.g., `hangfire.lock`) | Upsert requires a PK; sync will fail with "No primary key found" |
| Hangfire runtime tables (`hangfire.server`, `hangfire.lock`) | Transient per-instance state; syncing overwrites dev's active job runtime |

**Data types with special handling** (sync works, but be aware):

| Type | Example | Behavior |
|------|---------|----------|
| pgvector `vector` | `media.face.embedding`, `media.image_meta.embedding`, `interests.items.embedding` | PG→PG: source SELECT casts USER-DEFINED columns to `::text` so Npgsql reads them without a plugin; the text form (`[1,2,3]`) round-trips into the target `vector` column via COPY. (The older try/catch `GetFieldValue<string>` fallback only ever "worked" because `interests.items` had all-NULL embeddings — the first non-null vectors, in `media.*`, exposed and drove this fix.) |
| Custom enums | `core.curators.curator_tier` (curator_tier_enum) | Works — USER-DEFINED `::text` cast (above) covers enums too (label round-trips into the target enum column) |
| GENERATED ALWAYS AS … STORED | `media.image_meta.search_vector` (stored tsvector) | PG→PG: auto-excluded from insert/update column lists (detected via `is_generated='ALWAYS'`) — cannot be inserted into; the target recomputes them. Distinct from **trigger**-maintained `search_vector` columns (e.g. `media.location`, `media.audio_meta`), which are handled by `DisableTriggersDuringLoad` |

**During audit, also check for:**
- Materialized views (not base tables — won't appear in table list but could be confused)
- Partitioned tables (parent partitioned table can't be directly COPYed into)
- New schemas that might need their own profile

---

## Project Overview

A high-performance, standalone database synchronization service that supports **bi-directional sync** between **Microsoft SQL Server** and **PostgreSQL**. Designed for any database sync scenario requiring reliable, scheduled data replication.

### Supported Sync Combinations

| Source | Target | Status |
|--------|--------|--------|
| SQL Server | PostgreSQL | Supported |
| SQL Server | SQL Server | Supported |
| PostgreSQL | PostgreSQL | Supported |
| PostgreSQL | SQL Server | Supported |

### Key Design Decisions

1. **Multi-Profile Architecture**: Each profile represents a source/target database pair with its own schedule and table list. This allows syncing multiple databases with a single service instance.

2. **Timer + HTTP API**: Simple in-process scheduler with configurable intervals. No external dependencies like pg_cron. Cross-platform compatible (Windows and Linux).

3. **Staging Table + Upsert Pattern**: Instead of row-by-row operations, data is bulk-loaded to a temp staging table, then upserted in a single SQL statement. This achieves 50,000+ rows/second throughput.
   - PostgreSQL targets: Uses `COPY` protocol + `INSERT ... ON CONFLICT`
   - SQL Server targets: Uses `SqlBulkCopy` + `MERGE` statement

4. **Priority Groups**: Tables are grouped by priority number. Same-priority tables sync in parallel (up to MaxParallelTables), different priorities sync sequentially. This respects foreign key dependencies.

5. **Synchronized Deletes**: Simple delete mode - if a row exists in target but not in source, delete it from target. No soft-delete complexity.

---

## Minimal Configuration Example

```json
{
  "SyncService": {
    "HttpPort": 5123,
    "LogPath": "D:/Logs/DatabaseSync",
    "Profiles": [
      {
        "ProfileName": "my-sync",
        "Description": "SQL Server to PostgreSQL sync",

        "SourceConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=source.example.com;Database=MyDB;User Id=user;Password=pass;TrustServerCertificate=True"
        },

        "TargetConnection": {
          "Type": "PostgreSql",
          "ConnectionString": "Host=target.example.com;Database=mydb;Username=user;Password=pass"
        },

        "Schedule": {
          "StartTime": "06:00",
          "IntervalMinutes": 60,
          "RunImmediatelyOnStart": true,
          "Enabled": true
        },

        "Options": {
          "MaxParallelTables": 4,
          "CommandTimeoutSeconds": 300
        },

        "Tables": [
          {
            "SourceTable": "Customers",
            "TargetTable": "customers",
            "Mode": "Incremental",
            "TimestampColumn": "ModifiedDate",
            "DeleteMode": "Sync"
          },
          {
            "SourceTable": "Orders",
            "TargetTable": "orders",
            "Mode": "Incremental",
            "TimestampColumn": "LastUpdated",
            "LookbackHours": 24,
            "DeleteMode": "Sync"
          }
        ]
      }
    ]
  }
}
```

---

## External Profile Configuration

For better maintainability and scalability, profiles can be stored in separate JSON files instead of embedding them in `appsettings.json`.

### Benefits

- **Cleaner configuration**: Main `appsettings.json` stays small and focused
- **Easier maintenance**: One file per profile makes changes easier to track
- **Version control friendly**: See exactly which profile changed in git diffs
- **Environment-specific**: Different profile sets for dev/staging/prod
- **Backward compatible**: Existing inline profiles continue working

### Directory Structure

```
DatabaseSync/
├── profiles/                    # Base profiles (all environments)
│   ├── my-sync-1.json
│   ├── my-sync-2.json
│   └── reporting-sync.json
├── profiles.Development/        # Development-only profiles (optional)
│   └── local-test-sync.json
├── profiles.Production/         # Production-only profiles (optional)
│   └── prod-only-sync.json
└── appsettings.json            # Main config + optional inline profiles
```

### External Profile File Format

Each profile file contains a single `SyncProfile` object (not wrapped in an array):

```json
{
  "ProfileName": "my-sync",
  "Description": "SQL Server to PostgreSQL sync",
  "SourceConnection": {
    "Type": "SqlServer",
    "ConnectionString": "Server=source.example.com;Database=MyDB;..."
  },
  "TargetConnection": {
    "Type": "PostgreSql",
    "ConnectionString": "Host=target.example.com;Database=mydb;..."
  },
  "Schedule": {
    "StartTime": "06:00",
    "IntervalMinutes": 60,
    "RunImmediatelyOnStart": true,
    "Enabled": true
  },
  "Options": {
    "MaxParallelTables": 4,
    "CommandTimeoutSeconds": 300
  },
  "Tables": [
    {
      "SourceTable": "Customers",
      "TargetTable": "customers",
      "Mode": "Incremental",
      "TimestampColumn": "ModifiedDate",
      "DeleteMode": "Sync"
    }
  ]
}
```

### Configuration Settings

Add to `appsettings.json`:

```json
{
  "SyncService": {
    "EnableExternalProfiles": true,
    "ProfilesDirectory": "profiles",
    "Profiles": []  // Optional inline profiles for backward compatibility
  }
}
```

| Setting | Default | Purpose |
|---------|---------|---------|
| `EnableExternalProfiles` | `true` | Enable loading profiles from external files |
| `ProfilesDirectory` | `"profiles"` | Directory containing external profile JSON files |

### Loading Order and Precedence

Profiles are loaded with **last-wins** precedence:

1. **Inline profiles** from `appsettings.json`
2. **Base profiles** from `profiles/` directory
3. **Environment-specific** from `profiles.{Environment}/` directory

**Duplicate handling**: If the same `ProfileName` appears multiple times, the last-loaded profile wins. A warning is logged for each duplicate.

**Example**: If you have a profile named "Production" in both `appsettings.json` and `profiles/Production.json`, the external file version will be used (and a warning logged).

### Migration from Inline to External

**Option 1: Keep inline (no changes required)**
```json
{
  "SyncService": {
    "EnableExternalProfiles": false,
    "Profiles": [ /* existing profiles */ ]
  }
}
```

**Option 2: Move to external files**

1. Create `profiles/` directory in the application root
2. For each profile, create `profiles/{name}.json` with the profile content
3. Remove the inline `Profiles` array from `appsettings.json` (or set to `[]`)
4. Restart the service

**Option 3: Hybrid approach (gradual migration)**

Keep some profiles inline while adding new ones as external files. Both work simultaneously:

```json
{
  "SyncService": {
    "EnableExternalProfiles": true,
    "ProfilesDirectory": "profiles",
    "Profiles": [
      {
        "ProfileName": "legacy-sync",
        "Description": "Old inline profile"
        // ... rest of config
      }
    ]
  }
}
```

Plus external files in `profiles/` for new profiles.

### Environment-Specific Profiles

Create environment-specific directories to load different profiles per environment:

- `profiles/` - Loaded in all environments
- `profiles.Development/` - Loaded only in Development environment
- `profiles.Production/` - Loaded only in Production environment
- `profiles.Staging/` - Loaded only in Staging environment

The environment is detected from `DOTNET_ENVIRONMENT` or `ASPNETCORE_ENVIRONMENT` environment variable.

**Example use case**: Load a test profile only in Development:

```bash
# Create Development-only profile
mkdir profiles.Development
echo '{ "ProfileName": "dev-test", ... }' > profiles.Development/dev-test.json

# Set environment (Linux/macOS)
export DOTNET_ENVIRONMENT=Development

# Set environment (Windows)
set DOTNET_ENVIRONMENT=Development

# Run service - dev-test profile will load
dotnet run
```

### Error Handling

External profile loading is designed to be resilient:

| Error Type | Behavior | Log Level |
|------------|----------|-----------|
| Directory not found | Skip directory, continue | Warning |
| File not readable | Skip file, continue | Error |
| Invalid JSON | Skip file, continue | Error |
| Missing ProfileName | Skip file, continue | Error |
| Duplicate ProfileName | Use last-loaded, warn | Warning |

**Philosophy**: One bad profile file shouldn't prevent other profiles from loading. All errors are logged but don't crash the service.

### Validation

After loading, all profiles (inline and external) are validated together. If a validation error occurs, the error message includes the profile name to help identify which file has the issue.

---

## Features

| Feature | Notes |
|---------|-------|
| Multi-profile configuration | Each profile has its own connections, schedule, tables |
| Bi-directional sync | All 4 source/target combinations supported |
| SQL Server / PostgreSQL type mapping | Handles all common types both directions |
| Bulk upsert via staging tables | High performance for all database combinations |
| Incremental sync (timestamp-based) | Only syncs rows changed since last run |
| Full refresh sync | Upserts all rows |
| Sync history tracking | `_sync_history` table with per-table stats |
| Automatic last sync time | Uses history for incremental resume |
| Parallel table processing | Configurable MaxParallelTables |
| Priority-based ordering | Respects table dependencies |
| Synchronized deletes | Deletes rows from target not in source |
| HTTP API for control | Status, trigger, health endpoints |
| Scheduled execution | StartTime + IntervalMinutes |
| Day-specific scheduling | Different intervals and sync modes per day of week |
| Blackout window | Prevent syncs during maintenance/backup windows |
| Incremental with lookback | Re-sync recent data to catch late-arriving changes |
| Automatic column filtering | Skip source columns not in target table |
| Load-based throttling | Pause sync when source server CPU is high |
| Auto-create target tables | CreateIfMissing option |
| Source data filtering | SourceFilter WHERE clause |
| Single-instance enforcement | File lock prevents multiple instances from running |
| Batched MERGE for large tables | SQL Server targets batch MERGE in 1M row chunks for tables >1M rows |
| NOLOCK hints (SQL Server) | Reduce blocking on source database with WITH (NOLOCK) |
| Source row batching | Read source data in batches to reduce memory pressure |
| WIN1252 encoding support | Auto-detect target encoding and sanitize Unicode characters |
| Restricted table filtering | Automatically skip db_environment and _sync_history tables |
| PostgreSQL array support | Handle text[], integer[], and other array types in sync |
| GENERATED ALWAYS AS IDENTITY support | `OVERRIDING SYSTEM VALUE` clause for PG identity columns (backward-compatible with SERIAL) |
| Unsupported type fallback | PG→PG: source SELECT casts USER-DEFINED columns (pgvector `vector`, enums) to `::text`; text round-trips into the matching target column via COPY |
| GENERATED ALWAYS column exclusion | PG→PG: STORED generated columns (e.g. a tsvector `search_vector`) are auto-excluded from insert/update (detected via `is_generated='ALWAYS'`); the target recomputes them |
| Profile generator | Auto-generate complete sync profiles from source database schema |
| Graceful missing table handling | Skip missing source/target tables with warning instead of failing |
| Alphabetical profile ordering | Profiles execute in sorted order for cross-schema FK dependencies |
| Target-side sequence reset | Queries target DB for sequences after sync to prevent duplicate key errors |
| FK-safe mirror loads (`DisableTriggersDuringLoad`) | PG→PG: run target session with `session_replication_role='replica'` so self-referential/inbound FKs don't block unique-constraint recovery or orphan deletes (PK/UNIQUE still enforced) |
| Post-sync referential-integrity audit | PG targets: count child rows with no surviving parent across every inbound FK of every synced table; flags orphans in child tables outside the profile's sync scope, which never self-heal |
| Dev-fixture protection (`DeleteExclusionFilter`) | Target-side WHERE clause exempting rows from parity deletes, so a prod→dev mirror stops destroying dev-only test accounts |
| Delete-ratio safety ceiling (`MaxDeletePercent`) | Aborts a delete pass that would remove more than N% of the target (tables ≥ 100 rows), closing the PG path's unbounded-delete hole |

---

## Full Configuration Model

```
SyncService
├── HttpPort
├── LogPath (default: "logs", can be absolute path like "D:/Logs/DatabaseSync")
├── ProfileExecutionMode (Parallel/Sequential)
├── BlackoutWindow
│   ├── Enabled
│   ├── StartTime ("HH:mm")
│   └── EndTime ("HH:mm")
├── LoadThrottling
│   ├── Enabled
│   ├── MaxCpuPercent (default: 60)
│   ├── MaxActiveQueries (default: 50)
│   ├── CheckIntervalSeconds (default: 30)
│   ├── MaxWaitMinutes (default: 30)
│   └── CheckTiming (BeforeProfile/BeforeTable/Both)
├── Profiles[]
│   ├── ProfileName
│   ├── Description
│   ├── SourceConnection (Type, ConnectionString)
│   ├── TargetConnection (Type, ConnectionString)
│   ├── Schedule
│   │   ├── StartTime ("HH:mm")
│   │   ├── IntervalMinutes
│   │   ├── RunImmediatelyOnStart
│   │   ├── Enabled
│   │   ├── DaysOfWeek[] (legacy - use Schedules for day-specific modes)
│   │   └── Schedules[] (day-specific schedules)
│   │       ├── Days[] (day names: "Monday", "Tuesday", etc. or "Mon", "Tue", etc.)
│   │       ├── IntervalMinutes (optional, inherits from parent)
│   │       ├── StartTime (optional, inherits from parent)
│   │       └── ForceFullRefresh (override tables to use FullRefresh)
│   ├── Options
│   │   ├── MaxParallelTables
│   │   ├── CommandTimeoutSeconds
│   │   ├── EnableSyncHistory
│   │   ├── UseHistoryForIncrementalSync
│   │   ├── StopOnError
│   │   ├── UseNoLock (default: true - SQL Server sources only)
│   │   └── SourceBatchSize (default: 100000)
│   └── Tables[]
│       ├── SourceTable
│       ├── TargetTable
│       ├── Mode (FullRefresh/Incremental)
│       ├── TimestampColumn
│       ├── FallbackTimestampColumn
│       ├── LookbackHours
│       ├── Priority
│       ├── DeleteMode (None/Sync)
│       ├── SyncAllDeletes
│       ├── CreateIfMissing
│       └── SourceFilter
```

---

## Configuration Options Reference

### Profile Options

| Option | Default | Purpose | When to Change |
|--------|---------|---------|----------------|
| `MaxParallelTables` | `4` | Number of tables to sync concurrently | Increase for many independent tables; decrease if database can't handle load |
| `CommandTimeoutSeconds` | `300` | SQL command timeout | Increase for very large tables (millions of rows) |
| `EnableSyncHistory` | `true` | Track sync results in `_sync_history` table | Disable only if you don't need history/incremental sync |
| `UseHistoryForIncrementalSync` | `true` | Use `_sync_history` to find last sync time | Should almost always be `true` for incremental mode |
| `StopOnError` | `false` | Stop entire profile if one table fails | Set `true` when tables have dependencies |
| `UseNoLock` | `true` | Use WITH (NOLOCK) on SQL Server source queries | Set `false` if you need guaranteed consistency (rare) |
| `SourceBatchSize` | `100000` | Rows to read per batch from source | Decrease to reduce source DB memory pressure; set to 0 to disable batching |
| `DisableTriggersDuringLoad` | `false` | PG→PG only: run the **target** load session with `session_replication_role = 'replica'`, suppressing FK/RI triggers so the unique-constraint recovery delete and orphan deletes aren't blocked by self-referential or inbound FKs (PK/UNIQUE still enforced) | Set `true` on full prod→dev **mirror** profiles. **Never on a prod target.** Requires the target user to be a superuser or hold `GRANT SET ON PARAMETER session_replication_role` (PG 15+). Enabled on all 6 PG mirror profiles (7-CORE…12-RMP). **Suppressing RI also removes the parity-delete's blast-radius protection** — pair it with `AuditReferentialIntegrityAfterSync` and `DeleteExclusionFilter` below. See [FK self-reference fix](#fix-self-referential-fk-blocks-unique-constraint-recovery) |
| `ForceFullRefreshMaxRows` | `3000000` | Row ceiling for a **forced** full refresh (a day-schedule `ForceFullRefresh`, never a table's own `Mode`). A table whose *estimated* row count exceeds this keeps its configured `Mode`. `0` disables the guard | Leave at the default. `ForceFullRefresh` is **all-or-nothing per schedule**, so before this existed the Sunday entry flipped *every* table to FullRefresh — including `tbl_Archive_Track` at **137M rows**, which blew `tempdb`'s transaction log on **both** SQL Server targets every Sunday for three weeks. Small lookup tables still get their weekly full reconciliation. See [Forced full refresh](#forced-full-refresh-is-capped-by-row-count-aim-1966) |
| `MaxDeletePercent` | `0` (off) | Ratio ceiling on synchronized deletes: a delete pass that would remove more than this share of the target aborts for that table (logged ERROR, 0 rows deleted). Only evaluated when the target holds ≥ 100 rows — on a 12-row lookup table a routine 4-row delete is 33% and trips any sane threshold | Set to `25` on prod→dev mirrors. Until 2026-07-25 the **PostgreSQL delete path had no safety check at all** (the 50%/10% guards documented for delete sync are SQL-Server-only), so a source that returned no rows could empty a target table silently |
| `AuditReferentialIntegrityAfterSync` | `false` | PG targets only: after the run, walk `pg_constraint` for every inbound FK of every synced table and count child rows with no surviving parent. Logs a WARNING per constraint and flags findings whose child table is **outside this profile's sync scope** — those never self-heal. Read-only; an audit failure never fails the run | Set `true` on any profile with `DisableTriggersDuringLoad`. This is the detection that replaces the FK blocking the flag removes |

### Table Options

| Option | Default | Purpose | When to Use |
|--------|---------|---------|-------------|
| `Mode: FullRefresh` | - | Sync all rows every time | Small tables, lookup tables, tables without timestamps |
| `Mode: Incremental` | - | Only sync rows changed since last sync | Large tables with reliable timestamp column |
| `TimestampColumn` | - | Column to check for changes | Required for Incremental mode. Can be any datetime column (e.g., `ModifiedDate`, `LastEditDT`, `EntryDT`) |
| `FallbackTimestampColumn` | - | Fallback column when TimestampColumn is NULL | Use `COALESCE(TimestampColumn, FallbackTimestampColumn)` to catch new records with NULL timestamps |
| `LookbackHours` | `0` | 🔴 **Not a per-run widening.** Sets the window for a **first sync / stale watermark** only — it does NOT re-check the last N hours on every incremental run. See [What LookbackHours actually does](#-what-lookbackhours-actually-does-aim-1975) | Bounding the FIRST sync of a large table. Do **not** rely on it to catch late-arriving changes on a healthy daily table — measured, it never fires there |
| `DeleteMode: None` | default | Never delete rows from target | Append-only tables, when you want to preserve target data |
| `DeleteMode: Sync` | - | Delete rows from target not in source | When target must exactly mirror source |
| `SyncAllDeletes` | `false` | Full PK comparison for deletes in Incremental mode | Set `true` when using Incremental + DeleteMode.Sync to catch all deletes |
| `CreateIfMissing` | `false` | Auto-create target table | Initial setup, migrations. Don't use in prod without review |
| `Priority` | `100` | Sync order (lower = first) | Use when tables have FK dependencies. Same priority = parallel |
| `SourceFilter` | - | WHERE clause to filter source data | When you only want to sync a subset of rows. **Latent bug:** the delete path applies this to the source PK query but not the target's, so with `DeleteMode: Sync` every target row outside the filter becomes a delete candidate. No PG profile currently sets it |
| `DeleteExclusionFilter` | - | WHERE clause evaluated against the **target**; matching rows are removed from the delete candidate set, so a full-parity mirror never deletes them | Protecting dev-only test fixtures on a prod→dev mirror (see [Dev test fixtures](#dev-test-fixtures-on-a-prod-dev-mirror)). PG delete path only. **Never on a prod target** — it deliberately lets the target diverge |

### Database Type Configuration

The `Type` field in connection config accepts:
- **SQL Server**: `"SqlServer"`, `"mssql"`
- **PostgreSQL**: `"PostgreSql"`, `"postgres"`, `"pgsql"`

---

## Common Configuration Patterns

### Lookup/Reference Tables (small, rarely change)
```json
{
  "SourceTable": "Categories",
  "Mode": "FullRefresh",
  "DeleteMode": "Sync"
}
```

### Transaction Tables (large, frequently updated)
```json
{
  "SourceTable": "Orders",
  "Mode": "Incremental",
  "TimestampColumn": "ModifiedDate",
  "DeleteMode": "Sync"
}
```

### Tables with Late-Arriving Data
```json
{
  "SourceTable": "TrackingEvents",
  "Mode": "Incremental",
  "TimestampColumn": "LastEditDT",
  "LookbackHours": 72,
  "DeleteMode": "Sync"
}
```
Re-syncs all rows where `LastEditDT` >= (last sync time - 72 hours).

### Tables with NULL Timestamps on New Records
```json
{
  "SourceTable": "Activity",
  "Mode": "Incremental",
  "TimestampColumn": "LastEditDT",
  "FallbackTimestampColumn": "EntryDT",
  "LookbackHours": 72,
  "DeleteMode": "Sync"
}
```
Uses `COALESCE(LastEditDT, EntryDT)` - if `LastEditDT` is NULL, falls back to `EntryDT`.

### Tables with Foreign Key Dependencies
```json
{
  "SourceTable": "Parent",
  "Mode": "FullRefresh",
  "Priority": 1,
  "DeleteMode": "Sync"
},
{
  "SourceTable": "Child",
  "Mode": "FullRefresh",
  "Priority": 2,
  "DeleteMode": "Sync"
}
```

---

## Day-Specific Scheduling

The `Schedules` array allows different sync modes and intervals for different days of the week.

| Option | Default | Purpose |
|--------|---------|---------|
| `Days` | required | Day names: `"Sunday"`, `"Monday"`, etc. Also accepts: `"Sun"`, `"Mon"`, etc. |
| `IntervalMinutes` | inherited | Sync interval for these days |
| `StartTime` | inherited | Start time for these days |
| `ForceFullRefresh` | `false` | Forces all tables to FullRefresh regardless of their configured Mode |

**Example - Incremental weekdays, Full refresh on Sunday:**
```json
{
  "Schedule": {
    "StartTime": "05:00",
    "IntervalMinutes": 120,
    "RunImmediatelyOnStart": true,
    "Enabled": true,
    "Schedules": [
      {
        "Days": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"],
        "IntervalMinutes": 120,
        "ForceFullRefresh": false
      },
      {
        "Days": ["Sunday"],
        "IntervalMinutes": 360,
        "ForceFullRefresh": true
      }
    ]
  }
}
```

This configuration:
- **Monday-Saturday**: Runs every 2 hours using table-configured modes
- **Sunday**: Runs every 6 hours, forcing ALL tables to FullRefresh

---

## Blackout Window

Prevent syncs during maintenance or backup windows:

```json
{
  "BlackoutWindow": {
    "Enabled": true,
    "StartTime": "23:00",
    "EndTime": "05:00"
  }
}
```

No new syncs will start during the blackout window. Running syncs will complete.

---

## Load Throttling

Pause sync when source database is under heavy load:

```json
{
  "LoadThrottling": {
    "Enabled": true,
    "MaxCpuPercent": 60,
    "CheckIntervalSeconds": 30,
    "MaxWaitMinutes": 30,
    "CheckTiming": "BeforeTable"
  }
}
```

| Option | Default | Purpose |
|--------|---------|---------|
| `MaxCpuPercent` | `60` | Pause sync when CPU exceeds this (SQL Server) |
| `MaxActiveQueries` | `50` | Pause sync when active queries exceed this (PostgreSQL) |
| `CheckIntervalSeconds` | `30` | How often to re-check load when paused |
| `MaxWaitMinutes` | `30` | Maximum time to wait before proceeding anyway |
| `CheckTiming` | `BeforeTable` | When to check: `BeforeProfile`, `BeforeTable`, or `Both` |

**How it works:**
- **SQL Server**: Queries `sys.dm_os_ring_buffers` for CPU utilization (requires `VIEW SERVER STATE` permission)
- **PostgreSQL**: Queries `pg_stat_activity` for active connection count

---

## Source Database Performance

Two options help reduce impact on production source databases:

### NOLOCK Hint (SQL Server sources)

When `UseNoLock: true` (default), all SQL Server source queries use `WITH (NOLOCK)`:
- Reduces lock contention on source database
- Allows other queries to proceed without blocking
- May read uncommitted data (dirty reads) - acceptable for sync operations

```json
{
  "Options": {
    "UseNoLock": true
  }
}
```

### Source Batching

When `SourceBatchSize > 0` (default: 100000), source data is read in batches:
- Reduces memory pressure on source database server
- Allows other queries to interleave between batches
- Progress logged every batch

```json
{
  "Options": {
    "SourceBatchSize": 50000
  }
}
```

Set to `0` to disable batching (stream all rows in single query).

**Recommended settings for busy production databases:**
```json
{
  "Options": {
    "UseNoLock": true,
    "SourceBatchSize": 50000,
    "MaxParallelTables": 2
  }
}
```

---

## Target Encoding Support (WIN1252)

For PostgreSQL-to-PostgreSQL sync, the service automatically detects the target database's server encoding and handles character compatibility:

**How it works:**
1. On first connection, queries `SHOW server_encoding` to detect target encoding
2. If target is not UTF8 (e.g., WIN1252), logs a warning
3. During data transfer, characters outside the target encoding's range are replaced with `?`

**WIN1252 specifics:**
- WIN1252 is a single-byte encoding supporting characters 0x00-0xFF
- Unicode characters above U+00FF (emojis, arrows, CJK, etc.) are replaced with `?`
- This prevents encoding errors like: "character with byte sequence 0xf0 0x9f 0x8e in encoding UTF8 has no equivalent in encoding WIN1252"

**No configuration required** - encoding detection and sanitization is automatic.

---

## Timezone Handling

To prevent timestamps from being misinterpreted when syncing between servers with different timezone settings, the service automatically sets the PostgreSQL session timezone to UTC:

**How it works:**
1. Before any sync operation, `SET TIME ZONE 'UTC'` is executed on all PostgreSQL connections
2. For `timestamptz` columns, values are read/written in UTC context regardless of server's default timezone
3. For `timestamp` (naive) columns, values are preserved exactly as-is (no timezone conversion)
4. UTC DateTime values include timezone offset when written to ensure correct interpretation

**Why this matters:**
- PostgreSQL interprets `timestamptz` values based on the session timezone setting
- If source server is in EST and target is in UTC, timestamps could shift by 5 hours
- By setting session timezone to UTC on both connections, values are interpreted consistently

**Scenarios handled:**
| Column Type | Behavior |
|-------------|----------|
| `timestamp without time zone` | Values preserved exactly as-is |
| `timestamp with time zone` | Values normalized to UTC for consistent storage |
| DateTime with `Kind.Utc` | Written with `+00` offset to prevent misinterpretation |

**No configuration required** - timezone handling is automatic.

---

## Restricted Tables

The following tables are automatically skipped during sync to prevent system table corruption:

| Table | Reason |
|-------|--------|
| `db_environment` | Environment-specific settings that should not be synced |
| `_sync_history` | Sync history tracking table managed by the service |

These tables are filtered out regardless of whether they appear in the profile's table list. A warning is logged when tables are skipped:
```
Skipped 1 restricted tables (db_environment, _sync_history)
```

---

## Automatic Unique Constraint Recovery

When a unique constraint violation occurs during upsert, the service automatically attempts to recover. This works for both **PostgreSQL** and **SQL Server** targets.

### How It Works

| Step | PostgreSQL Target | SQL Server Target |
|------|-------------------|-------------------|
| **Detect** | Catches error `23505` (unique_violation) | Catches SqlException `2601`/`2627` (duplicate key) |
| **Parse** | Reads `ConstraintName` from `PostgresException` | Parses index name from error message via regex |
| **Find columns** | Queries `pg_index`/`pg_constraint` | Queries `sys.indexes`/`sys.index_columns` |
| **Delete conflicts** | Deletes target rows matching staging on constraint columns | Deletes target rows matching staging on constraint columns where PK differs |
| **Retry** | Re-runs `INSERT ... ON CONFLICT` | Re-runs `MERGE` |

### Example: SQL Server Recovery

A common scenario: `tbl_Account_User` has PK on `AccountUserID` but also a unique index `UX_tbl_Account_User_ACountID_UserID` on `(AccountID, UserID)`. When source has a new row with the same `AccountID`+`UserID` but different PK, MERGE tries to INSERT and hits the unique index. Recovery deletes the stale target row and retries.

```
[WRN] Unique constraint violation on 'UX_tbl_Account_User_ACountID_UserID' for table tbl_Account_User. Attempting automatic recovery...
[INF] Constraint 'UX_tbl_Account_User_ACountID_UserID' involves columns: AccountID, UserID
[INF] Deleted 1 conflicting rows from tbl_Account_User. Retrying MERGE...
[INF] MERGE retry succeeded after constraint recovery
```

### Example: PostgreSQL Recovery

```
[WRN] Unique constraint violation on 'idx_events_source_unique' for table core.events. Attempting automatic recovery...
[INF] Constraint 'idx_events_source_unique' involves columns: source_name, source_event_id
[INF] Deleted 4,426 conflicting rows from core.events. Retrying upsert...
[INF] Upsert retry succeeded after constraint recovery
```

**Safety limits:**
- Only attempts recovery if table has fewer than 300,000 rows (configurable)
- Only retries once to prevent infinite loops
- Logs all actions for audit trail

**FK limitation:** When the constraint recovery deletes conflicting rows from the target, those deletes may fail if child tables have FK references to the conflicting rows (or the table has a self-referential FK). For **PG→PG mirror** profiles this is solved by `DisableTriggersDuringLoad: true` (see [FK self-reference fix](#fix-self-referential-fk-blocks-unique-constraint-recovery) below) — RI triggers are suppressed on the target so the delete succeeds. Without that flag (or for SQL Server targets), truncate the target table (with `CASCADE` if needed) and re-sync from scratch.

**Configuration:**
The threshold can be adjusted in profile options (default: 300,000):
```json
{
  "Options": {
    "MaxRowsForConstraintRecovery": 300000
  }
}
```

Set to `0` to disable automatic recovery.

### Fix: self-referential FK blocks unique-constraint recovery

**Symptom:** A PG→PG mirror table with a secondary unique constraint AND inbound/self-referential FKs (the canonical case is `core.events`: unique `uq_events_source`, self-FK `events_duplicate_of_event_id_fkey`, plus 26 inbound child FKs) gets stuck — the unique-constraint recovery's `DELETE` is RESTRICTed by the FKs, the whole table transaction aborts, 0 rows sync, and downstream child tables then fail with FK errors (`23503`).

**Fix:** Set `DisableTriggersDuringLoad: true` on the profile (`ProfileOptions`). The target bulk-load session runs under `session_replication_role = 'replica'`, which suppresses FK/RI **triggers** (so deletes/inserts aren't blocked by FK ordering or self-FKs) while still enforcing PK/UNIQUE indexes (so the unique-violation recovery still fires — its delete now just succeeds). Applied at every target-connection open in `PostgreSqlBulkDataCopier`, and on the orphan-delete connection in `PostgreSqlSchemaAnalyzer.DeleteByPrimaryKeysAsync` (target analyzer only — never the source).

**Requirements / guardrails:**
- The flag is wired through `SyncOrchestrator` for the PG→PG copier only and defaults to `false` — it can never reach a SQL Server target or a target you don't opt in.
- **Never enable for a production target.** Enabled only on the 5 prod→dev mirror profiles (7-CORE, 8-EMP, 9-NXS, 10-WGO, 11-ACX).
- `session_replication_role` is superuser-only to set, **unless** the target user is granted the parameter (PG 15+):
  ```sql
  -- DEV target only — run once as the claude superuser on devpgsql:
  GRANT SET ON PARAMETER session_replication_role TO empdev;   -- emp database mirror users
  GRANT SET ON PARAMETER session_replication_role TO acxdev;   -- acx database mirror user
  GRANT SET ON PARAMETER session_replication_role TO rmpdev;   -- rmp database mirror user
  ```
  (parameter ACLs are cluster-wide in `pg_parameter_acl`, so one grant per role covers all databases). If the grant is missing, the GUC set is skipped with a one-time warning and sync proceeds with triggers on (pre-existing FK-block behavior) — it degrades, it doesn't crash.
- Full diagnosis and one-time manual reseed runbook: `devdocs/core-events-sync-stuck-fk-recovery.md`.

### What that flag costs — and the three settings that pay it back (AIM #1497)

`session_replication_role = 'replica'` is set for the **whole target connection**, so it suppresses RI for *every* statement — including the ordinary parity-delete, not just the unique-constraint recovery delete it was added for. That removes the DELETE's blast-radius protection, with two consequences that went unnoticed for six weeks:

1. **Orphaned child rows in schemas outside the sync scope.** A parity-delete of `core.events` / `core.users` is no longer RESTRICTed by inbound FKs. `faf` is the live case: it has **no sync profile at all** and holds **35 FKs into `core`** (25 into `core.users` alone), so its rows orphan silently. Every other schema with `core` children is itself mirrored, so its orphans get swept in the same pass. Measured damage before the fix: 14 orphaned `faf.ticket_types` + 339 orphaned `faf.event_announcements`.
2. **Direct loss of the mirrored table's own dev-only rows** — see [Dev test fixtures](#dev-test-fixtures-on-a-prod-dev-mirror) below.

**The suppression was deliberately NOT narrowed.** Scoping it to only the recovery delete (leaving ordinary deletes RI-enforced so they RESTRICT loudly) looks like the obvious fix and was rejected: the suppression is load-bearing on the delete path, because `core.events`' self-FK `events_duplicate_of_event_id_fkey` blocks the recovery DELETE — the exact failure the flag was added for. `AuditReferentialIntegrityAfterSync` gives the same signal without the fragility.

**Prove these fire before trusting a green result.** A clean "no orphans found" is indistinguishable from an audit that never ran. All three were proven with controlled probes 2026-07-25 (plant an orphan by deleting a parent under `session_replication_role='replica'`; temporarily set `MaxDeletePercent: 1` and insert 8 dev-only rows into a 233-row table). Probe recipes and the observed log lines are in `devdocs/core-events-sync-stuck-fk-recovery.md`.

**First real catch — auto-excluded child tables under a mirrored parent (RMP, 2026-07-25).** The audit's first production run flagged **138 orphaned `rmp.refresh_tokens` (83% of the table) + 2 `rmp.password_reset_tokens`**, all pointing at deleted `rmp.users`. The mechanism is structural and will recur on any profile in this shape: token/session tables are **auto-excluded from sync by Rule #132**, but their parent `users` table **is** mirrored — so every parity delete of a user strands that user's dev-side tokens, with no sync pass that would ever clean them up. Harm is low (dead tokens just fail auth) but it is genuine RI corruption under a validated FK, and it accumulates indefinitely. Cleaned 2026-07-25 (140 rows deleted, audit now clean). Expect it again after any large user-set change; the fix is a periodic delete, not a config change.

### Dev test fixtures on a prod→dev mirror

A full-parity mirror deletes any target row absent from the source — so **every dev-only test account in a mirrored table vanishes on the next sync**. This is why the documented FAF dev account `faftest@digsol.us` and the #1287 fixture `payouttest1287@digsol.us` did not exist when someone went looking for them. It is the more frequent day-to-day pain of the two consequences above, and it feeds the first one (a deleted `core.users` row orphans everything in `faf` that references it).

**Convention: dev-only test accounts use a `+devfixture@` email**, e.g. `faftest+devfixture@digsol.us`. Profile `7-CORE` protects them:

```json
{ "SourceTable": "core.users", "TargetTable": "core.users", "Mode": "FullRefresh",
  "DeleteMode": "Sync", "Priority": 1,
  "DeleteExclusionFilter": "email LIKE '%+devfixture@%' OR email IN ('connect1287@fanfare.events', ...)" }
```

The `IN (...)` list grandfathers fixtures created before the convention; prefer the pattern for anything new, since the explicit list has to be hand-edited and will rot. The older workaround — seed the account in **prod** so it mirrors down, which is how `jfilmer@nextsong.me` survives — still works and needs no config, at the cost of a test account in production.

**The filter protects against deletion, not against a PK collision.** A dev-only row that takes an ID the source later assigns to a different row is overwritten by the upsert, which keys on the PK. Sequence resets after each sync make this unlikely, not impossible.

## Credentials: profiles carry `${PLACEHOLDER}`, never a password (AIM #1821)

Since 2026-08-06 **no profile file contains a password.** Each connection string carries a
placeholder that `SecretResolver` substitutes at load time:

```json
"SourceConnection": {
  "Type": "PostgreSql",
  "ConnectionString": "Host=prodpgsql.digsol.us;Port=8282;Database=emp;Username=empprod;Password=${DBSYNC_EMPPROD_PASSWORD}",
  "SecretRef": "aim:config/7"
}
```

**Resolution order per `${NAME}`** (first hit wins): process environment variable → the secrets
file. `$${NAME}` escapes to a literal `${NAME}`; a password that genuinely contains `${` must use
that form. `SecretRef` is documentation only — never read at runtime, so the service has no
startup dependency on AIM being reachable.

| Host | Secrets file | Keys |
|---|---|---|
| ubu2 | `/opt/services/DatabaseSync/secrets.env` — **600 root:root** | 6 PostgreSQL |
| win2 | `C:\Services\DatabaseSync\secrets.env` | 3 LMPro SQL Server |
| workstation | `DatabaseSync/secrets.env` — 600, gitignored, **excluded from the devshare mirror** | all 9 |

**Placeholder → AIM config (rule #115 system of record).** Every connection also carries a
matching `SecretRef`; retrieve a value with `aim_get_config` + `reveal`.

| Placeholder | AIM config | Host / login | Profiles |
|---|---|---|---|
| `DBSYNC_EMPPROD_PASSWORD` | #7 | `empprod@prodpgsql` | 07,08,09,10,13 (source) |
| `DBSYNC_EMPDEV_PASSWORD` | #6 | `empdev@devpgsql` | 07,08,09,10,13 (target) |
| `DBSYNC_ACXPROD_PASSWORD` | #265 | `acxprod@prodpgsql` | 11 (source) |
| `DBSYNC_ACXDEV_PASSWORD` | #264 | `acxdev@devpgsql` | 11 (target) |
| `DBSYNC_RMPPROD_PASSWORD` | #65 | `rmpprod@prodpgsql` | 12 (source) |
| `DBSYNC_RMPDEV_PASSWORD` | #64 | `rmpdev@devpgsql` | 12 (target) |
| `DBSYNC_LMP_PRDSQL_PASSWORD` | **#272** | `jfilmer@prdsql.lmpro.us` (AWS RDS, prod) | 01,03,05 (source) |
| `DBSYNC_LMP_STGSQL_PASSWORD` | **#273** | `jfilmer@stgsql.lmpro.us` (AWS EC2, staging) | 01,03,05 target + 02,04,06 source |
| `DBSYNC_LMP_NOLA1SQL_PASSWORD` | **#274** | `jfilmer@10.10.2.10` = **win2** / `devmssql.digsol.us` | 02,04,06 (target) |

⚠ **"nola1sql" is a legacy filename, not a host.** Profiles 02/04/06 are named `*-nola1sql.json`
but the connection string points at `10.10.2.10`, which is **win2** (server_id 5) — so the service
runs on win2 and syncs into win2's own SQL Server. The env-var name keeps the legacy spelling to
match the filenames; the AIM config is keyed `mssql.lmpro.devmssql.password` by what it actually
is. Do not create a second "nola1sql" config.

⚠ Configs **#272/#273/#274 were created with NO VALUE** (2026-08-07) and must be filled in from the
AIM UI, not from a Claude Code session — `aim_create_config`/`aim_update_config` take the value as
a tool parameter, which would write a live credential into a session transcript (the AIM #1824
failure mode). The three LMPro logins are also personal-looking `jfilmer` accounts rather than
service roles, and were never part of the DSG PostgreSQL rotation work.

**Two failure modes are deliberately FATAL — the service refuses to start:**

1. **An unresolved placeholder.** `ConfigurationValidator` only *logs* errors; startup continues
   (`Program.cs`). Without the hard failure the service would sit `active` while connecting with
   the literal text `${NAME}` as the password — the exact shape of the nine-day outage below.
   One bad placeholder stops *every* profile; that trade-off is deliberate.
2. **A secrets file with any group/other permission bit** (Unix only; on Windows ACLs govern and
   the check is skipped). This is what stops a hand-copy at the default umask silently re-widening
   the file, since `/opt/services/DatabaseSync` has no automatic deploy.

**`ConnectionString` vs `EffectiveConnectionString` — get this right or you leak.** The model
splits them on purpose:

- `ConnectionString` — the on-disk template. **The only value ever serialized.**
- `ResolvedConnectionString` — `[JsonIgnore]`, so it is structurally impossible to write to disk.
- `EffectiveConnectionString` — what every consumer connects with.

Anything that opens a connection must use `EffectiveConnectionString`. Anything that *writes a
profile* must use `ConnectionString`. That split is why `ProfileGenerator` is safe for free: it
copies the `ConnectionConfig` verbatim into `profiles/_generated/*.json`, so it emits placeholders
rather than resolved passwords.

**Rotating a password is now a one-file edit** — change the value in each host's `secrets.env` and
restart. No profile file changes. `grep`-ing profiles for an old value no longer finds anything,
because the value is not there.

### Gotcha: `profiles/` is gitignored, so password rotations never reach it

*(Largely retired by the placeholder mechanism above — a rotation now touches `secrets.env`, not
13 profile files. Kept because the detection lessons still apply.)*

Profile files are in `.gitignore`, so they are only ever updated by hand-copying to
`/opt/services/DatabaseSync/profiles/` on ubu2.

**Nine-day outage, 2026-07-16 → 07-25.** `empdev` was rotated 2026-07-16 (AIM #1517, after the value leaked in whats-going-on git history). DatabaseSync's four `emp` profiles were not updated, so **7-CORE / 8-EMP / 9-NXS / 10-WGO failed nightly with `28P01 password authentication failed`** while ACX/RMP (different credentials) kept succeeding.

- **`systemctl is-active` is not a health check here.** A failing profile does not stop the host process — the service reported `active` throughout. Check `_sync_history` freshness per profile, or `/status`.
- **`journalctl` retention hid the age** (only went back to the last boot, four days), making a nine-day failure look recent. `_sync_history` is what dated it to the rotation.
- **When rotating any DB password**, update each host's `secrets.env` and restart. Current values: `aim_get_config` with `reveal`.

### 🔴 Auditing this repo for stray credentials — the recursive grep LIES

`grep -rl "Password=" .` reports **zero** credential files in this repo, and that is not because
there are none. Claude Code's Bash `grep` is a shell function wrapping **ugrep with
`--ignore-files`**, i.e. gitignore-aware — and `profiles/`, `publish/`, `bin/` and `secrets.env`
are all gitignored. Measured 2026-08-06: gitignore-aware grep found **4** files (all `.md`
examples); `find … -print0 | xargs -0 grep -l` found **237**.

```bash
# The only trustworthy form:
find . -path ./.git -prune -o -type f -print0 | xargs -0 grep -l "Password=" 2>/dev/null
# Classify, digit-safe (placeholder names contain digits, e.g. LMP_NOLA1SQL):
grep -ohE 'Password=[^;"]*' profiles/*.json \
  | sed -E 's/Password=\$\{[A-Za-z0-9_]+\}/PLACEHOLDER/; t; s/Password=.*/LITERAL/' | sort | uniq -c
```

> 🔴 **CORRECTION 2026-08-10 — the "verified across … and win2" claim below was FALSE for win2.** #1821 checked only win2's *active* `profiles/` directory. A canary over the whole of `C:\Services` found **90 literal credentials in 57 files**, in three trees nobody thought to look at: `profiles-backup-20260725/` (12 — pre-zero-padding profiles, so pre-placeholder), `profiles.Development/` (4 — **PostgreSQL** profiles on the SQL-Server-only host) and `publish/` (74 — the same stale-publish-output mechanism closed on ubu2, never cleaned here). Removed 2026-08-10, and `scripts/deploy-win2.sh --canary` now checks the whole tree and fails the deploy. **These were never rotated** (#1821 deliberately rotated nothing), so treat them as having been live throughout. The lesson is the same one this section already teaches, one level up: *an audit finds only what it thinks to look for* — five copies became eight once someone swept a directory rather than a file list.

**Five copies existed, and each prior audit found only the ones it thought to look for:**

| Location | Files | Mode | Reached by |
|---|---|---|---|
| `profiles/` (devlocal + mirror) | 13 | 600 | owner only |
| `publish/` (devlocal + mirror) | 24 | **777** | `github-runner` **read+write** |
| `bin/` (devlocal only) | 195 | 777 | mirror-excluded |
| `devdocs/DatabaseSyncConfigSummary.txt` | 1 | 664 | mirrored, world-readable |
| `/opt/services/DatabaseSync/publish/` | 36 | 775 | `github-runner` read |

All deleted 2026-08-06 (workstation_event #38, server_event #215). Two mechanisms created them and
both are now closed:

- **The csproj copied profiles into build output.** `<Content Update="profiles\**\*.json">` now
  carries `<CopyToPublishDirectory>Never</CopyToPublishDirectory>`. Verified: a `win-x64` publish
  emits **0** profile files.
- **`deploy-ubu2.sh`'s excludes were unanchored.** `--exclude='profiles/'` matches a directory of
  that name at *any* depth, so `--delete` skipped `publish/profiles/` and left it forever. The
  patterns are now anchored (`--exclude='/profiles/'`), and the script runs a **credential canary**
  that fails the deploy if any file outside `profiles/` contains `Password=`.

### Deploying: use `scripts/deploy-ubu2.sh`

The ubu2 deploy was hand-copied with no script on-host or in-repo — which is how the deployed
profiles ended up world-readable with three production credentials (server_event #207). The script
builds on ubu2 (rule #123), deploys binaries only, then **enforces `600 root:root` on
`profiles/` + `secrets.env`, proves `github-runner` is denied, and runs the credential canary.**

```bash
./scripts/deploy-ubu2.sh            # build + deploy + enforce + verify
./scripts/deploy-ubu2.sh --modes    # enforce modes only
```

**Profiles are never deployed by it** — they are per-host config (win2 has 01-06, ubu2 has 07-13)
and shipping the build's copy would overwrite the live set.

⚠ **`sudo` + globs**: the deployed `profiles/` dir is 750 root:root, so a glob written *outside*
`sudo` expands as the calling user and silently yields nothing — `sudo stat "$D"/*.json` returns
"No such file or directory" while the files are plainly there. Put the glob inside
`sudo bash -c '…'`. Hit twice now (server_event #207 and again #215); same shape as AIM #1507.

---

## Incremental Sync Behavior

| Scenario | Behavior |
|----------|----------|
| **Has sync history** | Uses `MaxSourceTimestamp` from last successful sync |
| **No history + LookbackHours > 0** | Uses `DateTime.UtcNow - LookbackHours` (smart first sync) |
| **No history + LookbackHours = 0** | Falls back to full sync (loads all rows) |
| **Has history, watermark RECENT** | Resumes from the watermark. **Lookback is NOT applied** — see below |
| **Has history, watermark already older than the lookback window** | Watermark minus `LookbackHours` (applied once) |

**First Sync Optimization**: With `LookbackHours: 72`, a first sync on a 4.2M row table loads only ~70K rows (rows from the last 72 hours) instead of all 4.2M rows.

### 🔴 What `LookbackHours` actually does (AIM #1975)

**It does NOT widen every incremental run**, and this file said it did for a long time. The guard only applies the lookback when the watermark is **already older** than the lookback window — a first sync, or after a gap. On a healthy daily table the watermark is recent, so it never fires.

    watermark in _sync_history : 2026-08-08 07:13:08   (nxs.setlist_songs, LookbackHours=168)
    if lookback applied        : would load since 2026-08-01 07:13
    ACTUAL log line            : "Loading rows changed since 08/08/2026 07:13:08"

**The code was deliberately left alone and the docs corrected instead** — the reverse of the obvious fix. Two measurements drove that:

- **Nothing has ever been missed because of it.** Rows older than the watermark were compared source-vs-target: **0 missing and 0 stale** across 6 PG tables (`core.talent`, `core.talent_genres`, `core.events`, `wgo.scrape_runs`, `wgo.raw_events`, `wgo.raw_talent`), and **0** across a 54,355-row slice of LMPro `tbl_OrderEmail`. The timestamp columns move forward monotonically on edit, so a changed row always lands in the next window on its own.
- **Turning it on would be very expensive.** `tbl_Archive_Track` currently syncs **0 rows/run**; its configured 425-day lookback covers **5,323,656 rows**, which it would then re-sync *every day* — plausibly re-creating the tempdb pressure #1966 just removed, and that is one table of 98 with a lookback configured.

> ⚠️ **Counting rows on each side does NOT test this** — it produced a false signal twice while investigating. A mirror is legitimately behind, so the target having *fewer* rows is normal; and restricting to "older than the watermark" makes the target show *more*, because a row updated in the source after the watermark leaves the source's window while the target still holds the stale copy inside it. **The only sound test is directional and PK-based**: source rows older than the watermark whose `(pk, content-hash)` has no counterpart in the target.

**If a table ever does need late-arriving-change coverage**, do not "fix" the guard globally — that changes all 98 entries at once. Give that one table a `SourceFilter` or a periodic reconciliation instead.

---

## Delete Synchronization (DeleteMode and SyncAllDeletes)

Delete synchronization ensures the target table mirrors the source by removing rows that no longer exist in the source. This is controlled by two settings that work together:

### DeleteMode

| Mode | Behavior |
|------|----------|
| `None` | Only INSERT and UPDATE - never delete from target |
| `Sync` | Synchronized deletes - delete rows in target that don't exist in source |

### SyncAllDeletes

This setting controls **how** deletes are detected when `DeleteMode: Sync` is enabled:

| SyncAllDeletes | Delete Detection Method | Best For |
|----------------|------------------------|----------|
| `false` (default) | **Staging Table Comparison** - Only compares PKs from rows in the current sync batch | FullRefresh mode (entire table is in staging) |
| `true` | **Full PK Comparison** - Compares ALL primary keys between source and target | Incremental mode (catches deletes outside sync window) |

### How Delete Detection Works

**Staging Table Comparison** (`SyncAllDeletes: false`):
1. After upserting data, compares PKs in staging table vs target table
2. Deletes rows in target that exist in staging but not in target
3. Fast, but only detects deletes for rows that were in the sync batch
4. Works perfectly for FullRefresh since all rows are in the staging table

**Full PK Comparison** (`SyncAllDeletes: true`):
1. Creates a staging table with ALL primary keys from source
2. Uses SQL-based `DELETE ... LEFT JOIN` to find and delete orphaned rows
3. Catches ALL deletes, even rows deleted outside the incremental time window
4. Uses optimized SQL operations with clustered indexes for performance

### Why This Matters for Incremental Mode

In Incremental mode, only recently-changed rows are synced based on `TimestampColumn`. If a row is deleted from source:
- The deleted row has no timestamp change (it doesn't exist)
- Without `SyncAllDeletes: true`, the delete is never detected
- The orphaned row remains in target indefinitely

**Example**: A row deleted from source 2 weeks ago won't appear in an incremental sync looking at the last 72 hours. Only `SyncAllDeletes: true` will catch it.

### Recommended Configuration

**FullRefresh tables** - Staging comparison is sufficient:
```json
{
  "Mode": "FullRefresh",
  "DeleteMode": "Sync"
}
```

**Incremental tables** - Use full PK comparison to catch all deletes:
```json
{
  "Mode": "Incremental",
  "TimestampColumn": "LastEditDT",
  "LookbackHours": 72,
  "DeleteMode": "Sync",
  "SyncAllDeletes": true
}
```

### Performance Characteristics

The Full PK Comparison uses an optimized SQL-based approach:
1. Bulk loads all source PKs to a staging table using `SqlBulkCopy`
2. Creates a clustered index on the staging table
3. Executes a single `DELETE ... LEFT JOIN` statement
4. All operations happen on the database server (no client-side PK loading)

**Tested performance**: A table with 70K+ rows completes delete sync in ~7 minutes (previously timed out after 1 hour with the old in-memory approach).

### Safety Thresholds

The full PK comparison includes built-in safety checks to prevent catastrophic data loss:

1. **Source Ratio Check**: If source has less than 50% of target's row count, the delete is aborted. This catches scenarios where the source database is incomplete (e.g., only incremental data was loaded).

2. **Delete Ratio Check**: If more than 10% of target rows would be deleted, the operation is aborted. This prevents accidental mass deletion due to configuration errors.

When a safety check triggers, an error is logged and 0 rows are deleted. Review the configuration and use FullRefresh mode if a large-scale delete is intentional.

### CRITICAL: Chained Sync Limitation

**DO NOT use `SyncAllDeletes: true` in chained incremental sync scenarios.**

A chained sync is when Profile A syncs to Database B, and Profile B then syncs Database B to Database C:
```
Production (34M rows) → [Profile 1] → Staging → [Profile 2] → Downstream
```

**The Problem**: If Profile 1 uses Incremental mode, Staging only receives the changed rows (e.g., 4K rows). When Profile 2 runs with `SyncAllDeletes: true`, it compares Staging's 4K PKs against Downstream's 34M rows and attempts to delete 34M rows (everything not in the 4K).

**Safe Configurations for Chained Syncs**:

| Profile Position | Recommended Settings |
|------------------|---------------------|
| First profile (Production → Staging) | `SyncAllDeletes: true` is safe (source is authoritative) |
| Downstream profiles (Staging → Other) | Use `SyncAllDeletes: false` or `DeleteMode: None` |

**Example - Safe chained sync for large incremental tables**:
```json
// Profile 1: Production → Staging (SyncAllDeletes OK)
{
  "SourceTable": "tbl_Track",
  "Mode": "Incremental",
  "DeleteMode": "Sync",
  "SyncAllDeletes": true  // Safe - source is authoritative
}

// Profile 2: Staging → Downstream (SyncAllDeletes NOT safe)
{
  "SourceTable": "tbl_Track",
  "Mode": "Incremental",
  "DeleteMode": "Sync",
  "SyncAllDeletes": false  // Required - staging is not authoritative
}
```

### Two-Phase Sync (FK-Safe Deletes)

For **PostgreSQL-to-PostgreSQL** syncs with `DeleteMode: Sync`, the service automatically uses two-phase sync to prevent foreign key constraint violations:

**Phase 1 - Upserts (Priority Order: 1→2→3→4)**
- Parent tables are synced first (lower priority numbers)
- All insert/update operations complete before any deletes
- Ensures child records can reference parent records

**Phase 2 - Deletes (Reverse Priority Order: 4→3→2→1)**
- Child tables are deleted first (higher priority numbers)
- Parent tables are deleted last
- Prevents FK violations when removing records

**How it works:**
1. Service detects PostgreSQL-to-PostgreSQL sync with any `DeleteMode: Sync` tables
2. Automatically enables two-phase mode (logged: "Using two-phase sync")
3. All tables complete upserts with deletes skipped
4. Deletes run in reverse priority order

**Example Priority Setup for FK Dependencies:**
```json
{
  "Tables": [
    { "SourceTable": "users", "Priority": 1, "DeleteMode": "Sync" },
    { "SourceTable": "orders", "Priority": 2, "DeleteMode": "Sync" },
    { "SourceTable": "order_items", "Priority": 3, "DeleteMode": "Sync" }
  ]
}
```

With this configuration:
- **Upserts**: users → orders → order_items
- **Deletes**: order_items → orders → users

This ensures `order_items` referencing `orders` are deleted before the parent `orders` rows, and `orders` referencing `users` are deleted before the parent `users` rows.

**Note:** Two-phase sync is automatic for PostgreSQL-to-PostgreSQL. Other database combinations handle deletes inline (within each table's sync operation).

---

## Profile Execution Order and Cross-Schema Dependencies

### Profile Ordering

When multiple profiles share a database and have cross-schema foreign key dependencies, **execution order matters**. Profiles are sorted **as strings** by `ProfileName`/filename before execution, both for scheduled runs (`ProfileExecutionMode: Sequential`) and HTTP-triggered syncs (`POST /sync`). String sorting is why the numeric prefix must be **zero-padded to two digits** — see the naming-convention warning above; an unpadded `7-` sorts *after* `10-`.

Use zero-padded numbered prefixes to control execution order:

```
07-CORE-prodpgsql-devpgsql   ← Runs first  (core.users, core.events, core.talent, etc.)
08-EMP-prodpgsql-devpgsql    ← Runs second (emp.user_event_assignments → core.events)
09-NXS-prodpgsql-devpgsql    ← Runs third  (nxs.songs, nxs.setlists, etc.)
10-WGO-prodpgsql-devpgsql    ← Runs fourth (wgo.event_clicks → core.events)
11-ACX-prodpgsql-devpgsql    ← Runs next   (acx database, all schemas — independent)
12-RMP-prodpgsql-devpgsql    ← Runs next   (rmp database, rmp schema — independent)
13-FAF-prodpgsql-devpgsql    ← Runs last   (faf.* — EVERY table has cross-schema FKs into core)
```

### Cross-Schema FK Dependencies

Tables in one schema often reference tables in another schema. These cross-schema FKs cannot be enforced by table priority within a single profile — they require the parent profile to complete first.

**Current cross-schema dependencies:**

| Child Table (schema) | Parent Table (schema) | Enforced By |
|-----------------------|-----------------------|-------------|
| `emp.user_event_assignments` | `core.events`, `core.users` | Profile order: CORE (7) before EMP (8) |
| `wgo.event_clicks` | `core.events` | Profile order: CORE (7) before WGO (10) |
| `wgo.raw_talent` | `core.talent` | Profile order: CORE (7) before WGO (10) |
| all 29 `faf.*` tables | `core.users`, `core.events`, `core.talent`, `core.venues` | Profile order: CORE (07) before FAF (13) — 35 FKs |

### Within-Profile Priority Assignment

Within a profile, tables are ordered by `Priority` number based on FK dependencies. Use topological sort from FK relationships:

1. Tables with no FK dependencies → Priority 1
2. Tables referencing only P1 tables → Priority 2
3. Continue until all tables have priorities

**Example from WGO profile:**
```
P1: activity_types, occasions, event_categories         (no FKs)
P2: scrape_sources, curated_lists, saved_searches       (FK to P1)
P3: scrape_runs, subscriptions, event_category_map...   (FK to P2)
P4: raw_venues, scrape_errors                           (FK to scrape_runs P3)
P5: raw_events, venue_tags, user_activity_log           (FK to raw_venues P4)
P6: raw_talent                                          (FK to raw_events P5)
```

**Common mistakes:**
- Placing a child table at the same priority as its parent (they run in parallel and race)
- Not accounting for self-referencing FKs (e.g., `raw_venues.parent_venue_id → raw_venues.raw_venue_id`)
- Forgetting cross-schema dependencies when creating per-schema profiles

---

## Graceful Missing Table Handling

When a source or target table doesn't exist (e.g., table was renamed, moved to another schema, or not yet created), the sync **skips** the table with a warning instead of failing.

### Behavior

| Scenario | Log Level | Status | Counts as Failure? |
|----------|-----------|--------|-------------------|
| Source table missing | Warning | Skipped (⊘) | No |
| Target table missing | Warning | Skipped (⊘) | No |
| Target missing + `CreateIfMissing: true` | Info | Created, then synced | No |
| Other sync errors | Error | Failed (✗) | Yes |

### Log Output

```
[WRN] ⊘ wgo.old_table: Source table 'wgo.old_table' not found - skipped
[WRN] ⊘ wgo.new_table: Target table 'wgo.new_table' not found - skipped
```

### Summary Display

Skipped tables appear separately in the sync summary:
```
Tables: 34/36 successful, 2 skipped
```

Skipped tables do **not** trigger `StopOnError` and do **not** cause the profile to report as failed. This allows schemas to evolve without breaking sync operations — update the profile when ready, and missing tables are safely ignored in the meantime.

---

## Dashboard

The web dashboard at `/dashboard` provides real-time visibility into sync operations.

### Dashboard Features

- **Profile Cards**: Each profile shows status, statistics, and next scheduled run
- **Sync Now Button**: Click "Sync Now" on any profile card to trigger an immediate sync (bypasses startup delay and schedule)
- **Table-level Sync**: On the profile detail page, each table has its own "Sync" button
- **Auto-refresh**: Dashboard auto-refreshes every 30 seconds
- **Startup Delay Banner**: When startup delay is active, shows countdown and "Start Now" button

### Dashboard Columns

| Column | Description |
|--------|-------------|
| **Table** | Source table name |
| **Mode** | Sync mode (Incremental or FullRefresh) |
| **Status** | Success/Failed indicator |
| **Rows** | Total rows processed (inserts + updates) |
| **Ins/Upd/Del** | Breakdown of inserts, updates, and deletes |
| **Recent %** | Percentage of updates that were for recently-modified records |
| **Duration** | Time taken for the sync operation |
| **Completed** | When the sync finished |

### Understanding "Recent %"

The **Recent %** column answers the question: *"Of the updates we performed, what percentage were for records that have been inserted or updated within the last 168 hours (7 days)?"*

**Calculation**: `RecentRowsCount / RowsUpdated * 100`

Where:
- `RecentRowsCount` = Number of source rows where the timestamp column >= (now - 168 hours)
- `RowsUpdated` = Number of rows that were updated (not inserted) during this sync

**Why this matters**:
- A high percentage (e.g., 80-100%) indicates most updates are for recently-changed data, which is expected behavior
- A low percentage suggests updates are happening to older records, which may indicate:
  - Retroactive data corrections in the source system
  - Late-arriving data with backdated timestamps
  - ⚠️ **Increasing `LookbackHours` will NOT help here** — it does not widen a normal incremental run (AIM #1975). Use a `SourceFilter` or a periodic reconciliation for that table instead.

**Note**: This metric only applies to updates. Inserts are excluded because new records are always "recent" by definition. The column shows `-` when there are no updates in the sync.

---

## Profile Generator

The dashboard includes a "Generate Complete Profile" button that automatically creates optimized sync profiles by analyzing the source database schema.

### What It Does

1. **Discovers all schemas and tables** - Queries the source database for all user tables
2. **Analyzes foreign key relationships** - Identifies dependencies between tables
3. **Calculates priority levels** - Uses topological sort to assign priorities respecting FK order
4. **Generates per-schema profiles** - Creates one JSON file per schema for easier management
5. **Preserves existing settings** - Copies configuration from existing table entries

### Generated Profile Settings

Each generated profile uses these defaults:

| Setting | Value | Reason |
|---------|-------|--------|
| Mode | `FullRefresh` | Ensures complete data sync |
| DeleteMode | `Sync` | Mirrors source exactly |
| SyncAllDeletes | `true` | Catches all deleted rows |
| Priority | FK-based | Parent tables sync before children |
| Schedule.Enabled | `false` | Requires manual review before enabling |

### Output Location

Generated profiles are saved to:
```
profiles/_generated/{profileName}-{schema}-full.json
```

Example: For profile `9-CORE-prodpgsql-devpgsql`, generates:
- `9-CORE-prodpgsql-devpgsql-core-full.json` (34 tables)
- `9-CORE-prodpgsql-devpgsql-cron-full.json` (2 tables)
- `9-CORE-prodpgsql-devpgsql-emp-full.json` (3 tables)
- `9-CORE-prodpgsql-devpgsql-nxs-full.json` (14 tables)
- `9-CORE-prodpgsql-devpgsql-wgo-full.json` (41 tables)

### Excluded Tables

The generator automatically excludes:
- System tables (`pg_*`, `information_schema.*`)
- Sync history table (`_sync_history`)
- Environment table (`db_environment`)

### API Usage

```bash
# Generate profiles via API
curl -X POST http://localhost:5123/admin/generate-profile/9-CORE-prodpgsql-devpgsql

# Response
{
  "success": true,
  "profileName": "9-CORE-prodpgsql-devpgsql",
  "filesGenerated": [
    "9-CORE-prodpgsql-devpgsql-core-full.json",
    "9-CORE-prodpgsql-devpgsql-cron-full.json",
    ...
  ],
  "totalTables": 94
}
```

### Priority Calculation

Tables are assigned priorities based on FK dependencies using topological sort:

1. Tables with no FK dependencies get priority 1
2. Tables that reference only priority-1 tables get priority 2
3. Process continues until all tables have priorities

**Example:**
```
users (priority 1) ← orders (priority 2) ← order_items (priority 3)
```

This ensures parent tables sync before their children, preventing FK constraint violations.

---

## Architecture

### Bulk Copier Classes

| Class | Source | Target | Bulk Method |
|-------|--------|--------|-------------|
| `BulkDataCopier` | SQL Server | PostgreSQL | Npgsql COPY protocol |
| `SqlServerBulkDataCopier` | SQL Server | SQL Server | SqlBulkCopy + MERGE |
| `PostgreSqlBulkDataCopier` | PostgreSQL | PostgreSQL | Npgsql COPY protocol |
| `PostgreSqlToSqlServerBulkCopier` | PostgreSQL | SQL Server | SqlBulkCopy + MERGE |

### Type Mapping

The `TypeMapper` class handles type conversion between databases:

**SQL Server -> PostgreSQL:**
- `int` -> `integer`
- `bigint` -> `bigint`
- `varchar(n)` -> `varchar(n)`
- `datetime2` -> `timestamp`
- `uniqueidentifier` -> `uuid`
- `bit` -> `boolean`

**PostgreSQL -> SQL Server:**
- `integer` -> `int`
- `bigint` -> `bigint`
- `varchar(n)` -> `varchar(n)`
- `timestamp` -> `datetime2`
- `uuid` -> `uniqueidentifier`
- `boolean` -> `bit`
- `text` -> `nvarchar(MAX)`

---

## Development Guidelines

### Source Database Safety

**The source database must NEVER be modified.** All source database connections are **strictly read-only**. Only `SELECT` queries, counts, and schema metadata queries are permitted on source connections. All write operations occur exclusively on the **target** database.

### Database User Conventions

**Environment-specific users**: Database users are specific to their environment. The production database user (e.g., `empprod`) only exists in the production database, and the development database user (e.g., `empdev`) only exists in the development database. Do not attempt to use cross-environment credentials.

**Restricted permissions**: Sync users should have data manipulation privileges only (SELECT, INSERT, UPDATE, DELETE, TRUNCATE) but should NOT have schema modification privileges (CREATE, DROP). Tables should be owned by a separate admin user (e.g., `postgres` or `claude`), not by the sync user.

> **IMPORTANT — Sequence permissions for sync users:** Any database user used as a **target** connection in a sync profile **must** have UPDATE privilege on sequences in every synced schema. Without this, `setval()` calls after sync (which reset IDENTITY sequences to match source data) will silently fail, causing sequence drift and eventual duplicate key errors. Apply these grants on the **target (dev) database only** — source connections are read-only.
>
> ```sql
> -- One-time: grant on all existing sequences
> GRANT UPDATE ON ALL SEQUENCES IN SCHEMA <schema> TO <sync_user>;
>
> -- Permanent: auto-grant on future sequences created by the table owner
> ALTER DEFAULT PRIVILEGES FOR ROLE claude IN SCHEMA <schema> GRANT UPDATE ON SEQUENCES TO <sync_user>;
> ```
>
> Run both statements for **every schema** the sync user targets. The `ALTER DEFAULT PRIVILEGES` ensures new tables with IDENTITY columns automatically get the correct grants — no manual intervention needed when schemas evolve.
>
> 🔴 **Two ways this silently rots, both found live 2026-07-25:**
>
> 1. **A newly-synced schema is never granted.** When ACX's 12 `media.*` tables were added (AIM #1281), the grants were not extended to the `media` schema — `acxdev` had UPDATE on **0 of 8** `media` sequences (also 0 in `core` and `public`). Every run logged `42501: permission denied for sequence <name>` and skipped the `setval()`, so those sequences never advanced past their dev values. Fixed by granting UPDATE + `ALTER DEFAULT PRIVILEGES` on `media`, `core`, and `public`; the warnings went to zero on the next run.
> 2. **`ALTER DEFAULT PRIVILEGES FOR ROLE claude` does not cover objects created by `postgres`.** Default privileges are per-granting-role. The three sequences still ungranted in the `emp` database (`core.user_push_subscriptions_*`, `nxs.event_setlists_*`, `wgo.trending_chip_counts_*`) are all owned by `postgres`, not `claude`. **Add a matching `FOR ROLE postgres` line** whenever tables may be created by either role.
>
> These failures are *logged*, not silent — but they appear as `WRN` amid a green run, so nothing draws attention to them. Audit with:
> ```sql
> SELECT n.nspname AS schema, count(*) AS sequences,
>        count(*) FILTER (WHERE has_sequence_privilege('<sync_user>', c.oid, 'UPDATE')) AS can_update
> FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
> WHERE c.relkind = 'S' AND n.nspname NOT LIKE 'pg\_%'
> GROUP BY 1 ORDER BY 1;
> ```
> (Filter on `relkind='S'` inside a `MATERIALIZED` CTE if you also select per-sequence detail — otherwise the planner may evaluate `has_sequence_privilege` against indexes and error with `"<name>" is not a sequence`.)

**Pre-creating sync history table**: When using restricted database users without CREATE permission, the `_sync_history` table must be pre-created by an admin user. Run the following SQL with a privileged user (e.g., `postgres` or `claude`):

```sql
-- Create the sync history table
CREATE TABLE IF NOT EXISTS "_sync_history" (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    profile_name VARCHAR(100) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    sync_start_time TIMESTAMP NOT NULL,
    sync_end_time TIMESTAMP NOT NULL,
    success BOOLEAN NOT NULL,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,   -- AIM #1946: a skipped table is success=true; without this it is indistinguishable from a real sync
    rows_processed BIGINT NOT NULL DEFAULT 0,
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_updated BIGINT NOT NULL DEFAULT 0,
    rows_deleted BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    max_source_timestamp TIMESTAMP,
    duration_seconds DOUBLE PRECISION NOT NULL,
    recent_rows_count BIGINT NOT NULL DEFAULT 0,
    total_source_rows BIGINT NOT NULL DEFAULT 0,
    created_datetime TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_history_profile_table ON "_sync_history" (profile_name, source_table);
CREATE INDEX IF NOT EXISTS idx_sync_history_run_id ON "_sync_history" (run_id);
CREATE INDEX IF NOT EXISTS idx_sync_history_sync_time ON "_sync_history" (sync_end_time DESC);

-- Grant permissions to the sync user
GRANT SELECT, INSERT, UPDATE, DELETE ON "_sync_history" TO your_sync_user;
GRANT USAGE, SELECT ON SEQUENCE _sync_history_id_seq TO your_sync_user;
```

### Adding a New Feature

1. Create models in `/Models` if needed
2. Add configuration in `/Configuration/SyncServiceConfig.cs`
3. Implement service logic in `/Services`
4. Update `Program.cs` if new endpoints needed
5. Update this CLAUDE.md file

### Code Patterns

**Logging**: Use structured logging with Serilog
```csharp
_logger.LogInformation("Syncing {Table} with {Rows} rows", tableName, rowCount);
```

**Error Handling**: Catch at orchestrator level, record in history
```csharp
try { ... }
catch (Exception ex)
{
    result.Success = false;
    result.Error = ex.Message;
    _logger.LogError(ex, "Sync failed for {Table}", tableName);
}
```

**Async All the Way**: Use async/await throughout
```csharp
public async Task<SyncResult> SyncTableAsync(...)
```

---

## Future Plans

### Monitoring & Alerting
- Email notifications on sync failure
- Webhook support (Slack, Teams, PagerDuty)
- Prometheus metrics endpoint

### Advanced Features
- Cron expression support for complex schedules
- Schema drift detection
- Data validation (row counts, checksums)
- Column transformations

---

## Project Info

- **Stack**: C# / .NET 8, SQL Server, PostgreSQL
- **Architecture**: Multi-profile, timer-based scheduler with HTTP API

*Last Updated (2026-08-09): **LookbackHours / watermark defects — docs corrected, two code bugs fixed (AIM #1975).** Chasing the "lookback applied twice" footnote from #1966 turned up three defects, and the most important one was resolved by changing the DOCS rather than the code. **D1: `LookbackHours` never applies on a normal incremental run** — the guard only fires when the watermark is already older than the window, so on a healthy daily table it is silently skipped (measured: watermark `2026-08-08 07:13:08` → `Loading rows changed since 08/08/2026 07:13:08`, zero hours applied), which is the opposite of what this file described. It was left AS-IS because measurement said the feature has never been needed and enabling it would be very expensive: **0 rows missing and 0 stale** across 6 PG tables and a 54,355-row LMPro slice, versus **5,323,656 rows/day** that `tbl_Archive_Track` alone would re-sync (it currently syncs 0) — across 98 entries with a lookback configured. 🔴 Counting rows on each side does NOT test this and gave a false signal twice: a mirror is legitimately behind, and restricting to "older than the watermark" makes the TARGET show more, because a row updated in the source after the watermark leaves the source's window while the target still holds the stale copy inside it. Only a directional PK+content-hash comparison settles it. **D2 (fixed): lookback applied twice on a first run** (`2160h → 180 days`, `168h → 336h`) because provenance was inferred from a timestamp comparison that is always true — the branch sets `UtcNow - Lookback` and the guard re-evaluates `UtcNow - Lookback` milliseconds later, which is fractionally larger. Now tracked with an explicit flag; verified both log lines show the same timestamp. **D3 (fixed): a FullRefresh wiped the incremental watermark** — it records `max_source_timestamp = NULL` and the lookup took the newest successful row without an `IS NOT NULL` filter, so the next incremental run logged "No sync history found" despite a good watermark sitting a row below, then fell back to lookback-from-now (doubled by D2). One line in both repositories; verified by poisoning the watermark and confirming the next run resumed from the real one. Prior: **`_sync_history.rows_deleted` now records two-phase deletes (AIM #1964)** — the third and last of the "history misstates the run" family. It read **0 across all 25,372 rows of all 7 PG profiles** while those runs really were deleting, because PG→PG two-phase writes the history row at the end of phase 1 (deletes skipped) and phase 2 then updated only the IN-MEMORY `SyncResult` — which is exactly why the summary and dashboard were right while history was wrong. The SQL Server targets, which delete inline, showed `max rows_deleted = 3,775,721`; that contrast is what confirmed the diagnosis and that the SQL Server path needs no change (the new call sits inside the PG-only two-phase branch, so it is a no-op there). Fixed with `UpdateDeleteCountAsync(runId, sourceTable, rowsDeleted)` keyed on `(run_id, source_table)`, which never throws — failing to annotate history is not a reason to fail a sync that already moved data. Deferring the phase-1 write was REJECTED (a mid-run crash would lose the whole run's history), as was a second delete-phase row (doubles rows, breaks every consumer). Proven against a PREDICTED number rather than "non-zero": 7 planted dev-only rows → parity delete removed exactly 7 → history recorded **7**, versus `max = 0` on every prior run. Prior: **Forced full refresh is now capped by row count (AIM #1966).** `tbl_Archive_Track` (137M rows) had been failing EVERY SUNDAY on both LMP_Archive profiles with `tempdb` transaction-log-full (error 9002), burning 1-3h per attempt and syncing 0 rows — 3 consecutive Sundays, escalating 0→4→8 errors, invisible because the profile just reported 29/30 and carried on. Root cause was NOT tempdb sizing (both independently-administered targets failed identically): `ForceFullRefresh` is **all-or-nothing per day-schedule**, so the Sunday entry flipped every table to FullRefresh regardless of size. New `ForceFullRefreshMaxRows` (default 3,000,000) keeps a table on its configured `Mode` when its estimated row count exceeds the limit; small tables still get their weekly full reconciliation. Estimates come from `sys.partitions` / `pg_class.reltuples`, never `COUNT(*)` — counting 137M rows to decide "is this too big to scan" would be self-defeating — and null means "change nothing", never zero. Chosen as a row THRESHOLD rather than a per-table opt-out flag so a table growing past the limit is exempted automatically; a hand-maintained list rots silently, which is the drift pattern that hid this for weeks. **Also flipped 10 entries that were configured `Mode: FullRefresh` outright despite exceeding 3M rows** (the guard only suppresses a *forced* refresh, not a configured one) — and caught that all five tables carried `TimestampColumn` AND `FallbackTimestampColumn` both set to `EntryDT`, a creation date, so converting them as-configured would have silently missed every in-place update; they now use `COALESCE(LastEditDT, EntryDT)`. `LookbackHours: 2160` is load-bearing because `max_source_timestamp` was NULL across 240+ prior FullRefresh runs, and without it the first incremental run falls back to a full scan. Verified with a two-sided control (guard correctly does NOT fire at 3M on a 6,114-row table, DOES fire at 100) plus real runs: profile 05 went **34,342,123 rows / 1h23m44s → 2,912,645 rows / 11m29s**, and profile 03 went **29/30 with a 2h42m failure → 30/30, 0 tempdb errors, `tbl_Archive_Track` incremental in 5m29s**. `rows=0` for that table is its normal steady state (every successful run for 9 days), so the weekly full refresh was buying nothing. ⚠ Noted in passing: lookback is applied TWICE on a first run (the second block's comment claims it only applies to history-derived times, but its condition never tests that) — pre-existing and errs safe. Prior: **Three independent bugs fixed (AIM #1946).** (1) `rmp.platform_admin_actions` had been skipping silently on every run of profile 12 — it was **retired into `rmp.audit_log`** by RMP migration 0037 (#1483), not dropped, so the entry was REPOINTED rather than deleted (the `wgo.curator_*` chase-don't-delete precedent, now 2 for 2). `audit_log.ip_address` is the fleet's first `inet` column and needed no code change — it falls through the COPY writer's final `ToString()` branch — confirmed by md5 parity at 272 rows, not by reading the branch. Full drift re-audit found nothing else uncovered. (2) **A skipped table wrote `success=true` to `_sync_history`**, so history said 39/39 while the summary said 38/39 — meaning the tool this file recommends for freshness checks overstated success and could not date when a skip began. Added a `skipped` column across both repositories, the orchestrator and the dashboard (which now excludes skips from the success rate rather than counting them as wins); each repo PROBES for the column so a `42501`-denied ALTER on a DML-only sync user cannot break all history writes with `42703`. Added manually as `claude` to emp/acx/rmp (all `postgres`-owned); 559 historical rows backfilled, all already-resolved drift. Proven with a deliberately-missing controlled probe. (3) **ubu2 was writing logs to a directory literally named `D:`** because `Path.IsPathRooted("D:/…")` is false on Linux, while the documented `/var/log/services/DatabaseSync` sat five months stale — fixed with a non-Windows guard in `Program.cs`, since `appsettings.Production.json` cannot work when BOTH hosts run as Production. ⚠ Found in passing and still open: `_sync_history.rows_deleted` is **always 0** on PG→PG two-phase profiles (0 across all 2,187 rows of profile 12) because history is written at the end of phase 1, before the phase-2 delete pass. Prior: **Profiles no longer contain passwords (AIM #1821).** All 26 connection strings across 13 profiles now carry `${PLACEHOLDER}` tokens resolved at load time by `SecretResolver` — environment variable first, then a 600 `secrets.env`. Verified 26/26 placeholders, 0 literals, across devlocal, the devshare mirror, ubu2 `/opt/services` and win2. `ConnectionConfig` splits template from resolved value, with `ResolvedConnectionString` marked `[JsonIgnore]` so a resolved secret is structurally unserializable — which is also what makes `ProfileGenerator` safe, since it copies the config verbatim into generated profiles. Two conditions are deliberately FATAL at startup: an unresolved placeholder, and a secrets file with any group/other bit. Both exist because `ConfigurationValidator` only *logs* errors, so without them the service would sit `active` while sending the literal `${NAME}` as a password — the shape of the nine-day empdev outage. **Five credential copies were found and deleted, not three:** the audit that reported 33 files was run with a gitignore-aware `grep` (Claude Code's Bash `grep` is ugrep `--ignore-files`), which reports ZERO credential files in this repo; `find | xargs grep` reported 237. The extra copies were `publish/` (24 at mode 777, world-WRITABLE, mirrored, holding a live `empprod`), `bin/` (195), a `devdocs` summary (mirrored, 664), and 36 files inside `/opt/services/DatabaseSync/publish/` readable by `github-runner`. Two mechanisms created them, both now closed: the csproj copied profiles into publish output (`CopyToPublishDirectory=Never`), and `deploy-ubu2.sh`'s `--exclude='profiles/'` was unanchored so `--delete` skipped nested copies (now `/profiles/`, plus a credential canary that fails the deploy on any credential file outside `profiles/`). New `scripts/deploy-ubu2.sh` replaces the hand-copied deploy and enforces `600 root:root` + proves `github-runner` is denied. No passwords were rotated (deliberate). ⚠ The 3 LMPro SQL Server logins still have **no AIM vault entry** — they now live only in the two `secrets.env` files. Prior: **Added profile 13-FAF-prodpgsql-devpgsql** — the `faf` schema finally has a mirror (29 tables, runs LAST because all 29 have cross-schema FKs into core). Deliberately `DeleteMode: None` on every table: dev holds ~285 dev-only FAF test rows that full parity would destroy. The trade-off is PK collisions instead — the first run already overwrote 4 dev rows (3 carts, 1 refund) because dev's IDs sit inside the range prod hasn't reached. No config avoids both losses; needs a deliberate reconciliation. FK graph contains a real CYCLE (tickets → order_items → resale_listings → tickets), broken at the two nullable edges. Prior: **Profile prefixes are now ZERO-PADDED (01-12)** — both sort sites compare strings, so `"10" < "7"` meant CORE was running FOURTH, after WGO, inverting the documented cross-schema FK order ever since WGO became profile 10. Masked all along by DisableTriggersDuringLoad suppressing the FKs that would have made it loud. Every renamed profile pins `ProfileId` to its old name so `_sync_history` stays continuous (verified: CORE still resolves 5,849 rows back to 2026-03-03, win2's 01-LMP_Main still returns its runs). **Profile drift audited against prod**: 9 missing tables added (5 core, 2 emp, 1 nxs), 3 stale `wgo.curator_*` entries removed — they had MOVED to the core schema, not vanished — and `wgo.trending_chip_counts` rejected because it has no PK (the upsert requires one). **ACX sequence grants fixed**: acxdev had UPDATE on 0 of 8 `media.*` sequences since #1281, so every run logged 42501 and skipped the setval; also learned `ALTER DEFAULT PRIVILEGES FOR ROLE claude` does NOT cover `postgres`-owned objects. **RI audit's first real catch**: 140 orphaned RMP token rows under deleted users — structural, since token tables are Rule-#132-excluded while their parent users table is mirrored; cleaned. Prior in this pass: Closed the blast-radius gap that `DisableTriggersDuringLoad` opened — suppressing RI on the target also unguards the ordinary parity-delete, which silently orphaned `faf` rows (the one schema with no sync profile: 35 FKs into `core`) and destroyed every dev-only test account in mirrored `core.*` tables. Three additions: `AuditReferentialIntegrityAfterSync` (post-run orphan count over `pg_constraint`, flags child tables outside sync scope), `DeleteExclusionFilter` (target-side WHERE exempting dev fixtures from parity deletes; convention is a `+devfixture@` email), and `MaxDeletePercent` (ratio ceiling — the PG delete path previously had NO safety check, unlike the SQL Server path). Deliberately did NOT narrow the `session_replication_role` scope: it is load-bearing on the delete path for `core.events`' self-FK. All three proven with controlled probes, not just observed green. Enabled on all 6 PG mirror profiles. Also fixed a nine-day silent outage: `empdev` was rotated 2026-07-16 (AIM #1517) and the gitignored profile files were never updated, so 7-CORE/8-EMP/9-NXS/10-WGO had failed nightly with 28P01 while `systemctl is-active` still reported healthy. Prior: Added profile 12-RMP-prodpgsql-devpgsql (rmp database, rmp schema, 39 tables, prod→dev mirror, daily 05:00, DisableTriggersDuringLoad) — RMP is now on ubu1 so the long-standing TODO is done. First PostGIS sync: `geography` columns round-trip via the USER-DEFINED `::text` cast (md5-identical), generated `listings.description_tsv` auto-excluded/recomputed. Excluded spatial_ref_sys, schemaversions, and token tables. Dev-side one-time setup: granted rmpdev the session_replication_role param + sequence UPDATEs, and pre-created `_sync_history` in **public** (the rmp DB's search_path puts rmp first, but the repo only existence-checks public). Verified 39/39 tables, counts match prod. Also cleaned 4 stray non-profile files (appsettings*.json, *.deps.json, *.runtimeconfig.json) out of ubu2's profiles/ dir that were being misparsed as a phantom 'Default' profile. Prior: Added all 12 `media.*` tables to profile 11-ACX (prod→dev mirror) for the ACX media catalog — AIM task #1281 (task listed 8; prod had drifted to 12: +face, +file_action, +person, +thumbnail). Fixed two latent PG→PG bugs the first non-null pgvector data exposed: (1) reading a `vector` threw because the try/catch fallback's `GetFieldValue<string>` is unsupported for `vector` — now the source SELECT casts USER-DEFINED columns to `::text` (covers vector + enums uniformly); (2) `media.image_meta.search_vector` is GENERATED ALWAYS STORED and can't be inserted into — generated columns are now auto-excluded (detected via `is_generated='ALWAYS'`) and recomputed on the target. Verified: 55/55 tables, all 12 media counts match prod, vector values md5-identical, search_vector recomputed on dev. Prior: added `DisableTriggersDuringLoad` (session_replication_role='replica') to unstick `core.events` PG→PG mirror sync where self-referential/inbound FKs blocked unique-constraint recovery and orphan deletes — enabled on all 5 PG mirror profiles, granted the GUC parameter to empdev/acxdev on devpgsql; added OVERRIDING SYSTEM VALUE for GENERATED ALWAYS AS IDENTITY support; added ACX profile (11-ACX-prodpgsql-devpgsql); fixed ubu1 UFW firewall blocking ubu2 on port 8282*
