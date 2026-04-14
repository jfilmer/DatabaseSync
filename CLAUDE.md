# CLAUDE.md - Database Sync Service Project Documentation

> **Note:** For comprehensive configuration examples and troubleshooting, see [QUICKSTART.md](../QUICKSTART.md) in the parent folder.

## Quick Start

### System Instructions

If necessary reference /mnt/devshare/ClaudeProjects/dev_docs_common/PostgreSQLConnectionInfo.txt to connect to db

**On conversation start:** Offer to run a profile completeness audit (see [Profile Completeness Audit](#profile-completeness-audit) below). Tables get added/removed/renamed in prod as apps evolve, so profiles drift over time.

### Run Locally
```bash
dotnet run
```

The dashboard opens automatically at `http://localhost:5123/dashboard`

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
| **win2** (win2.digsol.us) | SQL Server syncs | `ssh claude@win2.digsol.us` | `C:\Services\DatabaseSync` | LMP_Main, LMP_Archive, LMP_Account (profiles 1-6) |
| **ubu2** (ubu2.digsol.us) | PostgreSQL syncs | `ssh claude@ubu2.digsol.us` | `/opt/services/DatabaseSync` | emp `core`, `emp`, `nxs`, `wgo` schemas (profiles 7-10); acx all schemas (profile 11) |

**Important**: Only deploy SQL Server profiles to win2 and PostgreSQL profiles to ubu2. Mixing causes errors (e.g., PG profiles on win2 fail trying to create `_sync_history` with restricted permissions).

**Profile naming convention**: Profiles are numbered (7-CORE, 8-EMP, 9-NXS, 10-WGO, 11-ACX) so alphabetical sorting produces the correct execution order. CORE must sync first since emp, nxs, and wgo schemas have foreign keys to core tables. ACX syncs independently (separate database).

**TODO — RMP database sync**: Once the `rmp` database has been copied/migrated to ubu1 (production), a new sync profile (e.g., `12-RMP-prodpgsql-devpgsql`) will need to be created to sync it from prod to dev on ubu2. This is pending the RMP database setup on ubu1.

### Remote Deployment via SSH (win2)

Build locally, deploy remotely. The `claude` user on win2 has admin privileges for service management.

```bash
# 1. Publish from the project directory (runs on local Linux machine)
dotnet publish DatabaseSync.csproj -c Release -r win-x64 --self-contained true -o /tmp/DatabaseSync-publish

# 2. Stop the service
ssh claude@win2.digsol.us "sc stop DatabaseSync"

# 3. Copy published files to win2 (flat files only, excludes profiles/ directory)
scp /tmp/DatabaseSync-publish/*.* claude@win2.digsol.us:"C:/Services/DatabaseSync/"

# 4. Copy profile configs (SQL Server profiles only, 1-6)
scp profiles/[1-6]*.json claude@win2.digsol.us:"C:/Services/DatabaseSync/profiles/"

# 5. Start the service
ssh claude@win2.digsol.us "sc start DatabaseSync"

# 6. Verify
ssh claude@win2.digsol.us "sc query DatabaseSync"
ssh claude@win2.digsol.us "curl -s http://localhost:5123/health"
```

**Important**: Step 3 uses `scp *.* ` (flat files only) instead of `scp -r` to prevent the `profiles/` directory (which contains all profiles including PG profiles 7-10) from being copied to win2. Never use `scp -r` for the full publish directory — it will deploy PG profiles to win2, causing the dashboard to show PostgreSQL schemas that belong on ubu2. Note: `rsync` is not available on win2's PATH, so use `scp` for file transfer.

**Logs on win2**: `D:\Logs\DatabaseSync\sync-YYYYMMDD.log`

**Checking logs remotely**:
```bash
# Tail recent log entries
ssh claude@win2.digsol.us "powershell -command \"Get-Content 'D:\Logs\DatabaseSync\sync-20260303.log' -Tail 30\""

# Search for errors
ssh claude@win2.digsol.us "powershell -command \"Select-String -Path 'D:\Logs\DatabaseSync\sync-20260303.log' -Pattern 'ERR|fail'\""
```

### Remote Deployment via SSH (ubu2)

```bash
# 1. Publish from the project directory (runs on local Linux machine)
dotnet publish DatabaseSync.csproj -c Release -r linux-x64 --self-contained true -o /tmp/DatabaseSync-publish-linux

# 2. Stop the service
ssh claude@ubu2.digsol.us "sudo systemctl stop database-sync"

# 3. Copy published files to ubu2 staging directory
scp -r /tmp/DatabaseSync-publish-linux/* claude@ubu2.digsol.us:/tmp/DatabaseSync-deploy/

# 4. Deploy binaries (exclude profiles/ to preserve existing configs on server)
ssh claude@ubu2.digsol.us "sudo rsync -a --exclude 'profiles/' /tmp/DatabaseSync-deploy/ /opt/services/DatabaseSync/ && sudo chown -R www-data:www-data /opt/services/DatabaseSync/"

# 5. Update profiles only when changed (PG profiles only, 7-10)
scp profiles/[7-9]*.json profiles/10*.json claude@ubu2.digsol.us:/tmp/DatabaseSync-deploy/
ssh claude@ubu2.digsol.us "sudo cp /tmp/DatabaseSync-deploy/*.json /opt/services/DatabaseSync/profiles/ && sudo chown www-data:www-data /opt/services/DatabaseSync/profiles/*.json"

# 6. Start the service
ssh claude@ubu2.digsol.us "sudo systemctl start database-sync"

# 7. Verify
ssh claude@ubu2.digsol.us "sudo systemctl is-active database-sync"
ssh claude@ubu2.digsol.us "curl -s http://localhost:5123/health"
```

**Note**: The `rsync --exclude 'profiles/'` pattern prevents the build output's profiles directory (which may contain SQL Server profiles from the source tree) from overwriting the server's PG-only profiles. Always update profiles separately in step 5.

**Logs on ubu2**: `/var/log/services/DatabaseSync/sync-YYYYMMDD.log`

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

1. **Read each PG profile file** (`profiles/7-*.json` through `profiles/11-*.json`) to get the list of configured source tables per profile.

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

### Restricted Tables (always excluded)

| Table | Reason |
|-------|--------|
| `db_environment` | Environment-specific settings |
| `_sync_history` | Managed by DatabaseSync service |
| `__EFMigrationsHistory` | EF Core system table |

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
| Unsupported type fallback | Gracefully handle extension types (e.g. pgvector `vector`) by reading as text |
| Profile generator | Auto-generate complete sync profiles from source database schema |
| Graceful missing table handling | Skip missing source/target tables with warning instead of failing |
| Alphabetical profile ordering | Profiles execute in sorted order for cross-schema FK dependencies |
| Target-side sequence reset | Queries target DB for sequences after sync to prevent duplicate key errors |

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

### Table Options

| Option | Default | Purpose | When to Use |
|--------|---------|---------|-------------|
| `Mode: FullRefresh` | - | Sync all rows every time | Small tables, lookup tables, tables without timestamps |
| `Mode: Incremental` | - | Only sync rows changed since last sync | Large tables with reliable timestamp column |
| `TimestampColumn` | - | Column to check for changes | Required for Incremental mode. Can be any datetime column (e.g., `ModifiedDate`, `LastEditDT`, `EntryDT`) |
| `FallbackTimestampColumn` | - | Fallback column when TimestampColumn is NULL | Use `COALESCE(TimestampColumn, FallbackTimestampColumn)` to catch new records with NULL timestamps |
| `LookbackHours` | `0` | Re-sync rows from (lastSyncTime - hours) | Catch late-arriving changes or retroactive updates |
| `DeleteMode: None` | default | Never delete rows from target | Append-only tables, when you want to preserve target data |
| `DeleteMode: Sync` | - | Delete rows from target not in source | When target must exactly mirror source |
| `SyncAllDeletes` | `false` | Full PK comparison for deletes in Incremental mode | Set `true` when using Incremental + DeleteMode.Sync to catch all deletes |
| `CreateIfMissing` | `false` | Auto-create target table | Initial setup, migrations. Don't use in prod without review |
| `Priority` | `100` | Sync order (lower = first) | Use when tables have FK dependencies. Same priority = parallel |
| `SourceFilter` | - | WHERE clause to filter source data | When you only want to sync a subset of rows |

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

**FK limitation:** When the constraint recovery deletes conflicting rows from the target, those deletes may fail if child tables have FK references to the conflicting rows. If this happens, truncate the target table (with `CASCADE` if needed) and re-sync from scratch. This typically only occurs on initial sync when target data is stale.

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

---

## Incremental Sync Behavior

| Scenario | Behavior |
|----------|----------|
| **Has sync history** | Uses `MaxSourceTimestamp` from last successful sync |
| **No history + LookbackHours > 0** | Uses `DateTime.UtcNow - LookbackHours` (smart first sync) |
| **No history + LookbackHours = 0** | Falls back to full sync (loads all rows) |
| **With LookbackHours configured** | Extends sync window backward to catch late changes |

**First Sync Optimization**: With `LookbackHours: 72`, a first sync on a 4.2M row table loads only ~70K rows (rows from the last 72 hours) instead of all 4.2M rows.

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

When multiple profiles share a database and have cross-schema foreign key dependencies, **execution order matters**. Profiles are sorted alphabetically by `ProfileName` before execution, both for scheduled runs (`ProfileExecutionMode: Sequential`) and HTTP-triggered syncs (`POST /sync`).

Use numbered prefixes to control execution order:

```
7-CORE-prodpgsql-devpgsql    ← Runs first  (core.users, core.events, core.talent, etc.)
8-EMP-prodpgsql-devpgsql     ← Runs second (emp.user_event_assignments → core.events)
9-NXS-prodpgsql-devpgsql     ← Runs third  (nxs.songs, nxs.setlists, etc.)
10-WGO-prodpgsql-devpgsql    ← Runs fourth (wgo.event_clicks → core.events)
11-ACX-prodpgsql-devpgsql    ← Runs last   (acx database, all schemas — independent)
```

### Cross-Schema FK Dependencies

Tables in one schema often reference tables in another schema. These cross-schema FKs cannot be enforced by table priority within a single profile — they require the parent profile to complete first.

**Current cross-schema dependencies:**

| Child Table (schema) | Parent Table (schema) | Enforced By |
|-----------------------|-----------------------|-------------|
| `emp.user_event_assignments` | `core.events`, `core.users` | Profile order: CORE (7) before EMP (8) |
| `wgo.event_clicks` | `core.events` | Profile order: CORE (7) before WGO (10) |
| `wgo.raw_talent` | `core.talent` | Profile order: CORE (7) before WGO (10) |

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
  - Need to increase `LookbackHours` to catch these changes

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

*Last Updated: Added OVERRIDING SYSTEM VALUE for GENERATED ALWAYS AS IDENTITY support; added unsupported type fallback (pgvector vector); added ACX profile (11-ACX-prodpgsql-devpgsql); fixed ubu1 UFW firewall blocking ubu2 on port 8282*
