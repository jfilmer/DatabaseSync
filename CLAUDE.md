# CLAUDE.md - Database Sync Service Project Documentation

> **Note:** For comprehensive configuration examples and troubleshooting, see [QUICKSTART.md](../QUICKSTART.md) in the parent folder.

## Quick Start

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

**Linux (systemd)**:
```bash
sudo cp database-sync.service /etc/systemd/system/
sudo systemctl enable database-sync
sudo systemctl start database-sync
```

**Windows**:
```cmd
sc create DatabaseSync binPath="C:\Apps\DatabaseSync\DatabaseSync.exe" start=auto
sc start DatabaseSync
```

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

---

## Full Configuration Model

```
SyncService
├── HttpPort
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
│   │   └── StopOnError
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

*Last Updated: Added comprehensive delete synchronization documentation (DeleteMode + SyncAllDeletes)*
