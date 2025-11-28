# CLAUDE.md - Database Sync Service Project Documentation

## Project Overview

A high-performance, standalone database synchronization service that supports **bi-directional sync** between **Microsoft SQL Server** and **PostgreSQL**. Designed for any database sync scenario requiring reliable, scheduled data replication.

### Supported Sync Combinations

| Source | Target | Status |
|--------|--------|--------|
| SQL Server | PostgreSQL | ✅ Supported |
| SQL Server | SQL Server | ✅ Supported |
| PostgreSQL | PostgreSQL | ✅ Supported |
| PostgreSQL | SQL Server | ✅ Supported |

### Key Design Decisions

1. **Multi-Profile Architecture**: Each profile represents a source/target database pair with its own schedule and table list. This allows syncing multiple databases with a single service instance.

2. **Timer + HTTP API**: Simple in-process scheduler with configurable intervals. No external dependencies like pg_cron. Cross-platform compatible (Windows and Linux).

3. **Staging Table + Upsert Pattern**: Instead of row-by-row operations, data is bulk-loaded to a temp staging table, then upserted in a single SQL statement. This achieves 50,000+ rows/second throughput.
   - PostgreSQL targets: Uses `COPY` protocol + `INSERT ... ON CONFLICT`
   - SQL Server targets: Uses `SqlBulkCopy` + `MERGE` statement

4. **Priority Groups**: Tables are grouped by priority number. Same-priority tables sync in parallel (up to MaxParallelTables), different priorities sync sequentially. This respects foreign key dependencies.

5. **Synchronized Deletes**: Simple delete mode - if a row exists in target but not in source, delete it from target. No soft-delete complexity.

---

## Current Implementation Status

### ✅ Completed Features

| Feature | Status | Notes |
|---------|--------|-------|
| Multi-profile configuration | ✅ Complete | Each profile has its own connections, schedule, tables |
| **Bi-directional sync** | ✅ Complete | All 4 source/target combinations supported |
| SQL Server ↔ PostgreSQL type mapping | ✅ Complete | Handles all common types both directions |
| Bulk upsert via staging tables | ✅ Complete | High performance for all database combinations |
| Incremental sync (timestamp-based) | ✅ Complete | Only syncs rows changed since last run |
| Full refresh sync | ✅ Complete | Upserts all rows |
| Sync history tracking | ✅ Complete | `_sync_history` table with per-table stats |
| Automatic last sync time | ✅ Complete | Uses history for incremental resume |
| Parallel table processing | ✅ Complete | Configurable MaxParallelTables |
| Priority-based ordering | ✅ Complete | Respects table dependencies |
| Synchronized deletes | ✅ Complete | Deletes rows from target not in source |
| HTTP API for control | ✅ Complete | Status, trigger, health endpoints |
| Scheduled execution | ✅ Complete | StartTime + IntervalMinutes |
| Day-of-week filtering | ✅ Complete | Run only on specific days |
| Auto-create target tables | ✅ Complete | CreateIfMissing option |
| Source data filtering | ✅ Complete | SourceFilter WHERE clause |
| Profile execution modes | ✅ Complete | Parallel or Sequential profile execution |
| Blackout window | ✅ Complete | Prevent syncs during maintenance/backup windows |
| Incremental with lookback | ✅ Complete | Re-sync recent data to catch late-arriving changes |
| Automatic column filtering | ✅ Complete | Skip source columns not in target table |
| Load-based throttling | ✅ Complete | Pause sync when source server CPU is high |

### 🔧 Configuration Model

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
│   │   └── DaysOfWeek[]
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

### Configuration Options Explained

#### Profile Options

| Option | Default | Purpose | When to Change |
|--------|---------|---------|----------------|
| `MaxParallelTables` | `4` | Number of tables to sync concurrently | Increase for many independent tables; decrease if database can't handle load |
| `CommandTimeoutSeconds` | `300` | SQL command timeout | Increase for very large tables (millions of rows) |
| `EnableSyncHistory` | `true` | Track sync results in `_sync_history` table | Disable only if you don't need history/incremental sync |
| `UseHistoryForIncrementalSync` | `true` | Use `_sync_history` to find last sync time | Should almost always be `true` for incremental mode |
| `StopOnError` | `false` | Stop entire profile if one table fails | Set `true` when tables have dependencies |

#### Table Options

| Option | Default | Purpose | When to Use |
|--------|---------|---------|-------------|
| `Mode: FullRefresh` | - | Sync all rows every time | Small tables, lookup tables, tables without timestamps |
| `Mode: Incremental` | - | Only sync rows changed since last sync | Large tables with reliable timestamp column |
| `TimestampColumn` | - | Column to check for changes | Required for Incremental mode. Can be any datetime column (e.g., `ModifiedDate`, `LastEditDT`, `EntryDT`) |
| `FallbackTimestampColumn` | - | Fallback column when TimestampColumn is NULL | Use `COALESCE(TimestampColumn, FallbackTimestampColumn)` to catch new records with NULL timestamps |
| `LookbackHours` | `0` | Re-sync rows from (lastSyncTime - hours) | Catch late-arriving changes or retroactive updates. Works with any `TimestampColumn` |
| `DeleteMode: None` | default | Never delete rows from target | Append-only tables, when you want to preserve target data |
| `DeleteMode: Sync` | - | Delete rows from target not in source | When target must exactly mirror source |
| `SyncAllDeletes` | `false` | Full PK comparison for deletes in Incremental mode | Set `true` when using Incremental + DeleteMode.Sync to catch all deletes |
| `CreateIfMissing` | `false` | Auto-create target table | Initial setup, migrations. Don't use in prod without review |
| `Priority` | `100` | Sync order (lower = first) | Use when tables have FK dependencies. Same priority = parallel |
| `SourceFilter` | - | WHERE clause to filter source data | When you only want to sync a subset of rows |

#### Incremental Sync Behavior

When using `Mode: Incremental`, the sync determines where to start based on several factors:

| Scenario | Behavior |
|----------|----------|
| **Has sync history** | Uses `MaxSourceTimestamp` from last successful sync |
| **No history + LookbackHours > 0** | Uses `DateTime.UtcNow - LookbackHours` (smart first sync) |
| **No history + LookbackHours = 0** | Falls back to full sync (loads all rows) |
| **With LookbackHours configured** | Extends sync window backward to catch late changes |

**First Sync Optimization**: When a table has never been synced before (no entry in `_sync_history`), the service now uses `LookbackHours` from the current time instead of syncing the entire table. This dramatically improves first-sync performance:

- **Before**: First incremental sync loaded ALL rows (could be millions)
- **After**: First incremental sync only loads rows from the last N hours

**Example**: With `LookbackHours: 72`, a first sync on a 4.2M row table loads only ~70K rows (rows from the last 72 hours) instead of all 4.2M rows - a 98% reduction.

#### Load Throttling Options

Load throttling monitors the source database server and pauses sync operations when the server is under heavy load. This protects production databases from additional sync load during peak usage.

| Option | Default | Purpose |
|--------|---------|---------|
| `Enabled` | `false` | Enable/disable load monitoring |
| `MaxCpuPercent` | `60` | Pause sync when CPU exceeds this percentage (SQL Server) |
| `MaxActiveQueries` | `50` | Pause sync when active queries exceed this count (PostgreSQL) |
| `CheckIntervalSeconds` | `30` | How often to re-check load when paused |
| `MaxWaitMinutes` | `30` | Maximum time to wait before proceeding anyway |
| `CheckTiming` | `BeforeTable` | When to check: `BeforeProfile`, `BeforeTable`, or `Both` |

**How it works:**
- **SQL Server**: Queries `sys.dm_os_ring_buffers` for CPU utilization (requires `VIEW SERVER STATE` permission)
- **PostgreSQL**: Queries `pg_stat_activity` for active connection count

**Permission handling**: If the sync user lacks `VIEW SERVER STATE` permission on SQL Server, a warning is logged once and sync proceeds normally (load throttling is effectively disabled for that source).

**Example configuration:**
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

#### Recommended Configurations

**For lookup/reference tables (small, rarely change):**
```json
{
  "SourceTable": "Categories",
  "Mode": "FullRefresh",
  "DeleteMode": "Sync"
}
```

**For transaction tables (large, frequently updated):**
```json
{
  "SourceTable": "Orders",
  "Mode": "Incremental",
  "TimestampColumn": "ModifiedDate",
  "DeleteMode": "None"
}
```

**For tables with late-arriving data (use LookbackHours):**
```json
{
  "SourceTable": "tbl_Track",
  "Mode": "Incremental",
  "TimestampColumn": "LastEditDT",
  "LookbackHours": 72,
  "DeleteMode": "None"
}
```
This re-syncs all rows where `LastEditDT` >= (last sync time - 72 hours), catching any retroactive updates.

**For tables where new records may have NULL timestamps:**
```json
{
  "SourceTable": "tbl_Activity",
  "Mode": "Incremental",
  "TimestampColumn": "LastEditDT",
  "FallbackTimestampColumn": "EntryDT",
  "LookbackHours": 72,
  "DeleteMode": "None"
}
```
Uses `COALESCE(LastEditDT, EntryDT)` - if `LastEditDT` is NULL (new record never edited), falls back to `EntryDT`.

**For tables with dependencies:**
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

### Database Type Configuration

The `Type` field in connection config accepts:
- **SQL Server**: `"SqlServer"`, `"mssql"`
- **PostgreSQL**: `"PostgreSql"`, `"postgres"`, `"pgsql"`

### Delete Mode Behavior

| Mode | Behavior |
|------|----------|
| `None` | Only INSERT and UPDATE - never delete from target |
| `Sync` | Synchronized deletes - delete rows in target that don't exist in source |

When `DeleteMode: Sync` is enabled, the delete process uses an optimized SQL-based approach:
1. Uses the staging table (already loaded with source data) to identify rows to delete
2. Performs a single `DELETE ... LEFT JOIN` to remove rows that exist in target but not in source
3. This is significantly faster than loading PKs into memory, especially for large tables

**Performance note:** For a 34M row table with 6M deletes, the old approach took ~25 minutes; the optimized approach completes in ~1 minute.

---

## Architecture

### Bulk Copier Classes

Each source/target combination has a dedicated bulk copier:

| Class | Source | Target | Bulk Method |
|-------|--------|--------|-------------|
| `BulkDataCopier` | SQL Server | PostgreSQL | Npgsql COPY protocol |
| `SqlServerBulkDataCopier` | SQL Server | SQL Server | SqlBulkCopy + MERGE |
| `PostgreSqlBulkDataCopier` | PostgreSQL | PostgreSQL | Npgsql COPY protocol |
| `PostgreSqlToSqlServerBulkCopier` | PostgreSQL | SQL Server | SqlBulkCopy + MERGE |

### Type Mapping

The `TypeMapper` class handles type conversion between databases:

**SQL Server → PostgreSQL:**
- `int` → `integer`
- `bigint` → `bigint`
- `varchar(n)` → `varchar(n)`
- `datetime2` → `timestamp`
- `uniqueidentifier` → `uuid`
- `bit` → `boolean`
- etc.

**PostgreSQL → SQL Server:**
- `integer` → `int`
- `bigint` → `bigint`
- `varchar(n)` → `varchar(n)`
- `timestamp` → `datetime2`
- `uuid` → `uniqueidentifier`
- `boolean` → `bit`
- `text` → `nvarchar(MAX)`
- etc.

---

## Future Implementation Plans

### Phase 1: Enhanced Scheduling (Priority: High)

**Cron Expression Support**
- Replace simple interval with cron expressions for complex schedules
- Example: `"0 */2 * * 1-5"` (every 2 hours on weekdays)
- Library: Cronos or NCrontab

---

### Phase 2: Monitoring & Alerting (Priority: High)

**Email Notifications**
- Send email on sync failure
- Daily summary reports

**Webhook Support**
- POST to URL on sync complete/failure
- Integrate with Slack, Teams, PagerDuty

**Prometheus Metrics**
- Expose /metrics endpoint
- Rows synced, duration, error counts per profile/table

---

### Phase 3: UI Dashboard (Priority: Medium)

**Web Dashboard**
- Real-time sync status
- Profile management UI
- Sync history viewer
- Manual trigger buttons
- Log viewer

---

### Phase 4: Advanced Features (Priority: Low)

**Schema Sync**
- Detect schema changes in source
- Optionally apply DDL to target
- Alert on schema drift

**Data Validation**
- Row count comparison
- Checksum verification

**Transformation Support**
- Column renaming
- Value transformations

---

## Development Guidelines

### ⚠️ CRITICAL: Source Database Safety

**The source database must NEVER be modified.** This is a hard and fast rule.

All source database connections are **strictly read-only**. The only operations permitted on source connections are:
- `SELECT` queries to read data
- `SELECT COUNT(*)` / `SELECT MAX()` for statistics
- Schema metadata queries

**Never** perform INSERT, UPDATE, DELETE, CREATE, DROP, or any DDL/DML operations on source connections. All write operations (staging tables, upserts, deletes) occur exclusively on the **target** database.

When adding new features, maintain this invariant. Any code that operates on source connections must be read-only.

### Adding a New Feature

1. Create models in `/Models` if needed
2. Add configuration in `/Configuration/SyncServiceConfig.cs`
3. Implement service logic in `/Services`
4. Update `Program.cs` if new endpoints needed
5. Update this CLAUDE.md file
6. Add tests (when test project is created)

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

## Quick Reference

### Run Locally
```bash
dotnet run
```

The dashboard opens automatically at `http://localhost:5123/dashboard`

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

## Project History

- **Initial**: SQL Server to PostgreSQL sync service
- **Update**: Added bi-directional sync support (all 4 source/target combinations)
- **Architecture**: Multi-profile, timer-based scheduler with HTTP API
- **Developer Stack**: C# / .NET 8, SQL Server, PostgreSQL

---

*Last Updated: Dashboard now shows failed tables prominently when a profile fails; fixed sync history recording for validation failures*
