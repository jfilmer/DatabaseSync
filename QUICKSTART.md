# Database Sync Service - Quick Start Guide

A high-performance database synchronization service supporting bi-directional sync between SQL Server and PostgreSQL.

## Supported Sync Types

| Source | Target | Use Case |
|--------|--------|----------|
| SQL Server | SQL Server | Replicate between SQL Server instances |
| SQL Server | PostgreSQL | Migrate or replicate to PostgreSQL |
| PostgreSQL | PostgreSQL | Replicate between PostgreSQL instances |
| PostgreSQL | SQL Server | Migrate or replicate to SQL Server |

## Requirements

- **.NET 8.0 Runtime** or SDK
- **Source database**: Read access to source tables
- **Target database**: Read/Write access, permission to create tables (if using `CreateIfMissing`)
- **Primary keys**: All synced tables must have a primary key

## Configuration

All configuration is done in `appsettings.json`. The service supports multiple sync profiles, each with its own source, target, schedule, and table list.

### Minimal Configuration Example

```json
{
  "SyncService": {
    "HttpPort": 5123,
    "Profiles": [
      {
        "ProfileName": "MySync",
        "SourceConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=source-server;Database=SourceDB;User Id=user;Password=pass;TrustServerCertificate=True"
        },
        "TargetConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=target-server;Database=TargetDB;User Id=user;Password=pass;TrustServerCertificate=True"
        },
        "Schedule": {
          "IntervalMinutes": 15,
          "RunImmediatelyOnStart": true,
          "Enabled": true
        },
        "Tables": [
          {
            "SourceTable": "Customers",
            "Mode": "FullRefresh"
          }
        ]
      }
    ]
  }
}
```

## Profile Execution Mode

When running multiple profiles, you can control whether they run in parallel or sequentially.

```json
{
  "SyncService": {
    "ProfileExecutionMode": "Sequential",
    "Profiles": [ ... ]
  }
}
```

| Mode | Description |
|------|-------------|
| `Parallel` | Profiles run independently based on their own schedules (default) |
| `Sequential` | Profiles run in order - each waits for the previous to complete |

**When to use Sequential:**
- Profile 1 syncs data that Profile 2 depends on (e.g., Profile 1 syncs source A → B, Profile 2 syncs B → C)
- You want to minimize database load by not running multiple syncs simultaneously
- You have data dependencies between different database pairs

**When to use Parallel:**
- Profiles are independent and don't share data dependencies
- You want faster overall sync time by running profiles concurrently
- Different profiles sync at different intervals and shouldn't wait for each other

**Example - Sequential with data dependencies:**
```json
{
  "SyncService": {
    "ProfileExecutionMode": "Sequential",
    "Profiles": [
      {
        "ProfileName": "prod-to-staging",
        "Description": "Sync production to staging first"
      },
      {
        "ProfileName": "staging-to-reporting",
        "Description": "Then sync staging to reporting"
      }
    ]
  }
}
```

In Sequential mode, profiles execute in the order they appear in the `Profiles` array.

## Blackout Window

Prevent new syncs from starting during maintenance windows (e.g., database backups). Running syncs are allowed to complete.

```json
{
  "SyncService": {
    "BlackoutWindow": {
      "Enabled": true,
      "StartTime": "23:00",
      "EndTime": "05:00"
    },
    "Profiles": [ ... ]
  }
}
```

| Field | Description |
|-------|-------------|
| `Enabled` | Enable or disable the blackout window |
| `StartTime` | Start time in 24-hour format ("HH:mm") |
| `EndTime` | End time in 24-hour format ("HH:mm") |

**Key behaviors:**
- Uses local time (not UTC)
- Supports overnight windows (e.g., 23:00 to 05:00 spans midnight)
- Running syncs are allowed to complete - only new syncs are blocked
- Logs when entering/exiting the blackout window
- Profiles can still be triggered manually via API during blackout (if needed)

**Example use cases:**
- Database backup window: `"StartTime": "02:00", "EndTime": "04:00"`
- Overnight maintenance: `"StartTime": "23:00", "EndTime": "05:00"`
- Weekend maintenance: Combine with `DaysOfWeek` in schedule config

## Automatic Column Filtering

When the target table has fewer columns than the source, the sync automatically skips source columns that don't exist in the target. This allows syncing between tables with different schemas without errors.

**How it works:**
1. Before syncing, compares source and target table schemas
2. Only syncs columns that exist in both tables
3. Logs a warning listing any skipped columns
4. Records skipped columns in sync history warnings

**Example log output:**
```
Skipping 3 source columns not found in target: CreatedBy, ModifiedBy, IsDeleted
```

**When this is useful:**
- Target database has a simplified schema
- Some columns are deprecated in the target
- Target table is a view or subset of the source

## Connection Configuration

### Database Type Values

| Database | Accepted Values |
|----------|-----------------|
| SQL Server | `"SqlServer"`, `"mssql"` |
| PostgreSQL | `"PostgreSql"`, `"postgres"`, `"pgsql"` |

### Connection String Examples

#### SQL Server

**Basic connection (default port 1433):**
```json
{
  "Type": "SqlServer",
  "ConnectionString": "Server=myserver.example.com;Database=MyDatabase;User Id=myuser;Password=mypassword;TrustServerCertificate=True"
}
```

**With custom port:**
```json
{
  "Type": "SqlServer",
  "ConnectionString": "Server=myserver.example.com,8484;Database=MyDatabase;User Id=myuser;Password=mypassword;TrustServerCertificate=True"
}
```

**With extended timeout (for large tables):**
```json
{
  "Type": "SqlServer",
  "ConnectionString": "Server=myserver.example.com,1433;Database=MyDatabase;User Id=myuser;Password=mypassword;TrustServerCertificate=True;Command Timeout=3600"
}
```

**Windows Authentication (Integrated Security):**
```json
{
  "Type": "SqlServer",
  "ConnectionString": "Server=myserver.example.com;Database=MyDatabase;Integrated Security=True;TrustServerCertificate=True"
}
```

**SQL Server connection string parameters:**
| Parameter | Description | Example |
|-----------|-------------|---------|
| `Server` | Server hostname and optional port | `myserver.com,1433` |
| `Database` | Database name | `MyDatabase` |
| `User Id` | SQL authentication username | `sync_user` |
| `Password` | SQL authentication password | `MyPassword123` |
| `TrustServerCertificate` | Skip SSL certificate validation | `True` |
| `Command Timeout` | Query timeout in seconds | `3600` |
| `Integrated Security` | Use Windows authentication | `True` |

#### PostgreSQL

**Basic connection:**
```json
{
  "Type": "PostgreSql",
  "ConnectionString": "Host=myserver.example.com;Port=5432;Database=mydatabase;Username=myuser;Password=mypassword"
}
```

**With extended timeout:**
```json
{
  "Type": "PostgreSql",
  "ConnectionString": "Host=myserver.example.com;Port=5432;Database=mydatabase;Username=myuser;Password=mypassword;Command Timeout=3600"
}
```

**With SSL mode:**
```json
{
  "Type": "PostgreSql",
  "ConnectionString": "Host=myserver.example.com;Port=5432;Database=mydatabase;Username=myuser;Password=mypassword;SSL Mode=Require;Trust Server Certificate=True"
}
```

**PostgreSQL connection string parameters:**
| Parameter | Description | Example |
|-----------|-------------|---------|
| `Host` | Server hostname | `myserver.example.com` |
| `Port` | Server port (default 5432) | `5432` |
| `Database` | Database name | `mydatabase` |
| `Username` | Database username | `sync_user` |
| `Password` | Database password | `MyPassword123` |
| `Command Timeout` | Query timeout in seconds | `3600` |
| `SSL Mode` | SSL connection mode | `Require`, `Prefer`, `Disable` |
| `Trust Server Certificate` | Skip SSL certificate validation | `True` |

## Table Configuration

### Required Fields

| Field | Description |
|-------|-------------|
| `SourceTable` | Name of the table in the source database |
| `Mode` | Sync mode: `"FullRefresh"` or `"Incremental"` |

### Optional Fields

| Field | Default | Description |
|-------|---------|-------------|
| `TargetTable` | Same as source | Name of the table in target database |
| `TimestampColumn` | - | **Required for Incremental mode**. Column containing last modified timestamp |
| `FallbackTimestampColumn` | - | Fallback column when `TimestampColumn` is NULL (uses COALESCE) |
| `LookbackHours` | `0` | Re-sync rows from (lastSyncTime - hours) to catch late changes |
| `CreateIfMissing` | `false` | Auto-create target table if it doesn't exist |
| `DeleteMode` | `"None"` | `"None"` or `"Sync"` (delete rows not in source) |
| `Priority` | `100` | Lower numbers sync first. Same priority = parallel |
| `SourceFilter` | - | WHERE clause to filter source data (without `WHERE` keyword) |

### Sync Modes Explained

#### FullRefresh Mode

```json
{
  "SourceTable": "Products",
  "Mode": "FullRefresh"
}
```

**How it works:**
1. Reads ALL rows from the source table
2. Compares each row with the target using the primary key
3. Inserts new rows, updates existing rows
4. Optionally deletes rows not in source (if `DeleteMode: "Sync"`)

**When to use:**
- Small to medium tables (under 100k rows)
- Tables without a reliable timestamp column
- When you need guaranteed accuracy (e.g., lookup tables, reference data)
- Initial data load or when data integrity is critical

**Trade-offs:**
- Higher resource usage - reads entire table every sync
- Slower for large tables
- More network traffic

#### Incremental Mode

```json
{
  "SourceTable": "Orders",
  "Mode": "Incremental",
  "TimestampColumn": "ModifiedDate"
}
```

**How it works:**
1. Tracks the last sync time in the `_sync_history` table
2. Only reads rows where `TimestampColumn > lastSyncTime`
3. Inserts new rows, updates changed rows
4. Much faster since it only processes recent changes

**When to use:**
- Large tables (100k+ rows)
- Tables with a reliable "last modified" timestamp column
- High-frequency sync schedules (every few minutes)
- When minimizing database load is important

**Requirements:**
- Table MUST have a `TimestampColumn` that updates when rows change
- Common column names: `ModifiedDate`, `UpdatedAt`, `LastModified`, `ChangeDate`

**Trade-offs:**
- Won't detect changes if timestamp isn't updated
- First sync after setup processes all rows (uses minimum date)
- Requires proper indexing on timestamp column for performance

#### Incremental with Lookback

Use `LookbackHours` to re-sync recent data and catch late-arriving changes:

```json
{
  "SourceTable": "Orders",
  "Mode": "Incremental",
  "TimestampColumn": "ModifiedDate",
  "LookbackHours": 72
}
```

**How it works:**
1. Gets last sync time from history (e.g., `2025-01-15 10:00:00`)
2. Subtracts lookback hours (72h = 3 days): `2025-01-12 10:00:00`
3. Syncs all rows where `TimestampColumn > 2025-01-12 10:00:00`

**When to use:**
- Data may be backdated or modified after initial entry
- Source system has delayed updates or batch corrections
- You need to catch changes that were missed due to timing issues
- Replication from systems where timestamps can be retroactively updated

**Example values:**
| LookbackHours | Use Case |
|---------------|----------|
| `0` | No lookback (default) - only sync new changes |
| `24` | Re-sync last 24 hours - catch same-day corrections |
| `72` | Re-sync last 3 days - catch weekend batch updates |
| `168` | Re-sync last week - for systems with weekly reconciliation |

#### FallbackTimestampColumn

Use `FallbackTimestampColumn` when new records may have NULL values in the primary timestamp column:

```json
{
  "SourceTable": "Orders",
  "Mode": "Incremental",
  "TimestampColumn": "LastEditDT",
  "FallbackTimestampColumn": "EntryDT",
  "LookbackHours": 72
}
```

**How it works:**
1. Generates `WHERE COALESCE(LastEditDT, EntryDT) > @lastSyncTime`
2. If `LastEditDT` is NULL (new record never edited), uses `EntryDT` instead
3. Ensures new records are included even if the primary timestamp is NULL

**When to use:**
- Tables where `ModifiedDate` or `LastEditDT` is only populated on updates, not inserts
- Tables where new records have NULL timestamps until first edit
- Any scenario where your primary timestamp column may be NULL for valid records

**Common patterns:**
| TimestampColumn | FallbackTimestampColumn | Scenario |
|-----------------|------------------------|----------|
| `LastEditDT` | `EntryDT` | Modified date NULL until first edit |
| `ModifiedDate` | `CreatedDate` | Modified date NULL until first update |
| `UpdatedAt` | `InsertedAt` | Updated timestamp NULL until first change |

### CreateIfMissing Option

```json
{
  "SourceTable": "NewTable",
  "Mode": "FullRefresh",
  "CreateIfMissing": true
}
```

**How it works:**
- If the target table doesn't exist, automatically creates it
- Copies the schema from the source table (columns, types, primary key)
- Then proceeds with normal sync

**When to use:**
- Initial setup or migrations
- When syncing to a new/empty database
- Development environments where you want automatic table creation

**When NOT to use:**
- Production environments where you want explicit control over schema
- When target table needs different structure than source
- When you have custom indexes, constraints, or triggers on target

**What gets created:**
- All columns with matching data types
- Primary key constraint
- NOT NULL constraints

**What does NOT get created:**
- Foreign keys
- Indexes (other than primary key)
- Triggers
- Default values
- Computed columns

### Table Configuration Examples

**Full Refresh (simple):**
```json
{
  "SourceTable": "LookupTable",
  "Mode": "FullRefresh",
  "DeleteMode": "Sync"
}
```

**Incremental with all options:**
```json
{
  "SourceTable": "Orders",
  "TargetTable": "orders",
  "Mode": "Incremental",
  "TimestampColumn": "UpdatedAt",
  "CreateIfMissing": true,
  "DeleteMode": "Sync",
  "Priority": 2,
  "SourceFilter": "Status != 'Deleted' AND Year >= 2020"
}
```

## Schedule Configuration

| Field | Description |
|-------|-------------|
| `StartTime` | Time of day for first run (24h format, e.g., `"06:00"`) |
| `IntervalMinutes` | Minutes between sync runs |
| `RunImmediatelyOnStart` | Run sync when service starts |
| `Enabled` | Enable/disable scheduled sync (can still trigger via API) |
| `DaysOfWeek` | Array of days to run (0=Sunday, 1=Monday, etc.) |

**Example - Every 15 minutes, 24/7:**
```json
{
  "IntervalMinutes": 15,
  "RunImmediatelyOnStart": true,
  "Enabled": true
}
```

**Example - Daily at 2 AM, weekdays only:**
```json
{
  "StartTime": "02:00",
  "IntervalMinutes": 1440,
  "RunImmediatelyOnStart": false,
  "Enabled": true,
  "DaysOfWeek": [1, 2, 3, 4, 5]
}
```

## Options Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `MaxParallelTables` | `4` | Max tables to sync simultaneously (same priority) |
| `CommandTimeoutSeconds` | `300` | Database command timeout |
| `EnableSyncHistory` | `true` | Track sync history in `_sync_history` table |
| `UseHistoryForIncrementalSync` | `true` | Use history to determine last sync time |
| `StopOnError` | `false` | Stop profile sync if any table fails |

```json
{
  "Options": {
    "MaxParallelTables": 4,
    "CommandTimeoutSeconds": 3600,
    "EnableSyncHistory": true,
    "UseHistoryForIncrementalSync": true,
    "StopOnError": false
  }
}
```

### Options Explained - When to Change Defaults

| Option | When to Increase | When to Decrease |
|--------|------------------|------------------|
| `MaxParallelTables` | Many independent tables, powerful database server | Database struggles with concurrent load, shared/limited resources |
| `CommandTimeoutSeconds` | Very large tables (millions of rows), slow network | Never (default 300s is usually fine for small tables) |

**`EnableSyncHistory`** - Keep `true` unless:
- You're only doing one-time migrations and don't need history
- You're syncing to a database where you can't create the `_sync_history` table

**`UseHistoryForIncrementalSync`** - Keep `true` unless:
- You want to force full scans every time
- You have an external system tracking last sync time

**`StopOnError`** - Set to `true` when:
- Tables have foreign key dependencies and one failure should halt the whole sync
- Data integrity is more important than partial sync completion

## Configuration Quick Reference

### Which Sync Mode to Use?

| Table Characteristics | Recommended Mode | DeleteMode |
|----------------------|------------------|------------|
| Small lookup table (< 10k rows) | `FullRefresh` | `Sync` |
| Large table with timestamp column | `Incremental` | `None` or `Sync` |
| Large table without timestamp | `FullRefresh` | `None` (be careful!) |
| Transaction log / append-only | `Incremental` | `None` |
| Reference data (must match exactly) | `FullRefresh` | `Sync` |

### DeleteMode Guidance

| DeleteMode | Use When | Don't Use When |
|------------|----------|----------------|
| `None` | Append-only tables, audit logs, target has extra data you want to keep | You need target to exactly mirror source |
| `Sync` | Lookup tables, reference data, target must exactly match source | Large tables with no deletes in source (wastes time checking) |

### Priority Numbers

Use priorities when tables have foreign key dependencies:

```json
{
  "Tables": [
    { "SourceTable": "Categories", "Priority": 1 },
    { "SourceTable": "Products", "Priority": 2 },
    { "SourceTable": "OrderItems", "Priority": 3 }
  ]
}
```

- **Lower numbers sync first**
- **Same priority = synced in parallel**
- Parent tables should have lower priority than child tables

## Complete Configuration Examples

### SQL Server to SQL Server

```json
{
  "SyncService": {
    "HttpPort": 5123,
    "Profiles": [
      {
        "ProfileName": "Production",
        "Description": "Replicate production data to reporting server",

        "SourceConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=prod-sql.company.com;Database=ProdDB;User Id=sync_user;Password=SecurePass123;TrustServerCertificate=True;Command Timeout=3600"
        },

        "TargetConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=report-sql.company.com;Database=ReportDB;User Id=sync_user;Password=SecurePass123;TrustServerCertificate=True;Command Timeout=3600"
        },

        "Schedule": {
          "IntervalMinutes": 15,
          "RunImmediatelyOnStart": true,
          "Enabled": true
        },

        "Options": {
          "MaxParallelTables": 4,
          "CommandTimeoutSeconds": 3600,
          "EnableSyncHistory": true
        },

        "Tables": [
          {
            "SourceTable": "Customers",
            "Mode": "Incremental",
            "TimestampColumn": "ModifiedDate",
            "DeleteMode": "Sync",
            "Priority": 1
          },
          {
            "SourceTable": "Orders",
            "Mode": "Incremental",
            "TimestampColumn": "ModifiedDate",
            "DeleteMode": "None",
            "Priority": 2
          },
          {
            "SourceTable": "OrderItems",
            "Mode": "Incremental",
            "TimestampColumn": "CreatedDate",
            "DeleteMode": "Sync",
            "Priority": 3
          }
        ]
      }
    ]
  }
}
```

### SQL Server to PostgreSQL

```json
{
  "SyncService": {
    "HttpPort": 5123,
    "Profiles": [
      {
        "ProfileName": "Migration",
        "Description": "Migrate SQL Server data to PostgreSQL",

        "SourceConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=sql-server.company.com;Database=LegacyDB;User Id=reader;Password=ReadPass;TrustServerCertificate=True"
        },

        "TargetConnection": {
          "Type": "PostgreSql",
          "ConnectionString": "Host=postgres.company.com;Port=5432;Database=newdb;Username=writer;Password=WritePass"
        },

        "Schedule": {
          "IntervalMinutes": 60,
          "RunImmediatelyOnStart": true,
          "Enabled": true
        },

        "Tables": [
          {
            "SourceTable": "Users",
            "TargetTable": "users",
            "Mode": "FullRefresh",
            "CreateIfMissing": true,
            "DeleteMode": "Sync"
          }
        ]
      }
    ]
  }
}
```

### PostgreSQL to SQL Server

```json
{
  "SyncService": {
    "HttpPort": 5123,
    "Profiles": [
      {
        "ProfileName": "Analytics",
        "Description": "Sync PostgreSQL analytics to SQL Server data warehouse",

        "SourceConnection": {
          "Type": "PostgreSql",
          "ConnectionString": "Host=analytics-pg.company.com;Port=5432;Database=analytics;Username=reader;Password=ReadPass"
        },

        "TargetConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=datawarehouse.company.com;Database=DW;User Id=etl_user;Password=ETLPass;TrustServerCertificate=True"
        },

        "Schedule": {
          "StartTime": "03:00",
          "IntervalMinutes": 1440,
          "Enabled": true
        },

        "Tables": [
          {
            "SourceTable": "daily_metrics",
            "TargetTable": "DailyMetrics",
            "Mode": "Incremental",
            "TimestampColumn": "created_at",
            "CreateIfMissing": true
          }
        ]
      }
    ]
  }
}
```

## Running the Service

### Development (Linux/macOS)

```bash
# Start (foreground - Ctrl+C to stop)
dotnet run

# Start (background)
dotnet run &

# Stop (kills process on port 5123)
lsof -ti:5123 | xargs -r kill -9

# Restart
lsof -ti:5123 | xargs -r kill -9; sleep 2 && dotnet run
```

### Development (Windows PowerShell)

```powershell
# Start (foreground - Ctrl+C to stop)
dotnet run

# Start (background)
Start-Process -NoNewWindow dotnet run

# Stop (kills process on port 5123)
Get-NetTCPConnection -LocalPort 5123 -ErrorAction SilentlyContinue |
    ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }

# Or stop by process name
Get-Process DatabaseSync -ErrorAction SilentlyContinue | Stop-Process -Force
```

### Production (Windows Service)
```cmd
:: Install as Windows Service
sc create DatabaseSync binPath="C:\Apps\DatabaseSync\DatabaseSync.exe" start=auto

:: Start/Stop/Restart
sc start DatabaseSync
sc stop DatabaseSync
sc stop DatabaseSync && sc start DatabaseSync
```

### Production (Linux systemd)
```bash
# Install as systemd service
sudo cp database-sync.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable database-sync

# Start/Stop/Restart
sudo systemctl start database-sync
sudo systemctl stop database-sync
sudo systemctl restart database-sync

# Check status
sudo systemctl status database-sync

# View logs
sudo journalctl -u database-sync -f
```

## HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Status of all profiles |
| `/status/{profile}` | GET | Status of specific profile |
| `/profiles` | GET | List profile names |
| `/sync/{profile}` | POST | Trigger sync for profile |
| `/sync/{profile}?fullRefresh=true` | POST | Trigger full refresh sync |
| `/sync` | POST | Trigger all profiles |

### Examples

```bash
# Check health
curl http://localhost:5123/health

# Get status
curl http://localhost:5123/status

# Trigger sync
curl -X POST http://localhost:5123/sync/Production

# Trigger full refresh
curl -X POST "http://localhost:5123/sync/Production?fullRefresh=true"
```

## Troubleshooting

### Common Issues

**"SqlDateTime overflow" error:**
- Occurs on first incremental sync when no history exists
- Fixed in latest version - ensure you have the updated code

**"Connection refused" to PostgreSQL:**
- Check if `appsettings.Development.json` is overriding your configuration
- Verify the `Type` field matches your actual database

**"Table not found" error:**
- Verify table name and case sensitivity
- PostgreSQL uses lowercase by default
- SQL Server preserves original case

**Slow sync performance:**
- Increase `MaxParallelTables` for independent tables
- Use `Incremental` mode instead of `FullRefresh`
- Ensure proper indexes on `TimestampColumn`

### Logs

Logs are written to:
- Console (real-time)
- `logs/sync-{date}.log` (rolling daily files)

Set `LogLevel` to `"Debug"` in appsettings for verbose logging.
