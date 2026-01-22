# SQL Server → PostgreSQL Database Sync Service

A high-performance, cross-platform database synchronization service that copies data from Microsoft SQL Server to PostgreSQL.

## Features

- **Multi-Profile Support**: Configure multiple source/target database pairs
- **High Performance**: 50,000+ rows/second using PostgreSQL COPY protocol
- **Incremental Sync**: Only sync rows changed since last run
- **Synchronized Deletes**: Delete rows from target that don't exist in source
- **Parallel Processing**: Sync multiple tables concurrently
- **HTTP API**: Trigger syncs and check status via REST API
- **Flexible Scheduling**: Start time, interval, and day-of-week filtering
- **Sync History**: Automatic tracking of all sync operations
- **Cross-Platform**: Runs on Windows and Linux

## Quick Start

### 1. Configure

Edit `appsettings.json`:

```json
{
  "SyncService": {
    "HttpPort": 5123,
    "Profiles": [
      {
        "ProfileName": "MySync",
        "SourceConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=...;Database=...;User Id=...;Password=..."
        },
        "TargetConnection": {
          "Type": "PostgreSql", 
          "ConnectionString": "Host=...;Database=...;Username=...;Password=..."
        },
        "Schedule": {
          "StartTime": "00:00",
          "IntervalMinutes": 15,
          "Enabled": true
        },
        "Tables": [
          {
            "SourceTable": "Customers",
            "Mode": "Incremental",
            "TimestampColumn": "UpdatedAt",
            "DeleteMode": "Sync"
          }
        ]
      }
    ]
  }
}
```

### 2. Run

```bash
dotnet run
```

### 3. Monitor

```bash
# Check status
curl http://localhost:5123/status

# Trigger manual sync
curl -X POST http://localhost:5123/sync/MySync
```

## HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/status` | GET | Status of all profiles |
| `/status/{profile}` | GET | Status of specific profile |
| `/profiles` | GET | List profile names |
| `/sync/{profile}` | POST | Trigger sync for profile |
| `/sync/{profile}?fullRefresh=true` | POST | Force full refresh |
| `/sync` | POST | Trigger all profiles |

## Configuration Options

### Profile Settings

| Setting | Description |
|---------|-------------|
| `ProfileName` | Unique identifier for this sync profile |
| `SourceConnection` | SQL Server connection details |
| `TargetConnection` | PostgreSQL connection details |
| `Schedule` | When and how often to sync |
| `Options` | Performance and behavior settings |
| `Tables` | List of tables to sync |

### Schedule Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `StartTime` | "00:00" | Time of day for first run (HH:mm) |
| `IntervalMinutes` | 60 | Minutes between syncs |
| `RunImmediatelyOnStart` | true | Run sync when service starts |
| `Enabled` | true | Enable/disable automatic schedule |
| `DaysOfWeek` | null | Limit to specific days (0=Sun, 6=Sat) |

### Table Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `SourceTable` | (required) | Table name in SQL Server |
| `TargetTable` | (lowercase of source) | Table name in PostgreSQL |
| `Mode` | Incremental | FullRefresh or Incremental |
| `TimestampColumn` | null | Column for incremental sync |
| `Priority` | 100 | Sync order (lower = first) |
| `CreateIfMissing` | false | Create target table if missing |
| `DeleteMode` | None | None or Sync |
| `SourceFilter` | null | WHERE clause for source data |

### Delete Mode

| Mode | Description |
|------|-------------|
| `None` | Only insert and update rows - no deletes |
| `Sync` | Synchronized deletes - rows in target that don't exist in source are deleted |

**Note**: When `DeleteMode` is `Sync`, the service compares all primary key values between source and target, then deletes rows from target that no longer exist in source.

## Deployment

### Linux (systemd)

```ini
# /etc/systemd/system/database-sync.service
[Unit]
Description=Database Sync Service
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/database-sync
ExecStart=/opt/database-sync/DatabaseSync
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable database-sync
sudo systemctl start database-sync
```

### Windows Service

```cmd
sc create DatabaseSync binPath="C:\Apps\DatabaseSync\DatabaseSync.exe" start=auto
sc start DatabaseSync
```

### Docker

```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY publish/ .
EXPOSE 5123
ENTRYPOINT ["dotnet", "DatabaseSync.dll"]
```

## Type Mappings

| SQL Server | PostgreSQL |
|------------|------------|
| int | integer |
| bigint | bigint |
| bit | boolean |
| datetime/datetime2 | timestamp |
| varchar/nvarchar | varchar/text |
| uniqueidentifier | uuid |
| decimal/numeric | numeric |
| varbinary | bytea |

## Performance

Using staging table + COPY + upsert pattern:

| Rows | Approximate Time |
|------|------------------|
| 100,000 | 2-5 seconds |
| 1,000,000 | 20-45 seconds |
| 10,000,000 | 3-8 minutes |

## Documentation

See [CLAUDE.md](CLAUDE.md) for detailed project documentation and future plans.

## License

MIT License
