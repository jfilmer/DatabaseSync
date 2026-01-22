# Windows Server Installation Guide

This guide walks through deploying DatabaseSync as a Windows Service.

## Prerequisites

- Windows Server 2016 or later
- Administrator access
- Network access to source and target databases
- (Optional) .NET 8 SDK on build machine

---

## Option A: Build and Deploy (Recommended)

### Step 1: Publish the Application

Run this from your development machine (where the source code is):

```powershell
# Navigate to project directory (the one containing DatabaseSync.csproj)
cd DatabaseSync/DatabaseSync

# Publish as self-contained executable (no .NET runtime needed on server)
dotnet publish -c Release -r win-x64 --self-contained true -o ./publish
```

**Note:** Run from the project folder (with `.csproj`), not the solution folder (with `.sln`).

This creates a `publish` folder with everything needed to run on the server.

### Step 2: Copy Files to Server

Copy the `publish` folder to the server. Common locations:

```
C:\Services\DatabaseSync
D:\Apps\DatabaseSync
```

**Using PowerShell (from your machine):**
```powershell
# Copy to server (adjust paths as needed)
Copy-Item -Path ./publish/* -Destination "\\SERVER\C$\Services\DatabaseSync" -Recurse
```

**Or use RDP** and manually copy the folder.

### Step 3: Configure the Application

On the server, edit `appsettings.json`:

```powershell
# Open in Notepad
notepad C:\Services\DatabaseSync\appsettings.json
```

**Minimal configuration:**
```json
{
  "SyncService": {
    "HttpPort": 5123,
    "LogPath": "D:/Logs/DatabaseSync",
    "Profiles": [
      {
        "ProfileName": "production-sync",
        "Description": "Production to Staging sync",

        "SourceConnection": {
          "Type": "SqlServer",
          "ConnectionString": "Server=source-server;Database=MyDB;User Id=syncuser;Password=yourpassword;TrustServerCertificate=True"
        },

        "TargetConnection": {
          "Type": "PostgreSql",
          "ConnectionString": "Host=target-server;Database=mydb;Username=syncuser;Password=yourpassword"
        },

        "Schedule": {
          "StartTime": "06:00",
          "IntervalMinutes": 60,
          "RunImmediatelyOnStart": false,
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
    ]
  }
}
```

### Step 4: Create Log Directory

```powershell
# Create log directory (must match LogPath in config)
New-Item -ItemType Directory -Force -Path "D:\Logs\DatabaseSync"
```

### Step 5: Test the Application

Before installing as a service, test it runs correctly:

```powershell
# Run directly to test
cd C:\Services\DatabaseSync
.\DatabaseSync.exe
```

You should see:
- Configuration loading messages with file timestamps
- "Database Sync Service" banner
- Profile information
- Dashboard URL

Open `http://localhost:5123/dashboard` in a browser to verify.

Press `Ctrl+C` to stop.

### Step 6: Install as Windows Service

**Open PowerShell as Administrator:**

```powershell
# Create the service
sc.exe create DatabaseSync binPath="C:\Services\DatabaseSync\DatabaseSync.exe" start=auto DisplayName="Database Sync Service"

# Set description
sc.exe description DatabaseSync "Synchronizes data between databases - MS SQL - PostGreSQL"

# Configure automatic restart on failure
sc.exe failure DatabaseSync reset=86400 actions=restart/60000/restart/60000/restart/60000
```

**Explanation of failure recovery:**
- `reset=86400` - Reset failure count after 24 hours
- `actions=restart/60000/...` - Restart after 60 seconds on 1st, 2nd, and 3rd failures

### Step 7: Configure Service Account (Optional but Recommended)

By default, the service runs as `Local System`. For better security:

```powershell
# Run as a specific user (e.g., a service account)
sc.exe config DatabaseSync obj="DOMAIN\ServiceAccount" password="password"
```

Or use **Services MMC** (`services.msc`):
1. Find "Database Sync Service"
2. Right-click > Properties > Log On tab
3. Select "This account" and enter credentials

### Step 8: Start the Service

```powershell
# Start the service
sc.exe start DatabaseSync

# Verify it's running
sc.exe query DatabaseSync
```

### Step 9: Configure Firewall (if accessing dashboard remotely)

```powershell
# Allow inbound connections on port 5123
New-NetFirewallRule -DisplayName "DatabaseSync Dashboard" -Direction Inbound -Port 5123 -Protocol TCP -Action Allow
```

---

## Service Management Commands

### Basic Operations

```powershell
# Check status
sc.exe query DatabaseSync

# Start service
sc.exe start DatabaseSync

# Stop service
sc.exe stop DatabaseSync

# Restart service
sc.exe stop DatabaseSync; Start-Sleep -Seconds 5; sc.exe start DatabaseSync
```

### Update the Application

```powershell
# 1. Stop the service
sc.exe stop DatabaseSync

# 2. Wait for it to stop
Start-Sleep -Seconds 10

# 3. Copy new files (from your machine)
Copy-Item -Path ./publish/* -Destination "C:\Services\DatabaseSync" -Recurse -Force

# 4. Start the service
sc.exe start DatabaseSync
```

### View Logs

```powershell
# View recent log file
Get-Content "D:\Logs\DatabaseSync\sync-*.log" -Tail 100

# Follow log in real-time
Get-Content "D:\Logs\DatabaseSync\sync-*.log" -Tail 50 -Wait
```

### Uninstall Service

```powershell
# Stop first
sc.exe stop DatabaseSync

# Delete service
sc.exe delete DatabaseSync

# Optionally remove files
Remove-Item -Path "C:\Services\DatabaseSync" -Recurse -Force
```

---

## Troubleshooting

### Service Won't Start

1. **Check Windows Event Viewer:**
   - Open Event Viewer (`eventvwr.msc`)
   - Navigate to: Windows Logs > Application
   - Look for errors from "DatabaseSync"

2. **Test manually:**
   ```powershell
   cd C:\Services\DatabaseSync
   .\DatabaseSync.exe
   ```
   This shows startup errors in the console.

3. **Check config file:**
   - Verify JSON syntax is valid
   - Verify connection strings are correct
   - Verify LogPath directory exists

### Common Errors

| Error | Solution |
|-------|----------|
| "Access denied" on log directory | Grant write permissions to service account |
| "Cannot connect to database" | Check firewall, verify connection string |
| "Address already in use" | Another process is using port 5123; change `HttpPort` |
| "Configuration file not found" | Ensure `appsettings.json` is in the service directory |

### Check Configuration on Startup

The service logs configuration details on startup:
```
Loading configuration from: C:\Services\DatabaseSync\
  Environment: Production
  appsettings.json: 2,456 bytes, modified 2024-12-02 10:30:45
Loaded 3 profile(s) from configuration
```

If you see old timestamps, the file wasn't updated correctly.

---

## API Endpoints

Once running, the service exposes these endpoints:

| Endpoint | Description |
|----------|-------------|
| `http://localhost:5123/dashboard` | Web dashboard |
| `http://localhost:5123/health` | Health check |
| `http://localhost:5123/status` | JSON status of all profiles |
| `http://localhost:5123/sync/{profile}` | Trigger sync (POST) |

### Trigger Sync Manually

```powershell
# Trigger specific profile
Invoke-WebRequest -Method POST -Uri "http://localhost:5123/sync/production-sync"

# Trigger with full refresh
Invoke-WebRequest -Method POST -Uri "http://localhost:5123/sync/production-sync?fullRefresh=true"

# Check status
Invoke-WebRequest -Uri "http://localhost:5123/status" | Select-Object -ExpandProperty Content
```

---

## Quick Reference: All Commands

```powershell
# === INSTALLATION ===
# Publish (on dev machine)
dotnet publish -c Release -r win-x64 --self-contained true -o ./publish

# Create service (on server, as Admin)
sc.exe create DatabaseSync binPath="C:\Services\DatabaseSync\DatabaseSync.exe" start=auto DisplayName="Database Sync Service"
sc.exe description DatabaseSync "Synchronizes data between SQL Server and PostgreSQL databases"
sc.exe failure DatabaseSync reset=86400 actions=restart/60000/restart/60000/restart/60000

# Create log directory
New-Item -ItemType Directory -Force -Path "D:\Logs\DatabaseSync"

# Firewall rule (if needed)
New-NetFirewallRule -DisplayName "DatabaseSync Dashboard" -Direction Inbound -Port 5123 -Protocol TCP -Action Allow

# === MANAGEMENT ===
sc.exe start DatabaseSync
sc.exe stop DatabaseSync
sc.exe query DatabaseSync

# === UPDATE ===
sc.exe stop DatabaseSync
Start-Sleep -Seconds 10
Copy-Item -Path ./publish/* -Destination "C:\Services\DatabaseSync" -Recurse -Force
sc.exe start DatabaseSync

# === UNINSTALL ===
sc.exe stop DatabaseSync
sc.exe delete DatabaseSync
```

---

## Option B: Framework-Dependent Deployment

If .NET 8 Runtime is already installed on the server, you can publish a smaller package:

```powershell
# Publish (smaller, but requires .NET 8 on server)
dotnet publish -c Release -r win-x64 --self-contained false -o ./publish

# Install .NET 8 Runtime on server (if needed)
# Download from: https://dotnet.microsoft.com/download/dotnet/8.0
```

Service installation steps are the same as Option A.
