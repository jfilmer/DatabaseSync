using DatabaseSync.Configuration;
using DatabaseSync.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Serilog;

// When running as a Windows Service, the current directory is system32
// Use the application's base directory for logs and config
var basePath = AppContext.BaseDirectory;
var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production";

// Clear any file system caching - ensure we read fresh config files
var baseConfigPath = Path.Combine(basePath, "appsettings.json");
var envConfigPath = Path.Combine(basePath, $"appsettings.{environment}.json");

// Log config file info for debugging cache issues
Console.WriteLine($"Loading configuration from: {basePath}");
Console.WriteLine($"  Environment: {environment}");
if (File.Exists(baseConfigPath))
{
    var baseInfo = new FileInfo(baseConfigPath);
    Console.WriteLine($"  appsettings.json: {baseInfo.Length:N0} bytes, modified {baseInfo.LastWriteTime:yyyy-MM-dd HH:mm:ss}");
}
if (File.Exists(envConfigPath))
{
    var envInfo = new FileInfo(envConfigPath);
    Console.WriteLine($"  appsettings.{environment}.json: {envInfo.Length:N0} bytes, modified {envInfo.LastWriteTime:yyyy-MM-dd HH:mm:ss}");
}

// Read configuration early to get log path
// Use reloadOnChange: false to ensure we get a fresh read each startup
var earlyConfig = new ConfigurationBuilder()
    .SetBasePath(basePath)
    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
    .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: false)
    .Build();

var syncServiceConfig = earlyConfig.GetSection("SyncService").Get<SyncServiceConfig>() ?? new SyncServiceConfig();

// Determine log path - if relative, make it relative to basePath
var configuredLogPath = syncServiceConfig.LogPath ?? "logs";
var logDirectory = Path.IsPathRooted(configuredLogPath)
    ? configuredLogPath
    : Path.Combine(basePath, configuredLogPath);
var logPath = Path.Combine(logDirectory, "sync-.log");

// Ensure log directory exists
Directory.CreateDirectory(logDirectory);

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Warning)
    .MinimumLevel.Override("Microsoft.AspNetCore", Serilog.Events.LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
    .WriteTo.File(
        path: logPath,
        rollingInterval: RollingInterval.Day,
        outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

// Single-instance check using file lock
var lockFilePath = Path.Combine(AppContext.BaseDirectory, ".database-sync.lock");
FileStream? lockFile = null;

try
{
    lockFile = new FileStream(lockFilePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    // Write PID to lock file for debugging
    lockFile.SetLength(0);
    using var writer = new StreamWriter(lockFile, leaveOpen: true);
    writer.WriteLine($"PID: {Environment.ProcessId}");
    writer.WriteLine($"Started: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
    writer.Flush();
}
catch (IOException)
{
    // Another instance is running
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║  ERROR: Another instance of DatabaseSync is already running  ║");
    Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");
    Console.ResetColor();
    Console.WriteLine();
    Console.WriteLine($"Lock file: {lockFilePath}");
    Console.WriteLine();
    Console.WriteLine("If no other instance is running, delete the lock file and try again.");
    Console.WriteLine("Or kill the existing process first.");
    Log.Fatal("Another instance is already running. Lock file: {LockFile}", lockFilePath);
    return 1;
}

try
{
    Log.Information("╔══════════════════════════════════════════════════════════════╗");
    Log.Information("║        SQL Server/PostgreSQL Database Sync Service           ║");
    Log.Information("╚══════════════════════════════════════════════════════════════╝");

    // Set content root at builder creation (important for Windows Service where CWD is system32)
    var builder = WebApplication.CreateBuilder(new WebApplicationOptions
    {
        Args = args,
        ContentRootPath = basePath
    });

    // Enable Windows Service support (no-op on Linux/macOS)
    builder.Host.UseWindowsService();

    builder.Services.AddSerilog();

    // Explicitly add config files with reloadOnChange: false to avoid caching issues
    builder.Configuration.Sources.Clear();
    builder.Configuration
        .SetBasePath(basePath)
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
        .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: false)
        .AddEnvironmentVariables();

    // Load configuration
    var config = builder.Configuration.GetSection("SyncService").Get<SyncServiceConfig>()
        ?? new SyncServiceConfig();

    // Log what we loaded to help debug config issues
    Log.Information("Loaded {ProfileCount} profile(s) from configuration", config.Profiles.Count);
    foreach (var p in config.Profiles)
    {
        Log.Debug("  Profile '{Name}': {TableCount} tables", p.ProfileName, p.Tables.Count);
    }

    // Validate configuration
    var validationResult = ConfigurationValidator.Validate(config);

    if (validationResult.Errors.Any())
    {
        Log.Error("Configuration validation failed with {Count} error(s):", validationResult.Errors.Count);
        foreach (var error in validationResult.Errors)
        {
            Log.Error("  - {Error}", error);
        }
    }

    if (validationResult.Warnings.Any())
    {
        Log.Warning("Configuration has {Count} warning(s):", validationResult.Warnings.Count);
        foreach (var warning in validationResult.Warnings)
        {
            Log.Warning("  - {Warning}", warning);
        }
    }

    if (validationResult.IsValid && !validationResult.HasWarnings)
    {
        Log.Information("Configuration validated successfully - {Count} profile(s) loaded", config.Profiles.Count);
    }

    // Register services
    builder.Services.AddSingleton(config);
    builder.Services.AddSingleton(validationResult);
    builder.Services.AddSingleton<TypeMapper>();
    builder.Services.AddSingleton<ProfileScheduler>();
    builder.Services.AddHostedService(sp => sp.GetRequiredService<ProfileScheduler>());

    var app = builder.Build();

    // ══════════════════════════════════════════════════════════════
    // HTTP API Endpoints
    // ══════════════════════════════════════════════════════════════

    // Health check (conditionally enabled)
    if (config.EnableHealthChecks)
    {
        app.MapGet("/health", () => Results.Ok(new
        {
            Status = "healthy",
            Time = DateTime.UtcNow,
            Profiles = config.Profiles.Count
        })).WithName("HealthCheck");
    }

    // Get status of all profiles
    app.MapGet("/status", (ProfileScheduler scheduler) =>
    {
        var status = scheduler.GetAllProfileStatus();
        return Results.Ok(status);
    }).WithName("GetAllStatus");

    // Get status of a specific profile
    app.MapGet("/status/{profileName}", (string profileName, ProfileScheduler scheduler) =>
    {
        var status = scheduler.GetProfileStatus(profileName);
        
        return status != null 
            ? Results.Ok(status) 
            : Results.NotFound(new { Error = $"Profile '{profileName}' not found" });
    }).WithName("GetProfileStatus");

    // List all profile names
    app.MapGet("/profiles", (ProfileScheduler scheduler) =>
    {
        return Results.Ok(scheduler.GetProfileNames());
    }).WithName("ListProfiles");

    // Trigger sync for a specific profile
    app.MapPost("/sync/{profileName}", async (
        string profileName, 
        bool? fullRefresh, 
        ProfileScheduler scheduler) =>
    {
        try
        {
            Log.Information("API: Sync triggered for profile '{Profile}' (fullRefresh: {FullRefresh})", 
                profileName, fullRefresh ?? false);
            
            var result = await scheduler.TriggerProfileAsync(profileName, fullRefresh ?? false);
            
            return Results.Ok(new
            {
                Profile = profileName,
                RunId = result.RunId,
                Success = result.FailureCount == 0,
                TablesTotal = result.TableResults.Count,
                TablesSucceeded = result.SuccessCount,
                TablesFailed = result.FailureCount,
                RowsProcessed = result.TotalRowsProcessed,
                RowsInserted = result.TotalRowsInserted,
                RowsUpdated = result.TotalRowsUpdated,
                RowsDeleted = result.TotalRowsDeleted,
                Duration = FormatDuration(result.Duration.TotalSeconds),
                RowsPerSecond = (int)result.OverallRowsPerSecond,
                FailedTables = result.TableResults
                    .Where(t => !t.Success)
                    .Select(t => new { t.TableName, t.Error })
                    .ToList()
            });
        }
        catch (ArgumentException ex)
        {
            return Results.NotFound(new { Error = ex.Message });
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new { Error = ex.Message });
        }
    }).WithName("TriggerProfileSync");

    // Trigger sync for a specific table in a profile
    app.MapPost("/sync/{profileName}/{tableName}", async (
        string profileName,
        string tableName,
        bool? fullRefresh,
        ProfileScheduler scheduler) =>
    {
        try
        {
            Log.Information("API: Table sync triggered for '{Profile}/{Table}' (fullRefresh: {FullRefresh})",
                profileName, tableName, fullRefresh ?? false);

            var result = await scheduler.TriggerTableAsync(profileName, tableName, fullRefresh ?? false);

            return Results.Ok(new
            {
                Profile = profileName,
                Table = tableName,
                Success = result.Success,
                RowsProcessed = result.RowsProcessed,
                RowsInserted = result.RowsInserted,
                RowsUpdated = result.RowsUpdated,
                RowsDeleted = result.RowsDeleted,
                Duration = FormatDuration(result.Duration.TotalSeconds),
                Error = result.Error
            });
        }
        catch (ArgumentException ex)
        {
            return Results.NotFound(new { Error = ex.Message });
        }
        catch (InvalidOperationException ex)
        {
            return Results.Conflict(new { Error = ex.Message });
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Unexpected error syncing table {Table} in profile {Profile}", tableName, profileName);
            return Results.Json(new { Success = false, Error = ex.Message }, statusCode: 500);
        }
    }).WithName("TriggerTableSync");

    // Trigger sync for all enabled profiles
    app.MapPost("/sync", async (bool? fullRefresh, ProfileScheduler scheduler) =>
    {
        var results = new List<object>();

        foreach (var status in scheduler.GetAllProfileStatus().Where(s => !s.IsRunning))
        {
            try
            {
                var result = await scheduler.TriggerProfileAsync(status.ProfileName, fullRefresh ?? false);
                results.Add(new
                {
                    Profile = status.ProfileName,
                    Success = result.FailureCount == 0,
                    Tables = result.TableResults.Count,
                    RowsProcessed = result.TotalRowsProcessed,
                    Duration = FormatDuration(result.Duration.TotalSeconds)
                });
            }
            catch (Exception ex)
            {
                results.Add(new
                {
                    Profile = status.ProfileName,
                    Success = false,
                    Error = ex.Message
                });
            }
        }

        return Results.Ok(results);
    }).WithName("TriggerAllSync");

    // Get startup delay status
    app.MapGet("/startup-delay", (ProfileScheduler scheduler) =>
    {
        var status = scheduler.GetStartupDelayStatus();
        return Results.Ok(status);
    }).WithName("GetStartupDelayStatus");

    // Cancel startup delay and start syncs immediately
    app.MapPost("/startup-delay/cancel", (ProfileScheduler scheduler) =>
    {
        scheduler.CancelStartupDelay();
        return Results.Ok(new { Message = "Startup delay cancelled - syncs will begin immediately" });
    }).WithName("CancelStartupDelay");

    // Get sync history for a profile (JSON)
    app.MapGet("/history/{profileName}", async (string profileName, int? limit, ProfileScheduler scheduler) =>
    {
        var history = await scheduler.GetSyncHistoryAsync(profileName, limit ?? 50);
        return Results.Ok(history);
    }).WithName("GetSyncHistory");

    // HTML Dashboard - shows sync history for all profiles
    app.MapGet("/dashboard", async (ProfileScheduler scheduler, ConfigurationValidationResult configValidation) =>
    {
        var profiles = scheduler.GetProfileNames().ToList();
        var allHistory = new Dictionary<string, List<DatabaseSync.Models.SyncHistory>>();

        foreach (var profile in profiles)
        {
            allHistory[profile] = await scheduler.GetSyncHistoryAsync(profile, 100);
        }

        var startupDelay = scheduler.GetStartupDelayStatus();
        var html = GenerateDashboardHtml(scheduler.GetAllProfileStatus().ToList(), allHistory, configValidation, startupDelay);
        return Results.Content(html, "text/html");
    }).WithName("Dashboard");

    // Dashboard for specific profile
    app.MapGet("/dashboard/{profileName}", async (string profileName, ProfileScheduler scheduler) =>
    {
        var status = scheduler.GetProfileStatus(profileName);
        if (status == null)
        {
            return Results.NotFound($"Profile '{profileName}' not found");
        }

        var history = await scheduler.GetSyncHistoryAsync(profileName, 100);
        var html = GenerateProfileDashboardHtml(status, history);
        return Results.Content(html, "text/html");
    }).WithName("ProfileDashboard");

    // Admin: Get orphaned profile names in history (profiles that exist in history but not in config)
    app.MapGet("/admin/orphaned-profiles/{profileName}", async (string profileName, ProfileScheduler scheduler) =>
    {
        var profile = scheduler.GetProfile(profileName);
        if (profile == null)
        {
            return Results.NotFound(new { Error = $"Profile '{profileName}' not found in config" });
        }

        // Get history repository for this profile's target database
        DatabaseSync.Abstractions.ISyncHistoryRepository historyRepo;
        if (profile.TargetConnection.DatabaseType == DatabaseSync.Enums.DatabaseType.PostgreSql)
        {
            historyRepo = new DatabaseSync.PostgreSql.PostgreSqlSyncHistoryRepository(
                profile.TargetConnection.ConnectionString,
                app.Services.GetRequiredService<ILoggerFactory>().CreateLogger<DatabaseSync.PostgreSql.PostgreSqlSyncHistoryRepository>());
        }
        else
        {
            historyRepo = new DatabaseSync.SqlServer.SqlServerSyncHistoryRepository(
                profile.TargetConnection.ConnectionString,
                app.Services.GetRequiredService<ILoggerFactory>().CreateLogger<DatabaseSync.SqlServer.SqlServerSyncHistoryRepository>());
        }

        await historyRepo.InitializeAsync();
        var historyProfiles = await historyRepo.GetProfileNamesFromHistoryAsync();
        var configProfiles = config.Profiles.Select(p => p.EffectiveProfileId).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var orphanedProfiles = historyProfiles.Where(h => !configProfiles.Contains(h)).ToList();

        return Results.Ok(new
        {
            Database = profile.TargetConnection.ConnectionString.Split(';')
                .FirstOrDefault(p => p.StartsWith("Database=", StringComparison.OrdinalIgnoreCase) ||
                                     p.StartsWith("Initial Catalog=", StringComparison.OrdinalIgnoreCase)),
            ConfiguredProfiles = configProfiles.ToList(),
            ProfilesInHistory = historyProfiles,
            OrphanedProfiles = orphanedProfiles
        });
    }).WithName("GetOrphanedProfiles");

    // Admin: Rename profile in history (migrate old records to new profile ID)
    app.MapPost("/admin/rename-profile/{profileName}", async (
        string profileName,
        string oldProfileId,
        string newProfileId,
        ProfileScheduler scheduler) =>
    {
        var profile = scheduler.GetProfile(profileName);
        if (profile == null)
        {
            return Results.NotFound(new { Error = $"Profile '{profileName}' not found in config" });
        }

        // Get history repository for this profile's target database
        DatabaseSync.Abstractions.ISyncHistoryRepository historyRepo;
        if (profile.TargetConnection.DatabaseType == DatabaseSync.Enums.DatabaseType.PostgreSql)
        {
            historyRepo = new DatabaseSync.PostgreSql.PostgreSqlSyncHistoryRepository(
                profile.TargetConnection.ConnectionString,
                app.Services.GetRequiredService<ILoggerFactory>().CreateLogger<DatabaseSync.PostgreSql.PostgreSqlSyncHistoryRepository>());
        }
        else
        {
            historyRepo = new DatabaseSync.SqlServer.SqlServerSyncHistoryRepository(
                profile.TargetConnection.ConnectionString,
                app.Services.GetRequiredService<ILoggerFactory>().CreateLogger<DatabaseSync.SqlServer.SqlServerSyncHistoryRepository>());
        }

        await historyRepo.InitializeAsync();
        var updated = await historyRepo.RenameProfileAsync(oldProfileId, newProfileId);

        Log.Information("Renamed profile '{OldId}' to '{NewId}' in {Count} history records",
            oldProfileId, newProfileId, updated);

        return Results.Ok(new
        {
            OldProfileId = oldProfileId,
            NewProfileId = newProfileId,
            RecordsUpdated = updated
        });
    }).WithName("RenameProfile");

    // ══════════════════════════════════════════════════════════════
    // Start the application
    // ══════════════════════════════════════════════════════════════

    var url = $"http://0.0.0.0:{config.HttpPort}";
    
    Log.Information("");
    Log.Information("HTTP API available at: {Url}", url);
    if (config.EnableHealthChecks)
        Log.Information("  GET  /health              - Health check");
    Log.Information("  GET  /status              - Status of all profiles");
    Log.Information("  GET  /status/{{profile}}    - Status of specific profile");
    Log.Information("  GET  /profiles            - List profile names");
    Log.Information("  POST /sync/{{profile}}      - Trigger sync for profile");
    Log.Information("  POST /sync                - Trigger all profiles");
    Log.Information("  GET  /history/{{profile}}   - Sync history (JSON)");
    Log.Information("  GET  /dashboard           - HTML Dashboard");
    Log.Information("  GET  /dashboard/{{profile}} - Profile Dashboard");
    Log.Information("");

    app.Run(url);
}
catch (Exception ex)
{
    Log.Fatal(ex, "Application terminated unexpectedly");
    return 1;
}
finally
{
    await Log.CloseAndFlushAsync();
}

return 0;

// ══════════════════════════════════════════════════════════════
// HTML Dashboard Generation
// ══════════════════════════════════════════════════════════════

static string GenerateDashboardHtml(
    List<DatabaseSync.Models.ProfileStatusInfo> profiles,
    Dictionary<string, List<DatabaseSync.Models.SyncHistory>> allHistory,
    ConfigurationValidationResult? validationResult = null,
    DatabaseSync.Services.StartupDelayStatus? startupDelay = null)
{
    var sb = new System.Text.StringBuilder();
    sb.Append(@"<!DOCTYPE html>
<html lang=""en"">
<head>
    <meta charset=""UTF-8"">
    <meta name=""viewport"" content=""width=device-width, initial-scale=1.0"">
    <title>Database Sync Dashboard</title>
    <meta http-equiv=""refresh"" content=""30"">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        h1 { color: #00d9ff; }
        .refresh-info { color: #888; font-size: 0.9em; }
        .config-alert { padding: 15px 20px; border-radius: 8px; margin-bottom: 20px; }
        .config-alert-error { background: #5a1a1a; border: 1px solid #dc3545; color: #ff6b6b; }
        .config-alert-warning { background: #5a4a1a; border: 1px solid #ffc107; color: #ffd93d; }
        .config-alert-title { font-weight: bold; margin-bottom: 8px; font-size: 1.1em; }
        .config-alert ul { margin: 0; padding-left: 20px; }
        .config-alert li { margin: 4px 0; }
        .profiles { display: grid; gap: 20px; }
        .profile-card { background: #16213e; border-radius: 8px; padding: 20px; border-left: 4px solid #00d9ff; }
        .profile-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }
        .profile-name { font-size: 1.3em; font-weight: bold; color: #00d9ff; }
        .status-badge { padding: 4px 12px; border-radius: 12px; font-size: 0.85em; font-weight: bold; }
        .status-running { background: #ffc107; color: #000; }
        .status-success { background: #28a745; color: #fff; }
        .status-failed { background: #dc3545; color: #fff; }
        .status-disabled { background: #6c757d; color: #fff; }
        .status-pending { background: #17a2b8; color: #fff; }
        .current-tables { background: #1e3a5f; color: #ffc107; padding: 8px 12px; margin: 10px 0; border-radius: 4px; font-size: 0.9em; font-family: monospace; }
        .failed-tables-alert { background: #5a1a1a; border: 1px solid #dc3545; color: #ff6b6b; padding: 12px; margin: 15px 0; border-radius: 6px; font-size: 0.9em; }
        .failed-tables-alert ul { margin: 8px 0 0 0; padding-left: 20px; }
        .failed-tables-alert li { margin: 4px 0; cursor: help; }
        .failed-tables-alert li[title]:hover { background: #4a1515; border-radius: 3px; padding: 2px 4px; margin-left: -4px; }
        .failed-tables-alert strong { color: #ff8787; }
        .profile-meta { color: #888; font-size: 0.9em; margin-bottom: 15px; }
        .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-bottom: 15px; }
        .stat { background: #0f3460; padding: 12px; border-radius: 6px; text-align: center; }
        .stat-value { font-size: 1.4em; font-weight: bold; color: #00d9ff; }
        .stat-label { font-size: 0.8em; color: #888; margin-top: 4px; }
        table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
        th { background: #0f3460; padding: 10px; text-align: left; color: #00d9ff; }
        td { padding: 8px 10px; border-bottom: 1px solid #2a2a4a; }
        tr:hover { background: #1f1f3a; }
        .success { color: #28a745; }
        .failed { color: #dc3545; }
        .number { text-align: right; font-family: monospace; }
        .timestamp { color: #888; font-size: 0.85em; }
        .view-link { color: #00d9ff; text-decoration: none; }
        .view-link:hover { text-decoration: underline; }
        .no-data { color: #888; font-style: italic; padding: 20px; text-align: center; }
        .startup-delay-banner { background: linear-gradient(135deg, #1e3a5f 0%, #16213e 100%); border: 2px solid #ffc107; border-radius: 8px; padding: 20px; margin-bottom: 20px; display: flex; justify-content: space-between; align-items: center; }
        .startup-delay-info { flex: 1; }
        .startup-delay-title { color: #ffc107; font-size: 1.2em; font-weight: bold; margin-bottom: 8px; }
        .startup-delay-text { color: #eee; }
        .startup-delay-countdown { font-size: 2em; font-weight: bold; color: #00d9ff; font-family: monospace; min-width: 80px; text-align: center; }
        .start-now-btn { background: #28a745; color: #fff; border: none; padding: 12px 24px; border-radius: 6px; cursor: pointer; font-size: 1.1em; font-weight: bold; margin-left: 20px; transition: background 0.2s; }
        .start-now-btn:hover { background: #218838; }
        .start-now-btn:disabled { background: #666; cursor: not-allowed; }
        .profile-sync-btn { background: #0f3460; color: #00d9ff; border: 1px solid #00d9ff; padding: 6px 16px; border-radius: 4px; cursor: pointer; font-size: 0.9em; font-weight: bold; transition: all 0.2s; }
        .profile-sync-btn:hover { background: #00d9ff; color: #0f3460; }
        .profile-sync-btn:disabled { background: #333; color: #666; border-color: #666; cursor: not-allowed; }
        .profile-header { display: flex; justify-content: space-between; align-items: center; }
        .profile-header-left { display: flex; align-items: center; gap: 10px; }
    </style>
    <script>
        let countdownInterval = null;
        function startCountdown(seconds) {
            const countdownEl = document.getElementById('countdown');
            const bannerEl = document.getElementById('startup-delay-banner');
            if (!countdownEl || !bannerEl) return;

            let remaining = seconds;
            function update() {
                if (remaining <= 0) {
                    clearInterval(countdownInterval);
                    bannerEl.style.display = 'none';
                    location.reload();
                    return;
                }
                countdownEl.textContent = remaining + 's';
                remaining--;
            }
            update();
            countdownInterval = setInterval(update, 1000);
        }

        async function startNow() {
            const btn = document.getElementById('start-now-btn');
            const bannerEl = document.getElementById('startup-delay-banner');
            btn.disabled = true;
            btn.textContent = 'Starting...';

            try {
                const response = await fetch('/startup-delay/cancel', { method: 'POST' });
                if (response.ok) {
                    clearInterval(countdownInterval);
                    btn.textContent = 'Started!';
                    btn.style.background = '#28a745';
                    setTimeout(() => {
                        bannerEl.style.display = 'none';
                        location.reload();
                    }, 500);
                } else {
                    btn.textContent = 'Failed';
                    btn.style.background = '#dc3545';
                }
            } catch (e) {
                btn.textContent = 'Error';
                btn.style.background = '#dc3545';
            }
        }

        async function triggerProfileSync(profileName, btn) {
            const originalText = btn.textContent;
            btn.disabled = true;
            btn.textContent = 'Syncing...';

            try {
                const response = await fetch(`/sync/${encodeURIComponent(profileName)}`, { method: 'POST' });
                if (response.ok) {
                    btn.textContent = 'Started!';
                    btn.style.background = '#28a745';
                    btn.style.borderColor = '#28a745';
                    setTimeout(() => location.reload(), 1500);
                } else if (response.status === 409) {
                    btn.textContent = 'Running';
                    btn.style.background = '#ffc107';
                    btn.style.borderColor = '#ffc107';
                    btn.style.color = '#000';
                    setTimeout(() => {
                        btn.textContent = originalText;
                        btn.style.background = '';
                        btn.style.borderColor = '';
                        btn.style.color = '';
                        btn.disabled = false;
                    }, 2000);
                } else {
                    const result = await response.json().catch(() => ({}));
                    btn.textContent = 'Failed';
                    btn.style.background = '#dc3545';
                    btn.style.borderColor = '#dc3545';
                    alert('Sync failed: ' + (result.error || 'Unknown error'));
                    setTimeout(() => {
                        btn.textContent = originalText;
                        btn.style.background = '';
                        btn.style.borderColor = '';
                        btn.disabled = false;
                    }, 2000);
                }
            } catch (err) {
                btn.textContent = 'Error';
                btn.style.background = '#dc3545';
                btn.style.borderColor = '#dc3545';
                alert('Request failed: ' + err.message);
                setTimeout(() => {
                    btn.textContent = originalText;
                    btn.style.background = '';
                    btn.style.borderColor = '';
                    btn.disabled = false;
                }, 2000);
            }
        }
    </script>
</head>
<body>
    <div class=""header"">
        <h1>Database Sync Dashboard</h1>
        <span class=""refresh-info"">Auto-refresh: 30s | Last updated: " + DateTime.Now.ToString("HH:mm:ss") + @"</span>
    </div>");

    // Display startup delay banner if active
    if (startupDelay?.IsActive == true)
    {
        sb.Append($@"
    <div id=""startup-delay-banner"" class=""startup-delay-banner"">
        <div class=""startup-delay-info"">
            <div class=""startup-delay-title"">Startup Delay Active</div>
            <div class=""startup-delay-text"">Automatic sync is paused. Use table sync buttons or click Start Now to begin all scheduled syncs.</div>
        </div>
        <div id=""countdown"" class=""startup-delay-countdown"">{startupDelay.SecondsRemaining}s</div>
        <button id=""start-now-btn"" class=""start-now-btn"" onclick=""startNow()"">Start Now</button>
    </div>
    <script>startCountdown({startupDelay.SecondsRemaining});</script>");
    }

    // Display configuration errors
    if (validationResult?.Errors.Any() == true)
    {
        sb.Append(@"
    <div class=""config-alert config-alert-error"">
        <div class=""config-alert-title"">⚠ Configuration Errors</div>
        <ul>");
        foreach (var error in validationResult.Errors)
        {
            sb.Append($@"
            <li>{System.Web.HttpUtility.HtmlEncode(error)}</li>");
        }
        sb.Append(@"
        </ul>
    </div>");
    }

    // Display configuration warnings
    if (validationResult?.Warnings.Any() == true)
    {
        sb.Append(@"
    <div class=""config-alert config-alert-warning"">
        <div class=""config-alert-title"">⚠ Configuration Warnings</div>
        <ul>");
        foreach (var warning in validationResult.Warnings)
        {
            sb.Append($@"
            <li>{System.Web.HttpUtility.HtmlEncode(warning)}</li>");
        }
        sb.Append(@"
        </ul>
    </div>");
    }

    sb.Append(@"
    <div class=""profiles"">");

    foreach (var profile in profiles)
    {
        var history = allHistory.GetValueOrDefault(profile.ProfileName, new List<DatabaseSync.Models.SyncHistory>());
        var recentHistory = history.Take(10).ToList();

        // Calculate stats from recent history
        var last24h = history.Where(h => h.SyncEndTime > DateTime.UtcNow.AddHours(-24)).ToList();
        var totalRows = last24h.Sum(h => h.RowsProcessed);
        var totalInserts = last24h.Sum(h => h.RowsInserted);
        var totalUpdates = last24h.Sum(h => h.RowsUpdated);
        var totalDeletes = last24h.Sum(h => h.RowsDeleted);
        var successRate = last24h.Any() ? (last24h.Count(h => h.Success) * 100.0 / last24h.Count) : 0;

        // Determine status - check if profile has ever run (LastRunTime != null)
        var hasNeverRun = !profile.LastRunTime.HasValue;
        var statusClass = profile.IsRunning ? "status-running" :
                         !profile.ScheduleEnabled ? "status-disabled" :
                         hasNeverRun ? "status-pending" :
                         profile.LastRunSuccess ? "status-success" : "status-failed";
        var statusText = profile.IsRunning ? "Running" :
                        !profile.ScheduleEnabled ? "Disabled" :
                        hasNeverRun ? "Pending" :
                        profile.LastRunSuccess ? "Success" : "Failed";

        var runningInfoHtml = "";
        if (profile.IsRunning)
        {
            var startTimeStr = profile.CurrentRunStartTime?.ToLocalTime().ToString("HH:mm:ss") ?? "Unknown";
            var tablesStr = "";
            if (profile.CurrentTableProgress.Any())
            {
                var tableProgress = profile.CurrentTableProgress.Select(p =>
                {
                    var name = System.Web.HttpUtility.HtmlEncode(p.TableName);
                    var elapsed = (DateTime.UtcNow - p.StartTime).TotalSeconds;
                    var elapsedStr = elapsed < 60 ? $"{elapsed:F0}s" : $"{elapsed / 60:F1}m";

                    // Show progress if we have an estimate
                    if (p.EstimatedTotalRows > 0 && p.RowsProcessed > 0)
                    {
                        var pct = Math.Min(100, p.RowsProcessed * 100 / p.EstimatedTotalRows);
                        return $"{name} ({pct}% - {elapsedStr})";
                    }
                    return $"{name} ({elapsedStr})";
                });
                tablesStr = $" | Syncing: {string.Join(", ", tableProgress)}";
            }
            runningInfoHtml = $@"<div class=""current-tables"">Started: {startTimeStr}{tablesStr}</div>";
        }

        var nextRunStr = profile.IsRunning ? "" :
            profile.NextRunTime.HasValue ? $" | Next: {profile.NextRunTime.Value.ToLocalTime():MM-dd HH:mm}" : "";

        var syncBtnDisabled = profile.IsRunning ? "disabled" : "";
        sb.Append($@"
        <div class=""profile-card"">
            <div class=""profile-header"">
                <div class=""profile-header-left"">
                    <span class=""profile-name"">{System.Web.HttpUtility.HtmlEncode(profile.ProfileName)}</span>
                    <span class=""status-badge {statusClass}"">{statusText}</span>
                </div>
                <button class=""profile-sync-btn"" onclick=""triggerProfileSync('{Uri.EscapeDataString(profile.ProfileName)}', this)"" {syncBtnDisabled}>Sync Now</button>
            </div>{runningInfoHtml}
            <div class=""profile-meta"">
                {System.Web.HttpUtility.HtmlEncode(profile.Description ?? "")} |
                {profile.TableCount} tables |
                {System.Web.HttpUtility.HtmlEncode(profile.ScheduleDescription)}{nextRunStr} |
                <a href=""/dashboard/{Uri.EscapeDataString(profile.ProfileName)}"" class=""view-link"">View Details</a>
            </div>
            <div class=""stats"">
                <div class=""stat"">
                    <div class=""stat-value"">{totalRows:N0}</div>
                    <div class=""stat-label"">Rows (24h)</div>
                </div>
                <div class=""stat"">
                    <div class=""stat-value"">{totalInserts:N0}</div>
                    <div class=""stat-label"">Inserts</div>
                </div>
                <div class=""stat"">
                    <div class=""stat-value"">{totalUpdates:N0}</div>
                    <div class=""stat-label"">Updates</div>
                </div>
                <div class=""stat"">
                    <div class=""stat-value"">{totalDeletes:N0}</div>
                    <div class=""stat-label"">Deletes</div>
                </div>
                <div class=""stat"">
                    <div class=""stat-value"">{successRate:F0}%</div>
                    <div class=""stat-label"">Success Rate</div>
                </div>
            </div>");

        // Show failed tables prominently when last run failed (but not if never run)
        if (!profile.LastRunSuccess && !profile.IsRunning && profile.ScheduleEnabled && !hasNeverRun)
        {
            // Get the most recent run's failed tables from history
            var lastRunId = history.FirstOrDefault()?.RunId;
            var failedTables = new List<(string SourceTable, string? ErrorMessage)>();

            if (lastRunId != null)
            {
                failedTables = history
                    .Where(h => h.RunId == lastRunId && !h.Success)
                    .Select(h => (h.SourceTable, h.ErrorMessage))
                    .ToList();
            }

            if (failedTables.Count > 0)
            {
                sb.Append($@"
            <div class=""failed-tables-alert"">
                <strong>Failed Tables ({failedTables.Count}):</strong>
                <ul>");
                foreach (var ft in failedTables)
                {
                    var errorPreview = !string.IsNullOrEmpty(ft.ErrorMessage)
                        ? $" - {(ft.ErrorMessage.Length > 80 ? ft.ErrorMessage.Substring(0, 80) + "..." : ft.ErrorMessage)}"
                        : "";
                    var fullError = !string.IsNullOrEmpty(ft.ErrorMessage)
                        ? System.Web.HttpUtility.HtmlAttributeEncode(ft.ErrorMessage)
                        : "";
                    sb.Append($@"
                    <li title=""{fullError}""><strong>{System.Web.HttpUtility.HtmlEncode(ft.SourceTable)}</strong>{System.Web.HttpUtility.HtmlEncode(errorPreview)}</li>");
                }
                sb.Append(@"
                </ul>
            </div>");
            }
            else
            {
                // If no failures recorded in history, show the last run message
                sb.Append($@"
            <div class=""failed-tables-alert"">
                <strong>Last Run Failed:</strong> {System.Web.HttpUtility.HtmlEncode(profile.LastRunMessage ?? "Unknown error")}
            </div>");
            }
        }

        if (recentHistory.Any())
        {
            sb.Append(@"
            <table>
                <thead>
                    <tr>
                        <th>Time</th>
                        <th>Table</th>
                        <th>Status</th>
                        <th class=""number"">Processed</th>
                        <th class=""number"">Inserted</th>
                        <th class=""number"">Updated</th>
                        <th class=""number"">Deleted</th>
                        <th class=""number"">Recent %</th>
                        <th class=""number"">Duration</th>
                    </tr>
                </thead>
                <tbody>");

            foreach (var h in recentHistory)
            {
                var rowClass = h.Success ? "success" : "failed";
                // Calculate Recent % based on RowsUpdated (not RowsProcessed which includes inserts)
                // This answers: "Of the updates we performed, what % were for recent records?"
                var effectiveRecentRows = Math.Min(h.RecentRowsCount, h.RowsUpdated);
                var recentPct = h.RowsUpdated > 0 ? (effectiveRecentRows * 100.0 / h.RowsUpdated) : 0;
                var recentPctStr = h.RowsUpdated > 0 ? $"{recentPct:F1}%" : "-";
                sb.Append($@"
                    <tr>
                        <td class=""timestamp"">{h.SyncEndTime.ToLocalTime():MM-dd HH:mm}</td>
                        <td>{System.Web.HttpUtility.HtmlEncode(h.SourceTable)}</td>
                        <td class=""{rowClass}"">{(h.Success ? "OK" : "FAIL")}</td>
                        <td class=""number"">{h.RowsProcessed:N0}</td>
                        <td class=""number"">{h.RowsInserted:N0}</td>
                        <td class=""number"">{h.RowsUpdated:N0}</td>
                        <td class=""number"">{h.RowsDeleted:N0}</td>
                        <td class=""number"">{recentPctStr}</td>
                        <td class=""number"">{FormatDuration(h.DurationSeconds)}</td>
                    </tr>");
            }

            sb.Append(@"
                </tbody>
            </table>");
        }
        else
        {
            sb.Append(@"<div class=""no-data"">No sync history available</div>");
        }

        sb.Append(@"
        </div>");
    }

    sb.Append(@"
    </div>
</body>
</html>");

    return sb.ToString();
}

static string GenerateProfileDashboardHtml(
    DatabaseSync.Models.ProfileStatusInfo profile,
    List<DatabaseSync.Models.SyncHistory> history)
{
    var sb = new System.Text.StringBuilder();

    // Group history by table for per-table stats
    var byTable = history
        .GroupBy(h => h.SourceTable)
        .ToDictionary(g => g.Key, g => g.ToList());

    var last24h = history.Where(h => h.SyncEndTime > DateTime.UtcNow.AddHours(-24)).ToList();
    var totalRows = last24h.Sum(h => h.RowsProcessed);
    var totalInserts = last24h.Sum(h => h.RowsInserted);
    var totalUpdates = last24h.Sum(h => h.RowsUpdated);
    var totalDeletes = last24h.Sum(h => h.RowsDeleted);
    var avgDuration = last24h.Any() ? last24h.Average(h => h.DurationSeconds) : 0;

    sb.Append($@"<!DOCTYPE html>
<html lang=""en"">
<head>
    <meta charset=""UTF-8"">
    <meta name=""viewport"" content=""width=device-width, initial-scale=1.0"">
    <title>{System.Web.HttpUtility.HtmlEncode(profile.ProfileName)} - Sync Dashboard</title>
    <meta http-equiv=""refresh"" content=""30"">
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; padding: 20px; }}
        .header {{ margin-bottom: 20px; }}
        .back-link {{ color: #00d9ff; text-decoration: none; margin-bottom: 10px; display: inline-block; }}
        .back-link:hover {{ text-decoration: underline; }}
        h1 {{ color: #00d9ff; margin-bottom: 5px; }}
        .subtitle {{ color: #888; }}
        .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin: 20px 0; }}
        .stat {{ background: #16213e; padding: 20px; border-radius: 8px; text-align: center; }}
        .stat-value {{ font-size: 2em; font-weight: bold; color: #00d9ff; }}
        .stat-label {{ font-size: 0.9em; color: #888; margin-top: 5px; }}
        .section {{ background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 20px; }}
        .section-title {{ color: #00d9ff; margin-bottom: 15px; font-size: 1.2em; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.9em; }}
        th {{ background: #0f3460; padding: 10px; text-align: left; color: #00d9ff; }}
        td {{ padding: 8px 10px; border-bottom: 1px solid #2a2a4a; }}
        tr:hover {{ background: #1f1f3a; }}
        .success {{ color: #28a745; }}
        .failed {{ color: #dc3545; }}
        .number {{ text-align: right; font-family: monospace; }}
        .timestamp {{ color: #888; font-size: 0.85em; }}
        .error-msg {{ color: #dc3545; font-size: 0.85em; max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
        .sync-btn {{ background: #0f3460; color: #00d9ff; border: 1px solid #00d9ff; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 0.85em; }}
        .sync-btn:hover {{ background: #00d9ff; color: #0f3460; }}
        .sync-btn:disabled {{ background: #333; color: #666; border-color: #666; cursor: not-allowed; }}
    </style>
    <script>
        async function triggerSync(profile, table) {{
            const btn = event.target;
            const originalText = btn.textContent;
            btn.disabled = true;
            btn.textContent = 'Syncing...';

            try {{
                const response = await fetch(`/sync/${{profile}}/${{table}}`, {{ method: 'POST' }});
                let result;
                try {{
                    result = await response.json();
                }} catch (jsonErr) {{
                    result = {{ Error: 'Failed to parse response' }};
                }}

                if (response.ok && result.Success) {{
                    btn.textContent = 'Done!';
                    btn.style.background = '#28a745';
                    btn.style.borderColor = '#28a745';
                    setTimeout(() => location.reload(), 1500);
                }} else {{
                    btn.textContent = 'Failed';
                    btn.style.background = '#dc3545';
                    btn.style.borderColor = '#dc3545';
                    const errorMsg = result.Error || result.error || (response.status === 409 ? 'Profile is already running' : 'Unknown error (HTTP ' + response.status + ')');
                    alert('Sync failed: ' + errorMsg);
                    setTimeout(() => {{
                        btn.textContent = originalText;
                        btn.style.background = '';
                        btn.style.borderColor = '';
                        btn.disabled = false;
                    }}, 2000);
                }}
            }} catch (err) {{
                btn.textContent = 'Error';
                btn.style.background = '#dc3545';
                btn.style.borderColor = '#dc3545';
                alert('Request failed: ' + err.message);
                setTimeout(() => {{
                    btn.textContent = originalText;
                    btn.style.background = '';
                    btn.style.borderColor = '';
                    btn.disabled = false;
                }}, 2000);
            }}
        }}
    </script>
</head>
<body>
    <div class=""header"">
        <a href=""/dashboard"" class=""back-link"">← Back to Dashboard</a>
        <h1>{System.Web.HttpUtility.HtmlEncode(profile.ProfileName)}</h1>
        <p class=""subtitle"">{System.Web.HttpUtility.HtmlEncode(profile.Description ?? "")} | {profile.TableCount} tables | {System.Web.HttpUtility.HtmlEncode(profile.ScheduleDescription)}</p>
    </div>

    <div class=""stats"">
        <div class=""stat"">
            <div class=""stat-value"">{totalRows:N0}</div>
            <div class=""stat-label"">Rows Processed (24h)</div>
        </div>
        <div class=""stat"">
            <div class=""stat-value"">{totalInserts:N0}</div>
            <div class=""stat-label"">Inserted (24h)</div>
        </div>
        <div class=""stat"">
            <div class=""stat-value"">{totalUpdates:N0}</div>
            <div class=""stat-label"">Updated (24h)</div>
        </div>
        <div class=""stat"">
            <div class=""stat-value"">{totalDeletes:N0}</div>
            <div class=""stat-label"">Deleted (24h)</div>
        </div>
        <div class=""stat"">
            <div class=""stat-value"">{FormatDuration(avgDuration)}</div>
            <div class=""stat-label"">Avg Duration</div>
        </div>
    </div>

    <div class=""section"">
        <h2 class=""section-title"">Per-Table Summary (Last 24h)</h2>
        <table>
            <thead>
                <tr>
                    <th>Table</th>
                    <th class=""number"">Syncs</th>
                    <th class=""number"">Processed</th>
                    <th class=""number"">Inserted</th>
                    <th class=""number"">Updated</th>
                    <th class=""number"">Deleted</th>
                    <th class=""number"">Recent %</th>
                    <th>Last Sync</th>
                    <th>Status</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>");

    foreach (var table in byTable.OrderByDescending(t => t.Value.Max(h => h.SyncEndTime)))
    {
        var tableHistory = table.Value.Where(h => h.SyncEndTime > DateTime.UtcNow.AddHours(-24)).ToList();
        var lastSync = table.Value.OrderByDescending(h => h.SyncEndTime).FirstOrDefault();
        var statusClass = lastSync?.Success == true ? "success" : "failed";

        // Calculate Recent % - use most recent sync's values for accurate percentage
        // Based on RowsUpdated (not RowsProcessed) to show % of updates that were for recent records
        var mostRecentWithData = tableHistory.OrderByDescending(h => h.SyncEndTime)
            .FirstOrDefault(h => h.RowsUpdated > 0);
        var effectiveRecentRows = mostRecentWithData != null
            ? Math.Min(mostRecentWithData.RecentRowsCount, mostRecentWithData.RowsUpdated) : 0;
        var recentPercent = mostRecentWithData?.RowsUpdated > 0
            ? (effectiveRecentRows * 100.0 / mostRecentWithData.RowsUpdated) : 0;
        var recentPercentStr = mostRecentWithData?.RowsUpdated > 0 ? $"{recentPercent:F1}%" : "-";

        sb.Append($@"
                <tr>
                    <td>{System.Web.HttpUtility.HtmlEncode(table.Key)}</td>
                    <td class=""number"">{tableHistory.Count}</td>
                    <td class=""number"">{tableHistory.Sum(h => h.RowsProcessed):N0}</td>
                    <td class=""number"">{tableHistory.Sum(h => h.RowsInserted):N0}</td>
                    <td class=""number"">{tableHistory.Sum(h => h.RowsUpdated):N0}</td>
                    <td class=""number"">{tableHistory.Sum(h => h.RowsDeleted):N0}</td>
                    <td class=""number"">{recentPercentStr}</td>
                    <td class=""timestamp"">{lastSync?.SyncEndTime.ToLocalTime():MM-dd HH:mm}</td>
                    <td class=""{statusClass}"">{(lastSync?.Success == true ? "OK" : "FAIL")}</td>
                    <td><button class=""sync-btn"" onclick=""triggerSync('{Uri.EscapeDataString(profile.ProfileName)}', '{Uri.EscapeDataString(table.Key)}')"">Sync</button></td>
                </tr>");
    }

    sb.Append(@"
            </tbody>
        </table>
    </div>

    <div class=""section"">
        <h2 class=""section-title"">Recent Sync History</h2>
        <table>
            <thead>
                <tr>
                    <th>Time</th>
                    <th>Table</th>
                    <th>Status</th>
                    <th class=""number"">Processed</th>
                    <th class=""number"">Inserted</th>
                    <th class=""number"">Updated</th>
                    <th class=""number"">Deleted</th>
                    <th class=""number"">Recent %</th>
                    <th class=""number"">Duration</th>
                    <th>Error</th>
                </tr>
            </thead>
            <tbody>");

    foreach (var h in history.Take(50))
    {
        var rowClass = h.Success ? "success" : "failed";
        // Calculate Recent % based on RowsUpdated (not RowsProcessed which includes inserts)
        // This answers: "Of the updates we performed, what % were for recent records?"
        var effectiveRecentRows = Math.Min(h.RecentRowsCount, h.RowsUpdated);
        var recentPercent = h.RowsUpdated > 0 ? (effectiveRecentRows * 100.0 / h.RowsUpdated) : 0;
        var recentPercentStr = h.RowsUpdated > 0 ? $"{recentPercent:F1}%" : "-";
        sb.Append($@"
                <tr>
                    <td class=""timestamp"">{h.SyncEndTime.ToLocalTime():MM-dd HH:mm:ss}</td>
                    <td>{System.Web.HttpUtility.HtmlEncode(h.SourceTable)}</td>
                    <td class=""{rowClass}"">{(h.Success ? "OK" : "FAIL")}</td>
                    <td class=""number"">{h.RowsProcessed:N0}</td>
                    <td class=""number"">{h.RowsInserted:N0}</td>
                    <td class=""number"">{h.RowsUpdated:N0}</td>
                    <td class=""number"">{h.RowsDeleted:N0}</td>
                    <td class=""number"">{recentPercentStr}</td>
                    <td class=""number"">{FormatDuration(h.DurationSeconds)}</td>
                    <td class=""error-msg"" title=""{System.Web.HttpUtility.HtmlAttributeEncode(h.ErrorMessage ?? "")}"">{System.Web.HttpUtility.HtmlEncode(h.ErrorMessage ?? "")}</td>
                </tr>");
    }

    sb.Append(@"
            </tbody>
        </table>
    </div>
</body>
</html>");

    return sb.ToString();
}

static string FormatDuration(double seconds)
{
    var fractionalSeconds = seconds - Math.Floor(seconds);
    var centiseconds = (int)(fractionalSeconds * 100);

    if (seconds >= 3600)
    {
        var hours = (int)(seconds / 3600);
        var mins = (int)((seconds % 3600) / 60);
        var secs = (int)(seconds % 60);
        return $"{hours}h {mins:D2}m {secs:D2}.{centiseconds:D2}s";
    }
    else if (seconds >= 60)
    {
        var mins = (int)(seconds / 60);
        var secs = (int)(seconds % 60);
        return $"{mins}m {secs:D2}.{centiseconds:D2}s";
    }
    else
    {
        return $"{seconds:F2}s";
    }
}
