using System.Collections.Concurrent;
using DatabaseSync.Abstractions;
using DatabaseSync.Configuration;
using DatabaseSync.Models;
using DatabaseSync.PostgreSql;
using DatabaseSync.SqlServer;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DatabaseSync.Services;

/// <summary>
/// Background service that schedules and runs sync profiles based on their configured schedules
/// </summary>
public class ProfileScheduler : BackgroundService
{
    private readonly IServiceProvider _services;
    private readonly SyncServiceConfig _config;
    private readonly ILogger<ProfileScheduler> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<string, ProfileState> _profileStates = new();
    private readonly LoadMonitor _loadMonitor;

    // Startup delay tracking
    private DateTime? _startupDelayEndTime;
    private bool _startupDelayCompleted;

    public ProfileScheduler(
        IServiceProvider services,
        SyncServiceConfig config,
        ILogger<ProfileScheduler> logger,
        ILoggerFactory loggerFactory)
    {
        _services = services;
        _config = config;
        _logger = logger;
        _loggerFactory = loggerFactory;
        _loadMonitor = new LoadMonitor(loggerFactory.CreateLogger<LoadMonitor>());
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Profile Scheduler starting with {Count} profiles (mode: {Mode})",
            _config.Profiles.Count, _config.ProfileExecutionMode);

        // Log blackout window configuration
        if (_config.BlackoutWindow?.Enabled == true)
        {
            _logger.LogInformation(
                "Blackout window enabled: {Window} (no new syncs will start during this time)",
                _config.BlackoutWindow.GetDescription());
        }

        // Log load throttling configuration
        if (_config.LoadThrottling?.Enabled == true)
        {
            _logger.LogInformation(
                "Load throttling enabled: pause sync when source CPU > {MaxCpu}% (check: {Timing})",
                _config.LoadThrottling.MaxCpuPercent,
                _config.LoadThrottling.CheckTiming);
        }

        // Check if any profiles have RunImmediatelyOnStart and we have a startup delay
        var hasImmediateProfiles = _config.Profiles.Any(p =>
            p.Schedule.Enabled && p.Schedule.RunImmediatelyOnStart);

        if (_config.StartupDelaySeconds > 0 && hasImmediateProfiles)
        {
            _startupDelayEndTime = DateTime.UtcNow.AddSeconds(_config.StartupDelaySeconds);
            _startupDelayCompleted = false;
            _logger.LogInformation(
                "Startup delay enabled: {Seconds} seconds until auto-sync begins. " +
                "Use dashboard or API to start earlier or trigger specific tables.",
                _config.StartupDelaySeconds);
        }
        else
        {
            _startupDelayCompleted = true;
        }

        // Initialize profile states - maintain order for sequential mode
        var profileIndex = 0;
        foreach (var profile in _config.Profiles)
        {
            DateTime? nextRun = null;

            if (profile.Schedule.Enabled)
            {
                if (profile.Schedule.RunImmediatelyOnStart)
                {
                    // If startup delay is active, set next run to after the delay
                    nextRun = _startupDelayEndTime ?? DateTime.UtcNow;
                }
                else
                {
                    nextRun = profile.Schedule.GetNextRunTime(DateTime.UtcNow);
                }
            }

            _profileStates[profile.ProfileName] = new ProfileState
            {
                Profile = profile,
                NextRunTime = nextRun,
                IsRunning = false,
                ProfileIndex = profileIndex++
            };

            var scheduleStatus = profile.Schedule.Enabled
                ? $"Next run: {nextRun?.ToLocalTime():yyyy-MM-dd HH:mm:ss}"
                : "Disabled (manual trigger only)";

            _logger.LogInformation(
                "Profile '{Name}': {Tables} tables, {Schedule}. {Status}",
                profile.ProfileName,
                profile.Tables.Count,
                profile.Schedule.GetDescription(),
                scheduleStatus);
        }

        // Main scheduler loop - check every 10 seconds
        var wasInBlackout = false;
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.UtcNow;
            var localNow = now.ToLocalTime();

            // Check if startup delay has expired
            if (!_startupDelayCompleted && _startupDelayEndTime.HasValue && now >= _startupDelayEndTime.Value)
            {
                _startupDelayCompleted = true;
                _logger.LogInformation("Startup delay completed - automatic syncs will now begin");
            }

            // Check blackout window
            var isInBlackout = _config.BlackoutWindow?.IsInBlackoutWindow(localNow) == true;

            // Log state transitions
            if (isInBlackout && !wasInBlackout)
            {
                _logger.LogInformation(
                    "Entering blackout window ({Window}) - no new syncs will start",
                    _config.BlackoutWindow!.GetDescription());
            }
            else if (!isInBlackout && wasInBlackout)
            {
                _logger.LogInformation(
                    "Exiting blackout window - syncs can now start");
            }
            wasInBlackout = isInBlackout;

            // Skip scheduling new syncs during blackout window
            if (isInBlackout)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                continue;
            }

            if (_config.ProfileExecutionMode == ProfileExecutionMode.Sequential)
            {
                await RunSequentialAsync(now, stoppingToken);
            }
            else
            {
                RunParallel(now, stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
        }
    }

    private void RunParallel(DateTime now, CancellationToken stoppingToken)
    {
        foreach (var state in _profileStates.Values)
        {
            if (state.Profile.Schedule.Enabled &&
                state.NextRunTime.HasValue &&
                state.NextRunTime.Value <= now &&
                !state.IsRunning)
            {
                // Fire and forget - don't await so we can check other profiles
                _ = RunProfileAsync(state, stoppingToken);
            }
        }
    }

    private async Task RunSequentialAsync(DateTime now, CancellationToken stoppingToken)
    {
        // Get profiles in order, check if any are due to run
        var orderedProfiles = _profileStates.Values
            .OrderBy(s => s.ProfileIndex)
            .ToList();

        // Check if any profile is currently running
        if (orderedProfiles.Any(s => s.IsRunning))
        {
            return; // Wait for current profile to complete
        }

        // Find profiles that are due to run
        var profilesDueToRun = orderedProfiles
            .Where(s => s.Profile.Schedule.Enabled &&
                       s.NextRunTime.HasValue &&
                       s.NextRunTime.Value <= now)
            .ToList();

        if (!profilesDueToRun.Any())
        {
            return;
        }

        // Run profiles sequentially in order
        foreach (var state in profilesDueToRun.OrderBy(s => s.ProfileIndex))
        {
            if (stoppingToken.IsCancellationRequested)
                break;

            await RunProfileAsync(state, stoppingToken);
        }
    }

    private async Task RunProfileAsync(ProfileState state, CancellationToken stoppingToken)
    {
        if (state.IsRunning)
            return;

        var profile = state.Profile;
        var throttling = _config.LoadThrottling;

        // Check load throttling before starting profile (BeforeProfile or Both)
        if (throttling?.Enabled == true &&
            (throttling.CheckTiming == LoadCheckTiming.BeforeProfile ||
             throttling.CheckTiming == LoadCheckTiming.Both))
        {
            await _loadMonitor.WaitForLowLoadAsync(
                profile.SourceConnection.ConnectionString,
                profile.SourceConnection.DatabaseType,
                throttling.MaxCpuPercent,
                throttling.CheckIntervalSeconds,
                throttling.MaxWaitMinutes,
                stoppingToken);
        }

        state.IsRunning = true;
        state.CurrentRunStartTime = DateTime.UtcNow;
        state.LastRunTime = state.CurrentRunStartTime;

        // Check if today's schedule forces a full refresh
        var forceFullRefresh = profile.Schedule.ShouldForceFullRefresh(DateTime.Now.DayOfWeek);

        try
        {
            if (forceFullRefresh)
            {
                _logger.LogInformation("═══ Starting scheduled FULL REFRESH sync for profile '{Name}' (day schedule) ═══", profile.ProfileName);
            }
            else
            {
                _logger.LogInformation("═══ Starting scheduled sync for profile '{Name}' ═══", profile.ProfileName);
            }

            // Get estimated row counts from sync history for progress tracking
            var estimatedRows = await GetEstimatedRowsAsync(profile);
            var orchestrator = CreateOrchestrator(profile, state, estimatedRows);

            await orchestrator.InitializeAsync();
            var result = await orchestrator.SyncAllAsync(forceFullRefresh,
                                                          loadMonitor: throttling?.Enabled == true ? _loadMonitor : null,
                                                          loadThrottling: throttling);

            state.CurrentTables.Clear();
            state.LastRunSuccess = result.FailureCount == 0;
            state.LastRunMessage = $"{result.SuccessCount}/{result.TableResults.Count} tables, " +
                                   $"{result.TotalRowsProcessed:N0} rows in {FormatDuration(result.Duration)}";

            _logger.LogInformation(
                "═══ Profile '{Name}' complete: {Message} ═══",
                profile.ProfileName,
                state.LastRunMessage);
        }
        catch (Exception ex)
        {
            state.LastRunSuccess = false;
            state.LastRunMessage = ex.Message;
            
            _logger.LogError(ex, "Profile '{Name}' failed", profile.ProfileName);
        }
        finally
        {
            state.IsRunning = false;
            state.CurrentRunStartTime = null;

            if (profile.Schedule.Enabled)
            {
                state.NextRunTime = profile.Schedule.GetNextRunTime(DateTime.UtcNow);
                _logger.LogDebug(
                    "Profile '{Name}' next scheduled run: {NextRun}",
                    profile.ProfileName,
                    state.NextRunTime?.ToLocalTime());
            }
        }
    }

    private SyncOrchestrator CreateOrchestrator(SyncProfile profile, ProfileState? state = null, Dictionary<string, long>? estimatedRows = null)
    {
        // Create callback to track active tables if state is provided
        Action<string, bool>? tableStatusCallback = null;
        Action<string, long, string>? progressCallback = null;

        if (state != null)
        {
            tableStatusCallback = (tableName, isStarting) =>
            {
                if (isStarting)
                {
                    var estimated = estimatedRows?.GetValueOrDefault(tableName, 0) ?? 0;
                    state.CurrentTables.TryAdd(tableName, new TableSyncProgress
                    {
                        TableName = tableName,
                        StartTime = DateTime.UtcNow,
                        EstimatedTotalRows = estimated,
                        Phase = "Loading"
                    });
                }
                else
                {
                    state.CurrentTables.TryRemove(tableName, out _);
                }
            };

            progressCallback = (tableName, rowsProcessed, phase) =>
            {
                if (state.CurrentTables.TryGetValue(tableName, out var progress))
                {
                    progress.RowsProcessed = rowsProcessed;
                    progress.Phase = phase;
                }
            };
        }

        return new SyncOrchestrator(
            profile,
            _loggerFactory.CreateLogger<SyncOrchestrator>(),
            _loggerFactory.CreateLogger<SqlServerSchemaAnalyzer>(),
            _loggerFactory.CreateLogger<PostgreSqlSchemaAnalyzer>(),
            _loggerFactory.CreateLogger<SqlServerTargetAnalyzer>(),
            _loggerFactory.CreateLogger<BulkDataCopier>(),
            _loggerFactory.CreateLogger<SqlServerBulkDataCopier>(),
            _loggerFactory.CreateLogger<PostgreSqlBulkDataCopier>(),
            _loggerFactory.CreateLogger<PostgreSqlToSqlServerBulkCopier>(),
            _loggerFactory.CreateLogger<PostgreSqlSyncHistoryRepository>(),
            _loggerFactory.CreateLogger<SqlServerSyncHistoryRepository>(),
            tableStatusCallback,
            progressCallback);
    }

    /// <summary>
    /// Manually trigger a profile sync
    /// </summary>
    public async Task<SyncRunResult> TriggerProfileAsync(string profileName, bool fullRefresh = false)
    {
        var profile = _config.Profiles.FirstOrDefault(p => 
            p.ProfileName.Equals(profileName, StringComparison.OrdinalIgnoreCase));

        if (profile == null)
        {
            throw new ArgumentException($"Profile '{profileName}' not found");
        }

        if (_profileStates.TryGetValue(profile.ProfileName, out var state) && state.IsRunning)
        {
            throw new InvalidOperationException($"Profile '{profileName}' is already running");
        }

        _logger.LogInformation("Manual trigger for profile '{Name}' (fullRefresh: {FullRefresh})",
            profileName, fullRefresh);

        // Get or create state for tracking
        if (!_profileStates.TryGetValue(profile.ProfileName, out state))
        {
            state = new ProfileState { Profile = profile };
            _profileStates[profile.ProfileName] = state;
        }

        state.IsRunning = true;
        state.CurrentRunStartTime = DateTime.UtcNow;
        var orchestrator = CreateOrchestrator(profile, state);

        try
        {
            await orchestrator.InitializeAsync();
            var result = await orchestrator.SyncAllAsync(fullRefresh);

            state.CurrentTables.Clear();
            state.LastRunTime = state.CurrentRunStartTime;
            state.LastRunSuccess = result.FailureCount == 0;
            state.LastRunMessage = $"{result.SuccessCount}/{result.TableResults.Count} tables, " +
                                   $"{result.TotalRowsProcessed:N0} rows";

            return result;
        }
        finally
        {
            state.IsRunning = false;
            state.CurrentRunStartTime = null;
            state.CurrentTables.Clear();
        }
    }

    /// <summary>
    /// Manually trigger sync for a specific table in a profile
    /// </summary>
    public async Task<SyncResult> TriggerTableAsync(string profileName, string tableName, bool fullRefresh = false)
    {
        var profile = _config.Profiles.FirstOrDefault(p =>
            p.ProfileName.Equals(profileName, StringComparison.OrdinalIgnoreCase));

        if (profile == null)
        {
            throw new ArgumentException($"Profile '{profileName}' not found");
        }

        var tableConfig = profile.Tables.FirstOrDefault(t =>
            t.SourceTable.Equals(tableName, StringComparison.OrdinalIgnoreCase));

        if (tableConfig == null)
        {
            throw new ArgumentException($"Table '{tableName}' not found in profile '{profileName}'");
        }

        if (_profileStates.TryGetValue(profile.ProfileName, out var state) && state.IsRunning)
        {
            throw new InvalidOperationException($"Profile '{profileName}' is already running");
        }

        _logger.LogInformation("Manual trigger for table '{Table}' in profile '{Profile}' (fullRefresh: {FullRefresh})",
            tableName, profileName, fullRefresh);

        var orchestrator = CreateOrchestrator(profile);

        await orchestrator.InitializeAsync();
        return await orchestrator.SyncTableAsync(tableConfig, fullRefresh);
    }

    /// <summary>
    /// Get current status of all profiles
    /// </summary>
    public IEnumerable<ProfileStatusInfo> GetAllProfileStatus()
    {
        return _profileStates.Values.Select(s => new ProfileStatusInfo
        {
            ProfileName = s.Profile.ProfileName,
            Description = s.Profile.Description,
            IsRunning = s.IsRunning,
            ScheduleEnabled = s.Profile.Schedule.Enabled,
            NextRunTime = s.NextRunTime,
            LastRunTime = s.LastRunTime,
            LastRunSuccess = s.LastRunSuccess,
            LastRunMessage = s.LastRunMessage,
            IntervalMinutes = s.Profile.Schedule.IntervalMinutes,
            ScheduleDescription = s.Profile.Schedule.GetDescription(_config.BlackoutWindow),
            TableCount = s.Profile.Tables.Count,
            CurrentTableProgress = s.CurrentTables.Values.Select(p => new TableSyncProgressInfo
            {
                TableName = p.TableName,
                RowsProcessed = p.RowsProcessed,
                EstimatedTotalRows = p.EstimatedTotalRows,
                StartTime = p.StartTime,
                Phase = p.Phase
            }).ToList(),
            CurrentRunStartTime = s.CurrentRunStartTime
        });
    }

    /// <summary>
    /// Get status of a specific profile
    /// </summary>
    public ProfileStatusInfo? GetProfileStatus(string profileName)
    {
        if (!_profileStates.TryGetValue(profileName, out var state))
        {
            // Try case-insensitive match
            var key = _profileStates.Keys.FirstOrDefault(k =>
                k.Equals(profileName, StringComparison.OrdinalIgnoreCase));

            if (key == null || !_profileStates.TryGetValue(key, out state))
                return null;
        }

        return new ProfileStatusInfo
        {
            ProfileName = state.Profile.ProfileName,
            Description = state.Profile.Description,
            IsRunning = state.IsRunning,
            ScheduleEnabled = state.Profile.Schedule.Enabled,
            NextRunTime = state.NextRunTime,
            LastRunTime = state.LastRunTime,
            LastRunSuccess = state.LastRunSuccess,
            LastRunMessage = state.LastRunMessage,
            IntervalMinutes = state.Profile.Schedule.IntervalMinutes,
            ScheduleDescription = state.Profile.Schedule.GetDescription(_config.BlackoutWindow),
            TableCount = state.Profile.Tables.Count,
            CurrentTableProgress = state.CurrentTables.Values.Select(p => new TableSyncProgressInfo
            {
                TableName = p.TableName,
                RowsProcessed = p.RowsProcessed,
                EstimatedTotalRows = p.EstimatedTotalRows,
                StartTime = p.StartTime,
                Phase = p.Phase
            }).ToList(),
            CurrentRunStartTime = state.CurrentRunStartTime
        };
    }

    /// <summary>
    /// Get list of profile names
    /// </summary>
    public IEnumerable<string> GetProfileNames()
    {
        return _profileStates.Keys;
    }

    /// <summary>
    /// Get recent sync history for a profile
    /// </summary>
    public async Task<List<SyncHistory>> GetSyncHistoryAsync(string profileName, int limit = 50)
    {
        var profile = _config.Profiles.FirstOrDefault(p =>
            p.ProfileName.Equals(profileName, StringComparison.OrdinalIgnoreCase));

        if (profile == null)
        {
            return new List<SyncHistory>();
        }

        // Get the appropriate history repository based on target database type
        ISyncHistoryRepository historyRepo;
        if (profile.TargetConnection.DatabaseType == Enums.DatabaseType.PostgreSql)
        {
            historyRepo = new PostgreSqlSyncHistoryRepository(
                profile.TargetConnection.ConnectionString,
                _loggerFactory.CreateLogger<PostgreSqlSyncHistoryRepository>());
        }
        else
        {
            historyRepo = new SqlServerSyncHistoryRepository(
                profile.TargetConnection.ConnectionString,
                _loggerFactory.CreateLogger<SqlServerSyncHistoryRepository>());
        }

        // Ensure schema is up to date (adds any missing columns)
        await historyRepo.InitializeAsync();

        return await historyRepo.GetRecentHistoryAsync(profile.EffectiveProfileId, limit);
    }

    /// <summary>
    /// Get estimated row counts from sync history for progress tracking
    /// Uses TotalSourceRows from the most recent successful sync for each table
    /// </summary>
    private async Task<Dictionary<string, long>> GetEstimatedRowsAsync(SyncProfile profile)
    {
        var result = new Dictionary<string, long>();

        try
        {
            // Get recent history for the profile (limit to recent entries)
            var history = await GetSyncHistoryAsync(profile.ProfileName, 100);

            // Group by table and take the most recent successful sync's TotalSourceRows
            foreach (var tableGroup in history.Where(h => h.Success && h.TotalSourceRows > 0)
                                              .GroupBy(h => h.SourceTable))
            {
                var mostRecent = tableGroup.OrderByDescending(h => h.SyncEndTime).First();
                result[mostRecent.SourceTable] = mostRecent.TotalSourceRows;
            }

            if (result.Count > 0)
            {
                _logger.LogDebug(
                    "Loaded estimated row counts from history for {Count} tables",
                    result.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load estimated row counts from history");
        }

        return result;
    }

    /// <summary>
    /// Get the profile configuration
    /// </summary>
    public SyncProfile? GetProfile(string profileName)
    {
        return _config.Profiles.FirstOrDefault(p =>
            p.ProfileName.Equals(profileName, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Get startup delay status
    /// </summary>
    public StartupDelayStatus GetStartupDelayStatus()
    {
        if (_startupDelayCompleted || !_startupDelayEndTime.HasValue)
        {
            return new StartupDelayStatus
            {
                IsActive = false,
                SecondsRemaining = 0,
                EndTime = null
            };
        }

        var remaining = (_startupDelayEndTime.Value - DateTime.UtcNow).TotalSeconds;
        return new StartupDelayStatus
        {
            IsActive = remaining > 0,
            SecondsRemaining = Math.Max(0, (int)remaining),
            EndTime = _startupDelayEndTime.Value
        };
    }

    /// <summary>
    /// Cancel the startup delay and allow syncs to start immediately
    /// </summary>
    public void CancelStartupDelay()
    {
        if (_startupDelayCompleted)
            return;

        _logger.LogInformation("Startup delay cancelled via API - syncs will now begin");
        _startupDelayCompleted = true;
        _startupDelayEndTime = DateTime.UtcNow;

        // Update all profiles that were waiting for the delay to end
        foreach (var state in _profileStates.Values)
        {
            if (state.Profile.Schedule.Enabled &&
                state.Profile.Schedule.RunImmediatelyOnStart &&
                state.NextRunTime.HasValue &&
                state.NextRunTime.Value > DateTime.UtcNow)
            {
                state.NextRunTime = DateTime.UtcNow;
            }
        }
    }

    /// <summary>
    /// Format a TimeSpan as h:mm:ss.ff (e.g., 1:23:45.67)
    /// For durations under 1 hour: mm:ss.ff (e.g., 10:58.37)
    /// For durations under 1 minute: ss.ff (e.g., 45.23s)
    /// </summary>
    private static string FormatDuration(TimeSpan duration)
    {
        var totalSeconds = duration.TotalSeconds;
        var fractionalSeconds = totalSeconds - Math.Floor(totalSeconds);
        var centiseconds = (int)(fractionalSeconds * 100);

        if (duration.TotalHours >= 1)
        {
            return $"{(int)duration.TotalHours}h {duration.Minutes:D2}m {duration.Seconds:D2}.{centiseconds:D2}s";
        }
        else if (duration.TotalMinutes >= 1)
        {
            return $"{(int)duration.TotalMinutes}m {duration.Seconds:D2}.{centiseconds:D2}s";
        }
        else
        {
            return $"{duration.TotalSeconds:F2}s";
        }
    }
}

/// <summary>
/// Internal state tracking for a profile
/// </summary>
internal class ProfileState
{
    public SyncProfile Profile { get; set; } = null!;
    public DateTime? NextRunTime { get; set; }
    public DateTime? LastRunTime { get; set; }
    public bool IsRunning { get; set; }
    public bool LastRunSuccess { get; set; }
    public string? LastRunMessage { get; set; }
    public int ProfileIndex { get; set; }

    /// <summary>
    /// Thread-safe dictionary of tables currently being synced with progress
    /// </summary>
    public ConcurrentDictionary<string, TableSyncProgress> CurrentTables { get; } = new();

    /// <summary>
    /// When the current sync run started
    /// </summary>
    public DateTime? CurrentRunStartTime { get; set; }
}

/// <summary>
/// Progress tracking for a table sync operation
/// </summary>
public class TableSyncProgress
{
    public string TableName { get; set; } = string.Empty;
    public long RowsProcessed { get; set; }
    public long EstimatedTotalRows { get; set; }
    public DateTime StartTime { get; set; }
    public string Phase { get; set; } = "Loading";
}

/// <summary>
/// Status of the startup delay
/// </summary>
public class StartupDelayStatus
{
    /// <summary>
    /// Whether the startup delay is currently active
    /// </summary>
    public bool IsActive { get; set; }

    /// <summary>
    /// Seconds remaining until syncs begin
    /// </summary>
    public int SecondsRemaining { get; set; }

    /// <summary>
    /// When the delay will end (UTC)
    /// </summary>
    public DateTime? EndTime { get; set; }
}
