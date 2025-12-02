using DatabaseSync.Enums;

namespace DatabaseSync.Configuration;

/// <summary>
/// Root configuration for the sync service
/// </summary>
public class SyncServiceConfig
{
    /// <summary>
    /// HTTP port for the API and health checks
    /// </summary>
    public int HttpPort { get; set; } = 5123;

    /// <summary>
    /// Enable health check endpoints
    /// </summary>
    public bool EnableHealthChecks { get; set; } = true;

    /// <summary>
    /// Minimum log level (Verbose, Debug, Information, Warning, Error, Fatal)
    /// </summary>
    public string LogLevel { get; set; } = "Information";

    /// <summary>
    /// Path for log files. Can be absolute or relative to the application directory.
    /// Use forward slashes for cross-platform compatibility.
    /// Default: "logs" (creates logs folder in application directory)
    /// Example: "D:/Logs/DatabaseSync" or "/var/log/databasesync"
    /// </summary>
    public string LogPath { get; set; } = "logs";

    /// <summary>
    /// Profile execution mode: Parallel or Sequential
    /// Parallel: All profiles run independently based on their schedules
    /// Sequential: Profiles run in order (by list position), waiting for previous to complete
    /// </summary>
    public ProfileExecutionMode ProfileExecutionMode { get; set; } = ProfileExecutionMode.Parallel;

    /// <summary>
    /// Global blackout window - no new syncs will start during this time period.
    /// Useful for database maintenance windows (backups, etc.)
    /// Running syncs will be allowed to complete.
    /// </summary>
    public BlackoutWindowConfig? BlackoutWindow { get; set; }

    /// <summary>
    /// Load throttling configuration - pause sync when source server is under heavy load
    /// </summary>
    public LoadThrottlingConfig? LoadThrottling { get; set; }

    /// <summary>
    /// Sync profiles - each profile defines a source/target pair with its own schedule
    /// </summary>
    public List<SyncProfile> Profiles { get; set; } = new();

    /// <summary>
    /// Startup delay in seconds before RunImmediatelyOnStart syncs begin.
    /// This provides a window to access the dashboard and trigger manual table syncs
    /// before automatic syncs start. Set to 0 for immediate start (default).
    /// The delay can be cancelled via API or dashboard.
    /// </summary>
    public int StartupDelaySeconds { get; set; } = 0;
}

/// <summary>
/// Configuration for load-based throttling
/// </summary>
public class LoadThrottlingConfig
{
    /// <summary>
    /// Enable or disable load throttling
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Maximum CPU percentage before pausing sync (default: 60%)
    /// </summary>
    public int MaxCpuPercent { get; set; } = 60;

    /// <summary>
    /// Maximum active queries before pausing sync (used for PostgreSQL sources)
    /// </summary>
    public int MaxActiveQueries { get; set; } = 50;

    /// <summary>
    /// How often to check server load when waiting (seconds)
    /// </summary>
    public int CheckIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// Maximum time to wait for load to decrease before proceeding anyway (minutes)
    /// </summary>
    public int MaxWaitMinutes { get; set; } = 30;

    /// <summary>
    /// When to check load: BeforeProfile, BeforeTable, or Both
    /// </summary>
    public LoadCheckTiming CheckTiming { get; set; } = LoadCheckTiming.BeforeTable;
}

/// <summary>
/// When to check server load
/// </summary>
public enum LoadCheckTiming
{
    /// <summary>
    /// Check once before starting each profile sync
    /// </summary>
    BeforeProfile,

    /// <summary>
    /// Check before syncing each table (more responsive but more overhead)
    /// </summary>
    BeforeTable,

    /// <summary>
    /// Check both before profile and before each table
    /// </summary>
    Both
}

/// <summary>
/// Configuration for a global blackout window where no new syncs will start
/// </summary>
public class BlackoutWindowConfig
{
    /// <summary>
    /// Enable or disable the blackout window
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Start time of the blackout window (24-hour format: "HH:mm")
    /// Example: "23:00" for 11 PM
    /// </summary>
    public string StartTime { get; set; } = "23:00";

    /// <summary>
    /// End time of the blackout window (24-hour format: "HH:mm")
    /// Example: "05:00" for 5 AM
    /// Note: If EndTime is before StartTime, the window spans midnight
    /// </summary>
    public string EndTime { get; set; } = "05:00";

    /// <summary>
    /// Check if the current time is within the blackout window
    /// </summary>
    public bool IsInBlackoutWindow(DateTime currentTime)
    {
        if (!Enabled)
            return false;

        var startParts = StartTime.Split(':');
        var endParts = EndTime.Split(':');

        var startHour = int.Parse(startParts[0]);
        var startMinute = startParts.Length > 1 ? int.Parse(startParts[1]) : 0;
        var endHour = int.Parse(endParts[0]);
        var endMinute = endParts.Length > 1 ? int.Parse(endParts[1]) : 0;

        var currentMinutes = currentTime.Hour * 60 + currentTime.Minute;
        var startMinutes = startHour * 60 + startMinute;
        var endMinutes = endHour * 60 + endMinute;

        // Handle overnight windows (e.g., 23:00 to 05:00)
        if (startMinutes > endMinutes)
        {
            // Window spans midnight
            return currentMinutes >= startMinutes || currentMinutes < endMinutes;
        }
        else
        {
            // Window is within same day
            return currentMinutes >= startMinutes && currentMinutes < endMinutes;
        }
    }

    /// <summary>
    /// Get a human-readable description of the blackout window
    /// </summary>
    public string GetDescription()
    {
        if (!Enabled)
            return "Disabled";

        return $"{StartTime} to {EndTime}";
    }
}

/// <summary>
/// Controls how multiple profiles are executed
/// </summary>
public enum ProfileExecutionMode
{
    /// <summary>
    /// Profiles run independently based on their own schedules (default)
    /// </summary>
    Parallel,

    /// <summary>
    /// Profiles run sequentially in order - each waits for the previous to complete
    /// </summary>
    Sequential
}

/// <summary>
/// A sync profile defines a source database, target database, schedule, and tables to sync
/// </summary>
public class SyncProfile
{
    /// <summary>
    /// Unique stable identifier for this profile. Auto-generated if not specified.
    /// This ID is used in sync history and survives profile name changes.
    /// </summary>
    public string? ProfileId { get; set; }

    /// <summary>
    /// Unique name for this profile (e.g., "Production", "Reporting", "Archive")
    /// </summary>
    public string ProfileName { get; set; } = "Default";

    /// <summary>
    /// Gets the effective profile ID. If ProfileId is not specified,
    /// returns the ProfileName (for backward compatibility with existing history).
    /// </summary>
    public string EffectiveProfileId => !string.IsNullOrEmpty(ProfileId) ? ProfileId : ProfileName;

    /// <summary>
    /// Optional description of what this profile does
    /// </summary>
    public string? Description { get; set; }
    
    /// <summary>
    /// Source database connection configuration
    /// </summary>
    public ConnectionConfig SourceConnection { get; set; } = new();
    
    /// <summary>
    /// Target database connection configuration
    /// </summary>
    public ConnectionConfig TargetConnection { get; set; } = new();
    
    /// <summary>
    /// Schedule configuration for automated syncs
    /// </summary>
    public ScheduleConfig Schedule { get; set; } = new();
    
    /// <summary>
    /// Profile-specific options
    /// </summary>
    public ProfileOptions Options { get; set; } = new();
    
    /// <summary>
    /// Tables to sync within this profile
    /// </summary>
    public List<TableConfig> Tables { get; set; } = new();
}

/// <summary>
/// Database connection configuration
/// </summary>
public class ConnectionConfig
{
    /// <summary>
    /// Database type: SqlServer or PostgreSql
    /// </summary>
    public string Type { get; set; } = "SqlServer";
    
    /// <summary>
    /// Connection string for the database
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;
    
    /// <summary>
    /// Parsed database type enum
    /// </summary>
    public DatabaseType DatabaseType => Type.ToLower() switch
    {
        "sqlserver" or "mssql" => DatabaseType.SqlServer,
        "postgresql" or "postgres" or "pgsql" => DatabaseType.PostgreSql,
        _ => throw new ArgumentException($"Unknown database type: {Type}")
    };
}

/// <summary>
/// Schedule configuration - similar to SQL Server Agent job scheduling.
/// Supports both legacy single-schedule format and new day-specific schedules.
/// </summary>
public class ScheduleConfig
{
    /// <summary>
    /// Time of day for first run (24-hour format: "HH:mm")
    /// Example: "00:00" for midnight, "14:30" for 2:30 PM
    /// Used when Schedules array is not specified or empty.
    /// </summary>
    public string StartTime { get; set; } = "00:00";

    /// <summary>
    /// Minutes between sync runs
    /// Examples: 15 (every 15 min), 60 (hourly), 1440 (daily)
    /// Used when Schedules array is not specified or empty.
    /// </summary>
    public int IntervalMinutes { get; set; } = 60;

    /// <summary>
    /// Run sync immediately when service starts (before first scheduled time)
    /// </summary>
    public bool RunImmediatelyOnStart { get; set; } = true;

    /// <summary>
    /// Enable or disable this profile's automatic schedule
    /// When false, profile can still be triggered manually via API
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Optional: Only run on specific days of week (legacy format)
    /// 0 = Sunday, 1 = Monday, ..., 6 = Saturday
    /// Null or empty means run every day
    /// Example: [1,2,3,4,5] for weekdays only
    /// Note: If Schedules array is specified, this is ignored.
    /// </summary>
    public List<int>? DaysOfWeek { get; set; }

    /// <summary>
    /// Day-specific schedules - allows different intervals and modes for different days.
    /// When specified and not empty, takes precedence over legacy DaysOfWeek, IntervalMinutes, StartTime.
    /// Example: Run Incremental every 2 hours on weekdays, FullRefresh daily on weekends.
    /// </summary>
    public List<DaySchedule>? Schedules { get; set; }

    /// <summary>
    /// Check if this config uses day-specific schedules
    /// </summary>
    public bool HasDaySchedules => Schedules != null && Schedules.Any();

    /// <summary>
    /// Get the schedule that applies for a given day
    /// </summary>
    public DaySchedule? GetScheduleForDay(DayOfWeek day)
    {
        if (!HasDaySchedules)
            return null;

        return Schedules!.FirstOrDefault(s => s.AppliesToDay(day));
    }

    /// <summary>
    /// Get whether this schedule should force a full refresh for the given day
    /// </summary>
    public bool ShouldForceFullRefresh(DayOfWeek day)
    {
        var daySchedule = GetScheduleForDay(day);
        return daySchedule?.ForceFullRefresh ?? false;
    }

    /// <summary>
    /// Calculate the next run time from a given time
    /// </summary>
    public DateTime GetNextRunTime(DateTime fromTime)
    {
        if (HasDaySchedules)
        {
            return GetNextRunTimeFromDaySchedules(fromTime);
        }

        return GetNextRunTimeLegacy(fromTime);
    }

    private DateTime GetNextRunTimeFromDaySchedules(DateTime fromTime)
    {
        DateTime? earliestNextRun = null;

        // Check each day schedule
        foreach (var daySchedule in Schedules!)
        {
            var parsedDays = daySchedule.GetParsedDays();
            if (!parsedDays.Any())
                continue;

            var parts = (daySchedule.StartTime ?? StartTime).Split(':');
            var startHour = int.Parse(parts[0]);
            var startMinute = parts.Length > 1 ? int.Parse(parts[1]) : 0;
            var interval = daySchedule.IntervalMinutes ?? IntervalMinutes;

            // Check each day in this schedule
            foreach (var day in parsedDays)
            {
                var dayNum = (int)day;
                // Find the next occurrence of this day
                var daysUntil = ((dayNum - (int)fromTime.DayOfWeek) + 7) % 7;
                var targetDate = fromTime.Date.AddDays(daysUntil);
                var dayStart = targetDate.AddHours(startHour).AddMinutes(startMinute);

                DateTime nextRun;

                if (daysUntil == 0) // Today
                {
                    if (dayStart > fromTime)
                    {
                        nextRun = dayStart;
                    }
                    else
                    {
                        var minutesSinceStart = (fromTime - dayStart).TotalMinutes;
                        var intervalsPassed = (int)Math.Floor(minutesSinceStart / interval);
                        nextRun = dayStart.AddMinutes((intervalsPassed + 1) * interval);

                        // If next run is tomorrow, skip to next week for this day
                        if (nextRun.Date != targetDate)
                        {
                            targetDate = targetDate.AddDays(7);
                            nextRun = targetDate.AddHours(startHour).AddMinutes(startMinute);
                        }
                    }
                }
                else
                {
                    nextRun = dayStart;
                }

                if (earliestNextRun == null || nextRun < earliestNextRun)
                {
                    earliestNextRun = nextRun;
                }
            }
        }

        return earliestNextRun ?? GetNextRunTimeLegacy(fromTime);
    }

    private DateTime GetNextRunTimeLegacy(DateTime fromTime)
    {
        // Parse start time
        var parts = StartTime.Split(':');
        var startHour = int.Parse(parts[0]);
        var startMinute = parts.Length > 1 ? int.Parse(parts[1]) : 0;

        // Get today's start time
        var todayStart = fromTime.Date.AddHours(startHour).AddMinutes(startMinute);

        DateTime nextRun;

        if (todayStart > fromTime)
        {
            // Start time hasn't occurred yet today
            nextRun = todayStart;
        }
        else
        {
            // Calculate intervals since start time
            var minutesSinceStart = (fromTime - todayStart).TotalMinutes;
            var intervalsPassed = (int)Math.Floor(minutesSinceStart / IntervalMinutes);
            nextRun = todayStart.AddMinutes((intervalsPassed + 1) * IntervalMinutes);
        }

        // Apply day-of-week filter if specified
        if (DaysOfWeek != null && DaysOfWeek.Any())
        {
            while (!DaysOfWeek.Contains((int)nextRun.DayOfWeek))
            {
                nextRun = nextRun.Date.AddDays(1).AddHours(startHour).AddMinutes(startMinute);
            }
        }

        return nextRun;
    }

    /// <summary>
    /// Get a human-readable description of the schedule
    /// </summary>
    public string GetDescription(BlackoutWindowConfig? blackoutWindow = null)
    {
        if (HasDaySchedules)
        {
            return GetDaySchedulesDescription(blackoutWindow);
        }

        return GetLegacyDescription(blackoutWindow);
    }

    private string GetDaySchedulesDescription(BlackoutWindowConfig? blackoutWindow)
    {
        var parts = new List<string>();

        foreach (var daySchedule in Schedules!)
        {
            var interval = (daySchedule.IntervalMinutes ?? IntervalMinutes) switch
            {
                1 => "every minute",
                < 60 => $"every {daySchedule.IntervalMinutes ?? IntervalMinutes} min",
                60 => "hourly",
                < 1440 => $"every {(daySchedule.IntervalMinutes ?? IntervalMinutes) / 60}h",
                1440 => "daily",
                _ => $"every {(daySchedule.IntervalMinutes ?? IntervalMinutes) / 1440}d"
            };

            var parsedDays = daySchedule.GetParsedDays();
            var days = !parsedDays.Any()
                ? ""
                : string.Join("/", parsedDays.Select(d => d.ToString().Substring(0, 3)));

            var mode = daySchedule.ForceFullRefresh ? " (Full)" : "";

            parts.Add($"{days}: {interval}{mode}");
        }

        var description = string.Join(", ", parts);

        if (blackoutWindow?.Enabled == true)
        {
            description += $" (blackout {blackoutWindow.StartTime}-{blackoutWindow.EndTime})";
        }

        return description;
    }

    private string GetLegacyDescription(BlackoutWindowConfig? blackoutWindow)
    {
        var interval = IntervalMinutes switch
        {
            1 => "every minute",
            < 60 => $"every {IntervalMinutes} minutes",
            60 => "hourly",
            < 1440 => $"every {IntervalMinutes / 60} hours",
            1440 => "daily",
            _ => $"every {IntervalMinutes / 1440} days"
        };

        var days = DaysOfWeek == null || !DaysOfWeek.Any()
            ? ""
            : $" on {string.Join(", ", DaysOfWeek.Select(d => ((DayOfWeek)d).ToString()))}";

        // Check if the configured start time falls within the blackout window
        var effectiveStartTime = StartTime;
        var blackoutNote = "";

        if (blackoutWindow?.Enabled == true)
        {
            // Parse start time and check if it's in the blackout window
            var parts = StartTime.Split(':');
            var startHour = int.Parse(parts[0]);
            var startMinute = parts.Length > 1 ? int.Parse(parts[1]) : 0;
            var checkTime = DateTime.Today.AddHours(startHour).AddMinutes(startMinute);

            if (blackoutWindow.IsInBlackoutWindow(checkTime))
            {
                effectiveStartTime = blackoutWindow.EndTime;
                blackoutNote = $" (blackout until {blackoutWindow.EndTime})";
            }
        }

        return $"{interval} starting at {effectiveStartTime}{days}{blackoutNote}";
    }
}

/// <summary>
/// Day-specific schedule configuration.
/// Allows different sync intervals and modes for different days of the week.
/// </summary>
public class DaySchedule
{
    /// <summary>
    /// Days of the week this schedule applies to.
    /// Use day names: "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
    /// Also accepts 3-letter abbreviations: "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"
    /// Example: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"] for weekdays
    /// </summary>
    public List<string>? Days { get; set; }

    /// <summary>
    /// Minutes between sync runs for these days.
    /// If not specified, uses the parent ScheduleConfig.IntervalMinutes.
    /// </summary>
    public int? IntervalMinutes { get; set; }

    /// <summary>
    /// Start time for these days (24-hour format: "HH:mm").
    /// If not specified, uses the parent ScheduleConfig.StartTime.
    /// </summary>
    public string? StartTime { get; set; }

    /// <summary>
    /// Force full refresh mode on these days, overriding table-level Mode settings.
    /// When true, all tables sync as FullRefresh regardless of their configured Mode.
    /// </summary>
    public bool ForceFullRefresh { get; set; } = false;

    /// <summary>
    /// Parse day name strings to DayOfWeek values
    /// </summary>
    public List<DayOfWeek> GetParsedDays()
    {
        if (Days == null || !Days.Any())
            return new List<DayOfWeek>();

        var result = new List<DayOfWeek>();
        foreach (var day in Days)
        {
            if (TryParseDayOfWeek(day, out var dayOfWeek))
            {
                result.Add(dayOfWeek);
            }
        }
        return result;
    }

    /// <summary>
    /// Check if this schedule applies to a specific day
    /// </summary>
    public bool AppliesToDay(DayOfWeek day)
    {
        return GetParsedDays().Contains(day);
    }

    private static bool TryParseDayOfWeek(string value, out DayOfWeek dayOfWeek)
    {
        dayOfWeek = DayOfWeek.Sunday;
        if (string.IsNullOrWhiteSpace(value))
            return false;

        var normalized = value.Trim().ToLowerInvariant();

        // Try full names
        if (Enum.TryParse<DayOfWeek>(value, ignoreCase: true, out dayOfWeek))
            return true;

        // Try 3-letter abbreviations
        dayOfWeek = normalized switch
        {
            "sun" => DayOfWeek.Sunday,
            "mon" => DayOfWeek.Monday,
            "tue" => DayOfWeek.Tuesday,
            "wed" => DayOfWeek.Wednesday,
            "thu" => DayOfWeek.Thursday,
            "fri" => DayOfWeek.Friday,
            "sat" => DayOfWeek.Saturday,
            _ => (DayOfWeek)(-1)
        };

        return (int)dayOfWeek >= 0;
    }
}

/// <summary>
/// Profile-specific sync options
/// </summary>
public class ProfileOptions
{
    /// <summary>
    /// Maximum number of tables to sync in parallel
    /// </summary>
    public int MaxParallelTables { get; set; } = 4;
    
    /// <summary>
    /// Database command timeout in seconds
    /// </summary>
    public int CommandTimeoutSeconds { get; set; } = 3600;
    
    /// <summary>
    /// Track sync history in the _sync_history table
    /// </summary>
    public bool EnableSyncHistory { get; set; } = true;
    
    /// <summary>
    /// Automatically use last sync time from history for incremental syncs
    /// </summary>
    public bool UseHistoryForIncrementalSync { get; set; } = true;
    
    /// <summary>
    /// Stop syncing remaining tables if any table fails
    /// </summary>
    public bool StopOnError { get; set; } = false;
}

/// <summary>
/// Configuration for syncing a specific table
/// </summary>
public class TableConfig
{
    /// <summary>
    /// Source table name (as it appears in the source database)
    /// </summary>
    public string SourceTable { get; set; } = string.Empty;
    
    /// <summary>
    /// Target table name (optional - defaults to lowercase of SourceTable)
    /// </summary>
    public string? TargetTable { get; set; }
    
    /// <summary>
    /// Effective target table name
    /// </summary>
    public string EffectiveTargetTable => 
        string.IsNullOrEmpty(TargetTable) 
            ? SourceTable.ToLower() 
            : TargetTable.ToLower();
    
    /// <summary>
    /// Sync mode: FullRefresh or Incremental
    /// </summary>
    public SyncMode Mode { get; set; } = SyncMode.Incremental;
    
    /// <summary>
    /// Column containing last modified timestamp (required for Incremental mode)
    /// </summary>
    public string? TimestampColumn { get; set; }

    /// <summary>
    /// Fallback column to use when TimestampColumn is NULL.
    /// When specified, the query uses COALESCE(TimestampColumn, FallbackTimestampColumn).
    /// Useful for tables where new records may have NULL in the main timestamp column.
    /// Example: TimestampColumn = "LastEditDT", FallbackTimestampColumn = "EntryDT"
    /// </summary>
    public string? FallbackTimestampColumn { get; set; }

    /// <summary>
    /// Override last sync time (bypasses sync history lookup)
    /// </summary>
    public DateTime? LastSyncTimeOverride { get; set; }

    /// <summary>
    /// Lookback hours for Incremental mode - re-sync rows from (lastSyncTime - LookbackHours)
    /// This catches late-arriving changes or updates that may have been missed.
    /// Default is 0 (no lookback). Set to 72 for 3-day lookback, etc.
    /// </summary>
    public int LookbackHours { get; set; } = 0;

    /// <summary>
    /// Create target table if it doesn't exist
    /// </summary>
    public bool CreateIfMissing { get; set; } = false;
    
    /// <summary>
    /// Delete mode - sync deletes from source to target
    /// When enabled, rows that don't exist in source will be deleted from target
    /// </summary>
    public DeleteMode DeleteMode { get; set; } = DeleteMode.None;

    /// <summary>
    /// For Incremental sync with DeleteMode.Sync: perform full PK comparison to catch all deletes.
    /// When true: Compares ALL source PKs vs ALL target PKs (slower but catches all deletes)
    /// When false: Only deletes are based on the incremental sync window (may miss deletes)
    /// Default is false. For FullRefresh mode, this setting is ignored (always does full comparison).
    /// </summary>
    public bool SyncAllDeletes { get; set; } = false;

    /// <summary>
    /// Sync priority - lower numbers sync first
    /// Tables with same priority may sync in parallel
    /// </summary>
    public int Priority { get; set; } = 100;
    
    /// <summary>
    /// Optional WHERE clause filter for source data (without WHERE keyword)
    /// Example: "IsActive = 1" or "CreatedDate > '2020-01-01'"
    /// </summary>
    public string? SourceFilter { get; set; }
}
