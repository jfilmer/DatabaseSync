namespace DatabaseSync.Models;

/// <summary>
/// Represents a sync history record stored in the target database
/// </summary>
public class SyncHistory
{
    /// <summary>
    /// Auto-generated primary key
    /// </summary>
    public long Id { get; set; }
    
    /// <summary>
    /// Unique identifier for the sync run
    /// </summary>
    public Guid RunId { get; set; }
    
    /// <summary>
    /// Profile name that performed the sync
    /// </summary>
    public string ProfileName { get; set; } = string.Empty;
    
    /// <summary>
    /// Name of the source table
    /// </summary>
    public string SourceTable { get; set; } = string.Empty;
    
    /// <summary>
    /// Name of the target table
    /// </summary>
    public string TargetTable { get; set; } = string.Empty;
    
    /// <summary>
    /// When the sync started
    /// </summary>
    public DateTime SyncStartTime { get; set; }
    
    /// <summary>
    /// When the sync completed
    /// </summary>
    public DateTime SyncEndTime { get; set; }
    
    /// <summary>
    /// Whether the sync was successful
    /// </summary>
    public bool Success { get; set; }
    
    /// <summary>
    /// Number of rows processed
    /// </summary>
    public long RowsProcessed { get; set; }
    
    /// <summary>
    /// Number of rows inserted
    /// </summary>
    public long RowsInserted { get; set; }
    
    /// <summary>
    /// Number of rows updated
    /// </summary>
    public long RowsUpdated { get; set; }
    
    /// <summary>
    /// Number of rows deleted
    /// </summary>
    public long RowsDeleted { get; set; }
    
    /// <summary>
    /// Error message if sync failed
    /// </summary>
    public string? ErrorMessage { get; set; }
    
    /// <summary>
    /// Maximum timestamp value from source at sync time
    /// Used for incremental sync resume
    /// </summary>
    public DateTime? MaxSourceTimestamp { get; set; }

    /// <summary>
    /// Duration in seconds
    /// </summary>
    public double DurationSeconds { get; set; }

    /// <summary>
    /// Number of rows that were recently modified (within last 168 hours based on timestamp column)
    /// Used to identify good candidates for incremental sync
    /// </summary>
    public long RecentRowsCount { get; set; }

    /// <summary>
    /// Total number of rows in the source table at sync time
    /// Used to calculate Recent % (RecentRowsCount / TotalSourceRows)
    /// </summary>
    public long TotalSourceRows { get; set; }
}

/// <summary>
/// Summary of last sync for a specific table
/// </summary>
public class LastSyncInfo
{
    public string ProfileName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public DateTime? LastSuccessfulSync { get; set; }
    public DateTime? LastSyncAttempt { get; set; }
    public DateTime? MaxSourceTimestamp { get; set; }
    public bool LastSyncSuccessful { get; set; }
    public long TotalRowsSynced { get; set; }
}

/// <summary>
/// Status information for a sync profile
/// </summary>
public class ProfileStatusInfo
{
    public string ProfileName { get; set; } = string.Empty;
    public string? Description { get; set; }
    public bool IsRunning { get; set; }
    public bool ScheduleEnabled { get; set; }
    public DateTime? NextRunTime { get; set; }
    public DateTime? LastRunTime { get; set; }
    public bool LastRunSuccess { get; set; }
    public string? LastRunMessage { get; set; }
    public int IntervalMinutes { get; set; }
    public string ScheduleDescription { get; set; } = string.Empty;
    public int TableCount { get; set; }

    /// <summary>
    /// Progress details for tables currently being synced (when IsRunning is true)
    /// </summary>
    public List<TableSyncProgressInfo> CurrentTableProgress { get; set; } = new();

    /// <summary>
    /// When the current sync run started (when IsRunning is true)
    /// </summary>
    public DateTime? CurrentRunStartTime { get; set; }
}

/// <summary>
/// Progress information for a table sync operation (for display purposes)
/// </summary>
public class TableSyncProgressInfo
{
    public string TableName { get; set; } = string.Empty;
    public long RowsProcessed { get; set; }
    public long EstimatedTotalRows { get; set; }
    public DateTime StartTime { get; set; }
    public string Phase { get; set; } = "Loading"; // Loading, Upserting, Deleting

    /// <summary>
    /// Progress percentage (0-100), or -1 if unknown
    /// </summary>
    public int ProgressPercent => EstimatedTotalRows > 0
        ? (int)Math.Min(100, RowsProcessed * 100 / EstimatedTotalRows)
        : -1;

    /// <summary>
    /// Elapsed time since sync started
    /// </summary>
    public TimeSpan Elapsed => DateTime.UtcNow - StartTime;
}
