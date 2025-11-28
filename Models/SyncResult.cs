namespace DatabaseSync.Models;

/// <summary>
/// Results from a single table sync operation
/// </summary>
public class SyncResult
{
    /// <summary>
    /// Name of the table that was synced
    /// </summary>
    public string TableName { get; set; } = string.Empty;
    
    /// <summary>
    /// Whether the sync completed successfully
    /// </summary>
    public bool Success { get; set; }
    
    /// <summary>
    /// Total rows processed from source
    /// </summary>
    public long RowsProcessed { get; set; }
    
    /// <summary>
    /// Number of new rows inserted into target
    /// </summary>
    public long RowsInserted { get; set; }
    
    /// <summary>
    /// Number of existing rows updated in target
    /// </summary>
    public long RowsUpdated { get; set; }
    
    /// <summary>
    /// Number of rows deleted from target (if delete detection enabled)
    /// </summary>
    public long RowsDeleted { get; set; }

    /// <summary>
    /// Number of rows with timestamp within last 168 hours (7 days)
    /// Used to identify good candidates for incremental sync
    /// </summary>
    public long RecentRowsCount { get; set; }

    /// <summary>
    /// Time taken to complete the sync
    /// </summary>
    public TimeSpan Duration { get; set; }
    
    /// <summary>
    /// Error message if sync failed
    /// </summary>
    public string? Error { get; set; }
    
    /// <summary>
    /// Non-fatal warnings encountered during sync
    /// </summary>
    public List<string> Warnings { get; set; } = new();
    
    /// <summary>
    /// Calculated rows per second throughput
    /// </summary>
    public double RowsPerSecond => Duration.TotalSeconds > 0 
        ? RowsProcessed / Duration.TotalSeconds 
        : 0;
    
    /// <summary>
    /// Total rows affected (inserted + updated + deleted)
    /// </summary>
    public long TotalRowsAffected => RowsInserted + RowsUpdated + RowsDeleted;
}

/// <summary>
/// Results from a complete sync run (all tables in a profile)
/// </summary>
public class SyncRunResult
{
    /// <summary>
    /// Unique identifier for this sync run
    /// </summary>
    public Guid RunId { get; set; } = Guid.NewGuid();
    
    /// <summary>
    /// Profile name that was synced
    /// </summary>
    public string ProfileName { get; set; } = string.Empty;
    
    /// <summary>
    /// When the sync run started
    /// </summary>
    public DateTime StartTime { get; set; }
    
    /// <summary>
    /// When the sync run completed
    /// </summary>
    public DateTime EndTime { get; set; }
    
    /// <summary>
    /// Total duration of the sync run
    /// </summary>
    public TimeSpan Duration => EndTime - StartTime;
    
    /// <summary>
    /// Results for each table synced
    /// </summary>
    public List<SyncResult> TableResults { get; set; } = new();
    
    /// <summary>
    /// Number of tables that synced successfully
    /// </summary>
    public int SuccessCount => TableResults.Count(r => r.Success);
    
    /// <summary>
    /// Number of tables that failed to sync
    /// </summary>
    public int FailureCount => TableResults.Count(r => !r.Success);
    
    /// <summary>
    /// Total rows processed across all tables
    /// </summary>
    public long TotalRowsProcessed => TableResults.Sum(r => r.RowsProcessed);
    
    /// <summary>
    /// Total rows inserted across all tables
    /// </summary>
    public long TotalRowsInserted => TableResults.Sum(r => r.RowsInserted);
    
    /// <summary>
    /// Total rows updated across all tables
    /// </summary>
    public long TotalRowsUpdated => TableResults.Sum(r => r.RowsUpdated);
    
    /// <summary>
    /// Total rows deleted across all tables
    /// </summary>
    public long TotalRowsDeleted => TableResults.Sum(r => r.RowsDeleted);
    
    /// <summary>
    /// Overall rows per second throughput
    /// </summary>
    public double OverallRowsPerSecond => Duration.TotalSeconds > 0 
        ? TotalRowsProcessed / Duration.TotalSeconds 
        : 0;
}

/// <summary>
/// Result from a bulk copy operation
/// </summary>
public class BulkCopyResult
{
    public long RowsProcessed { get; set; }
    public long RowsInserted { get; set; }
    public long RowsUpdated { get; set; }
    public long RowsDeleted { get; set; }

    /// <summary>
    /// Number of rows that were recently modified (within last 168 hours based on timestamp column)
    /// </summary>
    public long RecentRowsCount { get; set; }
}
