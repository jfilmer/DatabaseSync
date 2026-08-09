using DatabaseSync.Models;

namespace DatabaseSync.Abstractions;

/// <summary>
/// Interface for analyzing database schema
/// </summary>
public interface ISchemaAnalyzer
{
    /// <summary>
    /// Find tables matching a pattern
    /// </summary>
    Task<List<string>> FindTablesAsync(string pattern);
    
    /// <summary>
    /// Get detailed schema information for a table
    /// </summary>
    Task<List<ColumnInfo>> GetTableSchemaAsync(string tableName);
    
    /// <summary>
    /// Check if a table exists
    /// </summary>
    Task<bool> TableExistsAsync(string tableName);
    
    /// <summary>
    /// Get the row count for a table
    /// </summary>
    Task<long> GetRowCountAsync(string tableName, string? whereClause = null);

    /// <summary>
    /// Approximate row count read from catalog metadata, NOT COUNT(*).
    /// Used to decide whether a table is too large to force a full refresh on
    /// (see ProfileOptions.ForceFullRefreshMaxRows) — a decision that must not itself
    /// cost a full scan, since COUNT(*) on the 137M-row tbl_Archive_Track is exactly
    /// the kind of work the check exists to avoid. Accuracy is irrelevant here: the
    /// threshold is an order-of-magnitude guard, not an exact bound.
    /// Returns null when the count cannot be determined, which callers MUST treat as
    /// "unknown - change nothing", preserving existing behaviour. AIM #1966.
    /// </summary>
    Task<long?> GetEstimatedRowCountAsync(string tableName);
    
    /// <summary>
    /// Get the maximum value of a timestamp column
    /// </summary>
    Task<DateTime?> GetMaxTimestampAsync(string tableName, string timestampColumn);

    /// <summary>
    /// Get count of rows where timestamp is within the specified hours from now
    /// Uses COALESCE with fallback column if provided
    /// </summary>
    Task<long> GetRecentRowsCountAsync(
        string tableName,
        string timestampColumn,
        string? fallbackTimestampColumn,
        int hoursBack,
        string? sourceFilter = null);
}

/// <summary>
/// Interface for sync history tracking
/// </summary>
public interface ISyncHistoryRepository
{
    /// <summary>
    /// Initialize the sync history table
    /// </summary>
    Task InitializeAsync();
    
    /// <summary>
    /// Record a sync operation
    /// </summary>
    Task RecordSyncAsync(SyncHistory history);

    /// <summary>
    /// Back-fill the delete count onto an already-written history row, keyed on
    /// (run_id, source_table) which is unique within a run.
    ///
    /// Needed because PG→PG two-phase sync writes the history row at the end of PHASE 1,
    /// with deletes deliberately skipped, and then performs the deletes in PHASE 2 — which
    /// previously only updated the in-memory SyncResult. That is why the run summary was
    /// right while _sync_history.rows_deleted read 0 on every row of every PG mirror
    /// profile (25,372 rows measured). The SQL Server path deletes inline, so its row is
    /// already correct and this is simply a no-op there.
    ///
    /// MUST NOT throw: a failure to annotate history is never a reason to fail a sync that
    /// already moved data. Implementations swallow and log. AIM #1964.
    /// </summary>
    Task UpdateDeleteCountAsync(Guid runId, string sourceTable, long rowsDeleted);
    
    /// <summary>
    /// Get the last successful sync info for a table
    /// </summary>
    Task<LastSyncInfo?> GetLastSyncInfoAsync(string profileName, string sourceTable);
    
    /// <summary>
    /// Get sync history for a table
    /// </summary>
    Task<List<SyncHistory>> GetSyncHistoryAsync(string profileName, string sourceTable, int limit = 10);
    
    /// <summary>
    /// Get all last sync info for a profile
    /// </summary>
    Task<Dictionary<string, LastSyncInfo>> GetAllLastSyncInfoAsync(string profileName);

    /// <summary>
    /// Get recent sync history across all tables for a profile
    /// </summary>
    Task<List<SyncHistory>> GetRecentHistoryAsync(string profileName, int limit = 50);

    /// <summary>
    /// Rename a profile in sync history (migrate old records to new profile ID)
    /// </summary>
    Task<int> RenameProfileAsync(string oldProfileName, string newProfileName);

    /// <summary>
    /// Get distinct profile names from sync history
    /// </summary>
    Task<List<string>> GetProfileNamesFromHistoryAsync();
}
