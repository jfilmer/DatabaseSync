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
}
