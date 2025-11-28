namespace DatabaseSync.Enums;

/// <summary>
/// Supported database types for connections
/// </summary>
public enum DatabaseType
{
    SqlServer,
    PostgreSql
}

/// <summary>
/// Sync mode for table synchronization
/// </summary>
public enum SyncMode
{
    /// <summary>
    /// Upsert all rows from source (insert new, update existing)
    /// </summary>
    FullRefresh,
    
    /// <summary>
    /// Only sync rows changed since last sync time (requires timestamp column)
    /// </summary>
    Incremental
}

/// <summary>
/// How to handle deleted rows
/// </summary>
public enum DeleteMode
{
    /// <summary>
    /// Don't sync deletes - only insert and update
    /// </summary>
    None,
    
    /// <summary>
    /// Synchronized deletes - if row doesn't exist in source, delete from target
    /// </summary>
    Sync
}
