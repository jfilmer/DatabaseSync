using Dapper;
using DatabaseSync.Abstractions;
using DatabaseSync.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DatabaseSync.SqlServer;

/// <summary>
/// Repository for tracking sync history in SQL Server
/// </summary>
public class SqlServerSyncHistoryRepository : ISyncHistoryRepository
{
    private readonly string _connectionString;
    private readonly ILogger<SqlServerSyncHistoryRepository> _logger;
    private const string TableName = "_sync_history";

    public SqlServerSyncHistoryRepository(
        string connectionString,
        ILogger<SqlServerSyncHistoryRepository> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task InitializeAsync()
    {
        const string createTableSql = $@"
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{TableName}')
            BEGIN
                CREATE TABLE [{TableName}] (
                    id BIGINT IDENTITY(1,1) PRIMARY KEY,
                    run_id UNIQUEIDENTIFIER NOT NULL,
                    profile_name VARCHAR(100) NOT NULL,
                    source_table VARCHAR(255) NOT NULL,
                    target_table VARCHAR(255) NOT NULL,
                    sync_start_time DATETIME2 NOT NULL,
                    sync_end_time DATETIME2 NOT NULL,
                    success BIT NOT NULL,
                    rows_processed BIGINT NOT NULL DEFAULT 0,
                    rows_inserted BIGINT NOT NULL DEFAULT 0,
                    rows_updated BIGINT NOT NULL DEFAULT 0,
                    rows_deleted BIGINT NOT NULL DEFAULT 0,
                    error_message NVARCHAR(MAX),
                    max_source_timestamp DATETIME2,
                    duration_seconds FLOAT NOT NULL,
                    recent_rows_count BIGINT NOT NULL DEFAULT 0,
                    total_source_rows BIGINT NOT NULL DEFAULT 0,
                    created_at DATETIME2 DEFAULT GETUTCDATE()
                );

                CREATE INDEX idx_sync_history_profile_table
                    ON [{TableName}] (profile_name, source_table);
                CREATE INDEX idx_sync_history_run_id
                    ON [{TableName}] (run_id);
                CREATE INDEX idx_sync_history_sync_time
                    ON [{TableName}] (sync_end_time DESC);
            END
        ";

        // Add columns if they don't exist (for existing tables)
        const string addRecentRowsColumnSql = $@"
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{TableName}' AND COLUMN_NAME = 'recent_rows_count'
            )
            BEGIN
                ALTER TABLE [{TableName}] ADD recent_rows_count BIGINT NOT NULL DEFAULT 0;
            END
        ";

        const string addTotalSourceRowsColumnSql = $@"
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{TableName}' AND COLUMN_NAME = 'total_source_rows'
            )
            BEGIN
                ALTER TABLE [{TableName}] ADD total_source_rows BIGINT NOT NULL DEFAULT 0;
            END
        ";

        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(createTableSql);
        await connection.ExecuteAsync(addRecentRowsColumnSql);
        await connection.ExecuteAsync(addTotalSourceRowsColumnSql);

        _logger.LogDebug("Sync history table initialized");
    }

    public async Task RecordSyncAsync(SyncHistory history)
    {
        const string sql = $@"
            INSERT INTO [{TableName}] (
                run_id, profile_name, source_table, target_table,
                sync_start_time, sync_end_time, success,
                rows_processed, rows_inserted, rows_updated, rows_deleted,
                error_message, max_source_timestamp, duration_seconds,
                recent_rows_count, total_source_rows
            ) VALUES (
                @RunId, @ProfileName, @SourceTable, @TargetTable,
                @SyncStartTime, @SyncEndTime, @Success,
                @RowsProcessed, @RowsInserted, @RowsUpdated, @RowsDeleted,
                @ErrorMessage, @MaxSourceTimestamp, @DurationSeconds,
                @RecentRowsCount, @TotalSourceRows
            )";

        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, new
        {
            history.RunId,
            history.ProfileName,
            history.SourceTable,
            history.TargetTable,
            history.SyncStartTime,
            history.SyncEndTime,
            history.Success,
            history.RowsProcessed,
            history.RowsInserted,
            history.RowsUpdated,
            history.RowsDeleted,
            history.ErrorMessage,
            history.MaxSourceTimestamp,
            history.DurationSeconds,
            history.RecentRowsCount,
            history.TotalSourceRows
        });

        _logger.LogDebug(
            "Recorded sync history for {Profile}/{Table}: {Success}",
            history.ProfileName,
            history.SourceTable,
            history.Success ? "SUCCESS" : "FAILED");
    }

    public async Task<LastSyncInfo?> GetLastSyncInfoAsync(string profileName, string sourceTable)
    {
        const string sql = $@"
            SELECT
                profile_name AS ProfileName,
                source_table AS TableName,
                MAX(CASE WHEN success = 1 THEN sync_end_time END) AS LastSuccessfulSync,
                MAX(sync_end_time) AS LastSyncAttempt,
                (SELECT TOP 1 max_source_timestamp
                 FROM [{TableName}] h2
                 WHERE h2.profile_name = h.profile_name
                   AND h2.source_table = h.source_table
                   AND h2.success = 1
                 ORDER BY sync_end_time DESC) AS MaxSourceTimestamp,
                (SELECT TOP 1 success
                 FROM [{TableName}] h3
                 WHERE h3.profile_name = h.profile_name
                   AND h3.source_table = h.source_table
                 ORDER BY sync_end_time DESC) AS LastSyncSuccessful,
                COALESCE(SUM(CASE WHEN success = 1 THEN rows_processed ELSE 0 END), 0) AS TotalRowsSynced
            FROM [{TableName}] h
            WHERE profile_name = @profileName AND source_table = @sourceTable
            GROUP BY profile_name, source_table";

        await using var connection = new SqlConnection(_connectionString);
        return await connection.QuerySingleOrDefaultAsync<LastSyncInfo>(
            sql,
            new { profileName, sourceTable });
    }

    public async Task<List<SyncHistory>> GetSyncHistoryAsync(string profileName, string sourceTable, int limit = 10)
    {
        var sql = $@"
            SELECT TOP (@limit)
                id AS Id,
                run_id AS RunId,
                profile_name AS ProfileName,
                source_table AS SourceTable,
                target_table AS TargetTable,
                sync_start_time AS SyncStartTime,
                sync_end_time AS SyncEndTime,
                success AS Success,
                rows_processed AS RowsProcessed,
                rows_inserted AS RowsInserted,
                rows_updated AS RowsUpdated,
                rows_deleted AS RowsDeleted,
                error_message AS ErrorMessage,
                max_source_timestamp AS MaxSourceTimestamp,
                duration_seconds AS DurationSeconds,
                recent_rows_count AS RecentRowsCount,
                total_source_rows AS TotalSourceRows
            FROM [{TableName}]
            WHERE profile_name = @profileName AND source_table = @sourceTable
            ORDER BY sync_end_time DESC";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<SyncHistory>(
            sql,
            new { profileName, sourceTable, limit });
        return results.ToList();
    }

    public async Task<Dictionary<string, LastSyncInfo>> GetAllLastSyncInfoAsync(string profileName)
    {
        const string sql = $@"
            WITH RankedHistory AS (
                SELECT
                    profile_name,
                    source_table,
                    sync_end_time,
                    max_source_timestamp,
                    success,
                    rows_processed,
                    ROW_NUMBER() OVER (PARTITION BY source_table ORDER BY sync_end_time DESC) AS rn
                FROM [{TableName}]
                WHERE profile_name = @profileName
            )
            SELECT
                profile_name AS ProfileName,
                source_table AS TableName,
                sync_end_time AS LastSyncAttempt,
                CASE WHEN success = 1 THEN sync_end_time END AS LastSuccessfulSync,
                max_source_timestamp AS MaxSourceTimestamp,
                success AS LastSyncSuccessful,
                rows_processed AS TotalRowsSynced
            FROM RankedHistory
            WHERE rn = 1";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<LastSyncInfo>(sql, new { profileName });
        return results.ToDictionary(r => r.TableName, r => r);
    }

    public async Task<List<SyncHistory>> GetRecentHistoryAsync(string profileName, int limit = 50)
    {
        var sql = $@"
            SELECT TOP (@limit)
                id AS Id,
                run_id AS RunId,
                profile_name AS ProfileName,
                source_table AS SourceTable,
                target_table AS TargetTable,
                sync_start_time AS SyncStartTime,
                sync_end_time AS SyncEndTime,
                success AS Success,
                rows_processed AS RowsProcessed,
                rows_inserted AS RowsInserted,
                rows_updated AS RowsUpdated,
                rows_deleted AS RowsDeleted,
                error_message AS ErrorMessage,
                max_source_timestamp AS MaxSourceTimestamp,
                duration_seconds AS DurationSeconds,
                recent_rows_count AS RecentRowsCount,
                total_source_rows AS TotalSourceRows
            FROM [{TableName}]
            WHERE profile_name = @profileName
            ORDER BY sync_end_time DESC";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<SyncHistory>(
            sql,
            new { profileName, limit });
        return results.ToList();
    }

    /// <summary>
    /// Clean up old sync history records
    /// </summary>
    public async Task<int> CleanupOldHistoryAsync(int retentionDays = 30)
    {
        var sql = $@"
            DELETE FROM [{TableName}]
            WHERE sync_end_time < DATEADD(DAY, -@retentionDays, GETUTCDATE())";

        await using var connection = new SqlConnection(_connectionString);
        var deleted = await connection.ExecuteAsync(sql, new { retentionDays });

        if (deleted > 0)
        {
            _logger.LogInformation(
                "Cleaned up {Count} sync history records older than {Days} days",
                deleted, retentionDays);
        }

        return deleted;
    }

    /// <summary>
    /// Rename a profile in sync history (migrate old records to new profile ID)
    /// </summary>
    public async Task<int> RenameProfileAsync(string oldProfileName, string newProfileName)
    {
        var sql = $@"
            UPDATE [{TableName}]
            SET profile_name = @newProfileName
            WHERE profile_name = @oldProfileName";

        await using var connection = new SqlConnection(_connectionString);
        var updated = await connection.ExecuteAsync(sql, new { oldProfileName, newProfileName });

        _logger.LogInformation(
            "Renamed profile '{OldName}' to '{NewName}' in {Count} history records",
            oldProfileName, newProfileName, updated);

        return updated;
    }

    /// <summary>
    /// Get distinct profile names from sync history
    /// </summary>
    public async Task<List<string>> GetProfileNamesFromHistoryAsync()
    {
        var sql = $@"
            SELECT DISTINCT profile_name
            FROM [{TableName}]
            ORDER BY profile_name";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<string>(sql);
        return results.ToList();
    }
}
