using Dapper;
using DatabaseSync.Abstractions;
using DatabaseSync.Models;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DatabaseSync.PostgreSql;

/// <summary>
/// Repository for tracking sync history in PostgreSQL
/// </summary>
public class PostgreSqlSyncHistoryRepository : ISyncHistoryRepository
{
    private readonly string _connectionString;
    private readonly ILogger<PostgreSqlSyncHistoryRepository> _logger;
    private const string TableName = "_sync_history";

    public PostgreSqlSyncHistoryRepository(
        string connectionString, 
        ILogger<PostgreSqlSyncHistoryRepository> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task InitializeAsync()
    {
        const string createTableSql = $@"
            CREATE TABLE IF NOT EXISTS ""{TableName}"" (
                id BIGSERIAL PRIMARY KEY,
                run_id UUID NOT NULL,
                profile_name VARCHAR(100) NOT NULL,
                source_table VARCHAR(255) NOT NULL,
                target_table VARCHAR(255) NOT NULL,
                sync_start_time TIMESTAMP NOT NULL,
                sync_end_time TIMESTAMP NOT NULL,
                success BOOLEAN NOT NULL,
                rows_processed BIGINT NOT NULL DEFAULT 0,
                rows_inserted BIGINT NOT NULL DEFAULT 0,
                rows_updated BIGINT NOT NULL DEFAULT 0,
                rows_deleted BIGINT NOT NULL DEFAULT 0,
                error_message TEXT,
                max_source_timestamp TIMESTAMP,
                duration_seconds DOUBLE PRECISION NOT NULL,
                recent_rows_count BIGINT NOT NULL DEFAULT 0,
                total_source_rows BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );

            CREATE INDEX IF NOT EXISTS idx_sync_history_profile_table
                ON ""{TableName}"" (profile_name, source_table);
            CREATE INDEX IF NOT EXISTS idx_sync_history_run_id
                ON ""{TableName}"" (run_id);
            CREATE INDEX IF NOT EXISTS idx_sync_history_sync_time
                ON ""{TableName}"" (sync_end_time DESC);
        ";

        // Add columns if they don't exist (for existing tables)
        const string addRecentRowsColumnSql = $@"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{TableName}' AND column_name = 'recent_rows_count'
                ) THEN
                    ALTER TABLE ""{TableName}"" ADD COLUMN recent_rows_count BIGINT NOT NULL DEFAULT 0;
                END IF;
            END $$;
        ";

        const string addTotalSourceRowsColumnSql = $@"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{TableName}' AND column_name = 'total_source_rows'
                ) THEN
                    ALTER TABLE ""{TableName}"" ADD COLUMN total_source_rows BIGINT NOT NULL DEFAULT 0;
                END IF;
            END $$;
        ";

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.ExecuteAsync(createTableSql);
        await connection.ExecuteAsync(addRecentRowsColumnSql);
        await connection.ExecuteAsync(addTotalSourceRowsColumnSql);

        _logger.LogDebug("Sync history table initialized");
    }

    public async Task RecordSyncAsync(SyncHistory history)
    {
        const string sql = $@"
            INSERT INTO ""{TableName}"" (
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

        await using var connection = new NpgsqlConnection(_connectionString);
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
                MAX(CASE WHEN success THEN sync_end_time END) AS LastSuccessfulSync,
                MAX(sync_end_time) AS LastSyncAttempt,
                (SELECT max_source_timestamp 
                 FROM ""{TableName}"" h2 
                 WHERE h2.profile_name = h.profile_name
                   AND h2.source_table = h.source_table 
                   AND h2.success = TRUE 
                 ORDER BY sync_end_time DESC 
                 LIMIT 1) AS MaxSourceTimestamp,
                (SELECT success 
                 FROM ""{TableName}"" h3 
                 WHERE h3.profile_name = h.profile_name
                   AND h3.source_table = h.source_table 
                 ORDER BY sync_end_time DESC 
                 LIMIT 1) AS LastSyncSuccessful,
                COALESCE(SUM(CASE WHEN success THEN rows_processed ELSE 0 END), 0) AS TotalRowsSynced
            FROM ""{TableName}"" h
            WHERE profile_name = @profileName AND source_table = @sourceTable
            GROUP BY profile_name, source_table";

        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.QuerySingleOrDefaultAsync<LastSyncInfo>(
            sql, 
            new { profileName, sourceTable });
    }

    public async Task<List<SyncHistory>> GetSyncHistoryAsync(string profileName, string sourceTable, int limit = 10)
    {
        const string sql = $@"
            SELECT
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
            FROM ""{TableName}""
            WHERE profile_name = @profileName AND source_table = @sourceTable
            ORDER BY sync_end_time DESC
            LIMIT @limit";

        await using var connection = new NpgsqlConnection(_connectionString);
        var results = await connection.QueryAsync<SyncHistory>(
            sql,
            new { profileName, sourceTable, limit });
        return results.ToList();
    }

    public async Task<Dictionary<string, LastSyncInfo>> GetAllLastSyncInfoAsync(string profileName)
    {
        const string sql = $@"
            SELECT DISTINCT ON (source_table)
                profile_name AS ProfileName,
                source_table AS TableName,
                sync_end_time AS LastSyncAttempt,
                CASE WHEN success THEN sync_end_time END AS LastSuccessfulSync,
                max_source_timestamp AS MaxSourceTimestamp,
                success AS LastSyncSuccessful,
                rows_processed AS TotalRowsSynced
            FROM ""{TableName}""
            WHERE profile_name = @profileName
            ORDER BY source_table, sync_end_time DESC";

        await using var connection = new NpgsqlConnection(_connectionString);
        var results = await connection.QueryAsync<LastSyncInfo>(sql, new { profileName });
        return results.ToDictionary(r => r.TableName, r => r);
    }

    public async Task<List<SyncHistory>> GetRecentHistoryAsync(string profileName, int limit = 50)
    {
        const string sql = $@"
            SELECT
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
            FROM ""{TableName}""
            WHERE profile_name = @profileName
            ORDER BY sync_end_time DESC
            LIMIT @limit";

        await using var connection = new NpgsqlConnection(_connectionString);
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
            DELETE FROM ""{TableName}""
            WHERE sync_end_time < NOW() - INTERVAL '{retentionDays} days'";

        await using var connection = new NpgsqlConnection(_connectionString);
        var deleted = await connection.ExecuteAsync(sql);
        
        if (deleted > 0)
        {
            _logger.LogInformation(
                "Cleaned up {Count} sync history records older than {Days} days", 
                deleted, retentionDays);
        }

        return deleted;
    }
}
