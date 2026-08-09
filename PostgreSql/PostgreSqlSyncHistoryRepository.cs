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

    /// <summary>
    /// Whether the target's _sync_history carries the 'skipped' column (AIM #1946).
    /// Probed during InitializeAsync rather than assumed: the table is frequently pre-created
    /// by an admin user while the sync user holds DML-only rights, so the ADD COLUMN below can
    /// be denied (42501). Writing an INSERT that names a column the table does not have would
    /// fail with 42703 and take down ALL history recording - far worse than the bug being
    /// fixed - so the INSERT adapts to what is actually there.
    /// </summary>
    private bool _hasSkippedColumn = true;

    public PostgreSqlSyncHistoryRepository(
        string connectionString, 
        ILogger<PostgreSqlSyncHistoryRepository> logger)
    {
        _connectionString = connectionString;
        _logger = logger;
    }

    public async Task InitializeAsync()
    {
        const string checkTableExistsSql = @"
            SELECT COUNT(1) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = '_sync_history'";

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
                skipped BOOLEAN NOT NULL DEFAULT FALSE,
                rows_processed BIGINT NOT NULL DEFAULT 0,
                rows_inserted BIGINT NOT NULL DEFAULT 0,
                rows_updated BIGINT NOT NULL DEFAULT 0,
                rows_deleted BIGINT NOT NULL DEFAULT 0,
                error_message TEXT,
                max_source_timestamp TIMESTAMP,
                duration_seconds DOUBLE PRECISION NOT NULL,
                recent_rows_count BIGINT NOT NULL DEFAULT 0,
                total_source_rows BIGINT NOT NULL DEFAULT 0,
                created_datetime TIMESTAMP DEFAULT NOW()
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

        const string addSkippedColumnSql = $@"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = '{TableName}' AND column_name = 'skipped'
                ) THEN
                    ALTER TABLE ""{TableName}"" ADD COLUMN skipped BOOLEAN NOT NULL DEFAULT FALSE;
                END IF;
            END $$;
        ";

        const string checkSkippedColumnSql = $@"
            SELECT COUNT(1) FROM information_schema.columns
            WHERE table_name = '{TableName}' AND column_name = 'skipped'";

        await using var connection = new NpgsqlConnection(_connectionString);

        // Check if table exists first - skip CREATE if it does (for users without CREATE permission)
        var tableExists = await connection.ExecuteScalarAsync<int>(checkTableExistsSql) > 0;

        if (!tableExists)
        {
            try
            {
                await connection.ExecuteAsync(createTableSql);
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                _logger.LogWarning(
                    "Cannot create sync history table - permission denied. " +
                    "Please create the _sync_history table manually with a privileged user.");
                throw;
            }
        }

        // Only run ALTER TABLE if table exists and user might have ALTER permission
        if (tableExists)
        {
            try
            {
                await connection.ExecuteAsync(addRecentRowsColumnSql);
                await connection.ExecuteAsync(addTotalSourceRowsColumnSql);
                await connection.ExecuteAsync(addSkippedColumnSql);
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                _logger.LogDebug("Cannot alter sync history table - permission denied. Columns may already exist.");
            }
        }

        // Probe rather than assume - see _hasSkippedColumn.
        _hasSkippedColumn = await connection.ExecuteScalarAsync<int>(checkSkippedColumnSql) > 0;
        if (!_hasSkippedColumn)
        {
            _logger.LogWarning(
                "_sync_history has no 'skipped' column - skipped tables will be recorded as success=true " +
                "and will be indistinguishable from real successes. Add it with a privileged user: " +
                "ALTER TABLE \"_sync_history\" ADD COLUMN skipped BOOLEAN NOT NULL DEFAULT FALSE;");
        }

        _logger.LogDebug("Sync history table initialized");
    }

    public async Task RecordSyncAsync(SyncHistory history)
    {
        var sql = $@"
            INSERT INTO ""{TableName}"" (
                run_id, profile_name, source_table, target_table,
                sync_start_time, sync_end_time, success,
                {(_hasSkippedColumn ? "skipped," : "")}
                rows_processed, rows_inserted, rows_updated, rows_deleted,
                error_message, max_source_timestamp, duration_seconds,
                recent_rows_count, total_source_rows
            ) VALUES (
                @RunId, @ProfileName, @SourceTable, @TargetTable,
                @SyncStartTime, @SyncEndTime, @Success,
                {(_hasSkippedColumn ? "@Skipped," : "")}
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
            history.Skipped,
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
            history.Skipped ? "SKIPPED" : history.Success ? "SUCCESS" : "FAILED");
    }

    /// <summary>
    /// Back-fill rows_deleted onto the phase-1 history row. See ISyncHistoryRepository. AIM #1964.
    /// </summary>
    public async Task UpdateDeleteCountAsync(Guid runId, string sourceTable, long rowsDeleted)
    {
        const string sql = $@"
            UPDATE ""{TableName}""
            SET rows_deleted = @rowsDeleted
            WHERE run_id = @runId AND source_table = @sourceTable";

        try
        {
            await using var connection = new NpgsqlConnection(_connectionString);
            var updated = await connection.ExecuteAsync(sql, new { runId, sourceTable, rowsDeleted });

            if (updated == 0)
            {
                _logger.LogWarning(
                    "No history row to annotate with {Rows:N0} deletes for {Table} (run {RunId}) - " +
                    "the delete count is correct in the run summary but absent from _sync_history",
                    rowsDeleted, sourceTable, runId);
            }
        }
        catch (Exception ex)
        {
            // Never fail a sync that already moved data just because history could not be annotated.
            _logger.LogWarning(ex,
                "Failed to record {Rows:N0} deletes for {Table} in sync history",
                rowsDeleted, sourceTable);
        }
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
        var sql = $@"
            SELECT
                id AS Id,
                run_id AS RunId,
                profile_name AS ProfileName,
                source_table AS SourceTable,
                target_table AS TargetTable,
                sync_start_time AS SyncStartTime,
                sync_end_time AS SyncEndTime,
                success AS Success,
                {(_hasSkippedColumn ? "skipped" : "FALSE")} AS Skipped,
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
        var sql = $@"
            SELECT
                id AS Id,
                run_id AS RunId,
                profile_name AS ProfileName,
                source_table AS SourceTable,
                target_table AS TargetTable,
                sync_start_time AS SyncStartTime,
                sync_end_time AS SyncEndTime,
                success AS Success,
                {(_hasSkippedColumn ? "skipped" : "FALSE")} AS Skipped,
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

    /// <summary>
    /// Rename a profile in sync history (migrate old records to new profile ID)
    /// </summary>
    public async Task<int> RenameProfileAsync(string oldProfileName, string newProfileName)
    {
        const string sql = $@"
            UPDATE ""{TableName}""
            SET profile_name = @newProfileName
            WHERE profile_name = @oldProfileName";

        await using var connection = new NpgsqlConnection(_connectionString);
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
        const string sql = $@"
            SELECT DISTINCT profile_name
            FROM ""{TableName}""
            ORDER BY profile_name";

        await using var connection = new NpgsqlConnection(_connectionString);
        var results = await connection.QueryAsync<string>(sql);
        return results.ToList();
    }
}
