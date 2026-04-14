using Dapper;
using DatabaseSync.Configuration;
using DatabaseSync.Enums;
using DatabaseSync.Models;
using DatabaseSync.PostgreSql;
using DatabaseSync.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DatabaseSync.Services;

/// <summary>
/// High-performance bulk data copier using staging tables and UPSERT
/// </summary>
public class BulkDataCopier
{
    private readonly string _sourceConnectionString;
    private readonly string _targetConnectionString;
    private readonly TypeMapper _typeMapper;
    private readonly ILogger<BulkDataCopier> _logger;
    private readonly int _commandTimeout;

    /// <summary>
    /// Optional callback for progress updates (rowsProcessed)
    /// Called every 100,000 rows during bulk load
    /// </summary>
    public Action<long>? ProgressCallback { get; set; }

    /// <summary>
    /// Use WITH (NOLOCK) hint on SQL Server source queries to reduce blocking.
    /// Default: true
    /// </summary>
    public bool UseNoLock { get; set; } = true;

    /// <summary>
    /// Batch size for reading rows from source database.
    /// When set > 0, source data is read in batches using OFFSET/FETCH.
    /// Set to 0 to disable batching.
    /// Default: 100000 (100K rows per batch)
    /// </summary>
    public int SourceBatchSize { get; set; } = 100000;

    /// <summary>
    /// Maximum table row count for automatic unique constraint violation recovery.
    /// If a unique constraint violation occurs and the table has fewer rows than this,
    /// the service will attempt to delete conflicting rows and retry.
    /// Set to 0 to disable automatic recovery.
    /// Default: 300000 (300K rows)
    /// </summary>
    public int MaxRowsForConstraintRecovery { get; set; } = 300000;

    public BulkDataCopier(
        string sourceConnectionString,
        string targetConnectionString,
        TypeMapper typeMapper,
        ILogger<BulkDataCopier> logger,
        int commandTimeout = 3600)
    {
        _sourceConnectionString = sourceConnectionString;
        _targetConnectionString = targetConnectionString;
        _typeMapper = typeMapper;
        _logger = logger;
        _commandTimeout = commandTimeout;
    }

    /// <summary>
    /// Perform a bulk upsert: insert new rows, update existing rows
    /// </summary>
    /// <param name="skipDelete">If true, skip the delete phase (for two-phase sync where deletes run separately)</param>
    public async Task<BulkCopyResult> BulkUpsertAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        TableConfig config,
        bool skipDelete = false)
    {
        var result = new BulkCopyResult();
        
        var pkColumns = columns.Where(c => c.IsPrimaryKey).ToList();
        if (!pkColumns.Any())
        {
            throw new InvalidOperationException(
                $"Table {sourceTableName} has no primary key. Cannot perform upsert.");
        }

        var insertColumns = columns.Where(c => !c.IsIdentity).ToList();
        var updateColumns = columns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}";
        if (stagingTableName.Length > 63) stagingTableName = stagingTableName[..63];

        await using var sourceConn = new SqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        // Set timezone to UTC for consistent timestamp handling
        await SetTimezoneUtcAsync(targetConn);

        try
        {
            // Create staging table
            _logger.LogDebug("Creating staging table {Staging}", stagingTableName);
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName);

            // Build source query with optional NOLOCK hint
            var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"[{c.ColumnName}]"));
            var noLockHint = UseNoLock ? " WITH (NOLOCK)" : "";
            var sourceQuery = $"SELECT {sourceColumnList} FROM [{sourceTableName}]{noLockHint}";

            if (!string.IsNullOrEmpty(config.SourceFilter))
            {
                sourceQuery += $" WHERE {config.SourceFilter}";
            }

            // Bulk load to staging
            _logger.LogInformation("Loading data from SQL Server to staging table{NoLock}...",
                UseNoLock ? " (NOLOCK)" : "");
            var targetColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, targetColumnList, insertColumns);

            _logger.LogInformation("Loaded {Rows:N0} rows to staging table", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            // Get count before upsert
            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            // Execute upsert
            _logger.LogInformation("Executing upsert to target table...");
            await ExecuteUpsertAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns, result.RowsProcessed);

            // Calculate stats
            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

            // Reset sequences to prevent future ID conflicts
            await ResetSequencesAsync(targetConn, targetTableName);

            // Handle synchronized deletes
            if (config.DeleteMode == DeleteMode.Sync)
            {
                result.RowsDeleted = await SyncDeletesAsync(
                    sourceConn, targetConn, sourceTableName, targetTableName, pkColumns, config.SourceFilter);
            }

            _logger.LogInformation(
                "Upsert complete: {Inserted:N0} inserted, {Updated:N0} updated, {Deleted:N0} deleted",
                result.RowsInserted, result.RowsUpdated, result.RowsDeleted);
        }
        finally
        {
            try
            {
                await targetConn.ExecuteAsync($"DROP TABLE IF EXISTS \"{stagingTableName}\"");
            }
            catch { /* Ignore cleanup errors */ }
        }

        return result;
    }

    /// <summary>
    /// Perform incremental upsert - only rows changed since last sync
    /// </summary>
    /// <param name="skipDelete">If true, skip the delete phase (for two-phase sync where deletes run separately)</param>
    public async Task<BulkCopyResult> IncrementalUpsertAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        TableConfig config,
        DateTime lastSyncTime,
        bool skipDelete = false)
    {
        var result = new BulkCopyResult();

        var pkColumns = columns.Where(c => c.IsPrimaryKey).ToList();
        if (!pkColumns.Any())
        {
            throw new InvalidOperationException(
                $"Table {sourceTableName} has no primary key. Cannot perform upsert.");
        }

        var insertColumns = columns.Where(c => !c.IsIdentity).ToList();
        var updateColumns = columns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}";
        if (stagingTableName.Length > 63) stagingTableName = stagingTableName[..63];

        await using var sourceConn = new SqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        // Set timezone to UTC for consistent timestamp handling
        await SetTimezoneUtcAsync(targetConn);

        try
        {
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName);

            // Build incremental query with optional NOLOCK hint
            var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"[{c.ColumnName}]"));
            var noLockHint = UseNoLock ? " WITH (NOLOCK)" : "";

            // Use COALESCE if FallbackTimestampColumn is specified
            string timestampExpression;
            if (!string.IsNullOrEmpty(config.FallbackTimestampColumn))
            {
                timestampExpression = $"COALESCE([{config.TimestampColumn}], [{config.FallbackTimestampColumn}])";
            }
            else
            {
                timestampExpression = $"[{config.TimestampColumn}]";
            }

            var whereClause = $"{timestampExpression} > @lastSyncTime";

            if (!string.IsNullOrEmpty(config.SourceFilter))
            {
                whereClause += $" AND ({config.SourceFilter})";
            }

            var sourceQuery = $"SELECT {sourceColumnList} FROM [{sourceTableName}]{noLockHint} WHERE {whereClause}";

            _logger.LogInformation("Loading rows changed since {LastSync}{NoLock}...",
                lastSyncTime, UseNoLock ? " (NOLOCK)" : "");

            var targetColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
            
            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, targetColumnList, 
                insertColumns, new { lastSyncTime });

            _logger.LogInformation("Found {Rows:N0} changed rows", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            await ExecuteUpsertAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns, result.RowsProcessed);

            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

            // Reset sequences to prevent future ID conflicts
            await ResetSequencesAsync(targetConn, targetTableName);

            // Handle synchronized deletes (full PK comparison)
            // For incremental sync, only perform deletes if SyncAllDeletes is enabled
            if (config.DeleteMode == DeleteMode.Sync && config.SyncAllDeletes)
            {
                result.RowsDeleted = await SyncDeletesAsync(
                    sourceConn, targetConn, sourceTableName, targetTableName, pkColumns, config.SourceFilter);
            }
            else if (config.DeleteMode == DeleteMode.Sync && !config.SyncAllDeletes)
            {
                _logger.LogDebug("Skipping delete sync for incremental mode (SyncAllDeletes is false)");
            }
        }
        finally
        {
            try
            {
                await targetConn.ExecuteAsync($"DROP TABLE IF EXISTS \"{stagingTableName}\"");
            }
            catch { /* Ignore */ }
        }

        return result;
    }

    private async Task CreateStagingTableAsync(NpgsqlConnection conn, string stagingTableName, string targetTableName)
    {
        await conn.ExecuteAsync($@"
            CREATE TEMP TABLE ""{stagingTableName}""
            (LIKE ""{targetTableName}"" INCLUDING DEFAULTS)
            ON COMMIT DROP",
            commandTimeout: _commandTimeout);
    }

    /// <summary>
    /// Set session timezone to UTC for consistent timestamp handling.
    /// This ensures timestamptz values are written consistently regardless of
    /// the target server's default timezone setting.
    /// </summary>
    private async Task SetTimezoneUtcAsync(NpgsqlConnection conn)
    {
        await conn.ExecuteAsync("SET TIME ZONE 'UTC'", commandTimeout: _commandTimeout);
        _logger.LogDebug("PostgreSQL session timezone set to UTC");
    }

    private async Task<long> BulkLoadToStagingAsync(
        SqlConnection sourceConn,
        NpgsqlConnection targetConn,
        string sourceQuery,
        string stagingTableName,
        string targetColumnList,
        List<ColumnInfo> columns,
        object? parameters = null)
    {
        long rowsLoaded = 0;

        await using var reader = await sourceConn.ExecuteReaderAsync(
            sourceQuery, parameters, commandTimeout: _commandTimeout);

        await using var writer = await targetConn.BeginBinaryImportAsync(
            $"COPY \"{stagingTableName}\" ({targetColumnList}) FROM STDIN (FORMAT BINARY)");

        while (await reader.ReadAsync())
        {
            await writer.StartRowAsync();

            for (int i = 0; i < columns.Count; i++)
            {
                var value = reader.GetValue(i);
                var column = columns[i];

                if (value == DBNull.Value || value == null)
                {
                    await writer.WriteNullAsync();
                }
                else
                {
                    try
                    {
                        var converted = _typeMapper.ConvertValue(value, column.DataType, column.MappedDataType);

                        if (converted == DBNull.Value)
                            await writer.WriteNullAsync();
                        else
                            await writer.WriteAsync(converted!);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(
                            "Type conversion error at row {Row}, column {Column}: {Error}",
                            rowsLoaded + 1, column.ColumnName, ex.Message);
                        await writer.WriteNullAsync();
                    }
                }
            }

            rowsLoaded++;

            if (rowsLoaded % 100000 == 0)
            {
                _logger.LogDebug("Loaded {Rows:N0} rows to staging...", rowsLoaded);
                ProgressCallback?.Invoke(rowsLoaded);
            }
        }

        await writer.CompleteAsync();
        return rowsLoaded;
    }

    private async Task ExecuteUpsertAsync(
        NpgsqlConnection conn,
        string stagingTable,
        string targetTable,
        List<ColumnInfo> insertColumns,
        List<ColumnInfo> updateColumns,
        List<ColumnInfo> pkColumns,
        long rowsInStaging = 0)
    {
        var insertColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
        var pkColumnList = string.Join(", ", pkColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));

        string upsertSql = BuildUpsertSql(targetTable, stagingTable, insertColumnList, pkColumnList, updateColumns);

        try
        {
            await conn.ExecuteAsync(upsertSql, commandTimeout: _commandTimeout);
        }
        catch (PostgresException ex) when (ex.SqlState == "23505") // unique_violation
        {
            // Attempt automatic recovery for unique constraint violations
            await HandleUniqueConstraintViolationAsync(
                conn, ex, stagingTable, targetTable,
                insertColumnList, pkColumnList, updateColumns, rowsInStaging);
        }
    }

    private string BuildUpsertSql(
        string targetTable,
        string stagingTable,
        string insertColumnList,
        string pkColumnList,
        List<ColumnInfo> updateColumns)
    {
        if (updateColumns.Any())
        {
            var updateSetClause = string.Join(", ",
                updateColumns.Select(c => $"\"{c.ColumnName.ToLower()}\" = EXCLUDED.\"{c.ColumnName.ToLower()}\""));

            return $@"
                INSERT INTO ""{targetTable}"" ({insertColumnList})
                OVERRIDING SYSTEM VALUE
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO UPDATE SET {updateSetClause}";
        }
        else
        {
            return $@"
                INSERT INTO ""{targetTable}"" ({insertColumnList})
                OVERRIDING SYSTEM VALUE
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO NOTHING";
        }
    }

    /// <summary>
    /// Handle unique constraint violation by identifying the conflicting constraint,
    /// deleting conflicting rows, and retrying the upsert.
    /// </summary>
    private async Task HandleUniqueConstraintViolationAsync(
        NpgsqlConnection conn,
        PostgresException ex,
        string stagingTable,
        string targetTable,
        string insertColumnList,
        string pkColumnList,
        List<ColumnInfo> updateColumns,
        long rowsInStaging)
    {
        var constraintName = ex.ConstraintName;

        if (string.IsNullOrEmpty(constraintName))
        {
            _logger.LogError("Unique constraint violation but constraint name not available. Cannot recover.");
            throw ex;
        }

        _logger.LogWarning(
            "Unique constraint violation on '{Constraint}' for table {Table}. Attempting automatic recovery...",
            constraintName, targetTable);

        // Safety check: only attempt recovery for reasonably-sized tables
        if (MaxRowsForConstraintRecovery > 0)
        {
            var targetRowCount = await conn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTable}\"",
                commandTimeout: _commandTimeout);

            if (targetRowCount > MaxRowsForConstraintRecovery)
            {
                _logger.LogError(
                    "Table {Table} has {RowCount:N0} rows, exceeding recovery threshold of {Threshold:N0}. " +
                    "Automatic recovery disabled for safety. Please resolve the constraint violation manually.",
                    targetTable, targetRowCount, MaxRowsForConstraintRecovery);
                throw ex;
            }
        }

        // Get the columns involved in the violated constraint
        var constraintColumns = await GetConstraintColumnsAsync(conn, targetTable, constraintName);

        if (!constraintColumns.Any())
        {
            _logger.LogError(
                "Could not determine columns for constraint '{Constraint}'. Cannot recover.",
                constraintName);
            throw ex;
        }

        _logger.LogInformation(
            "Constraint '{Constraint}' involves columns: {Columns}",
            constraintName, string.Join(", ", constraintColumns));

        // Delete conflicting rows from target
        var deletedCount = await DeleteConflictingRowsAsync(
            conn, stagingTable, targetTable, constraintColumns);

        _logger.LogInformation(
            "Deleted {Count:N0} conflicting rows from {Table}. Retrying upsert...",
            deletedCount, targetTable);

        // Retry the upsert
        var upsertSql = BuildUpsertSql(targetTable, stagingTable, insertColumnList, pkColumnList, updateColumns);

        try
        {
            await conn.ExecuteAsync(upsertSql, commandTimeout: _commandTimeout);
            _logger.LogInformation("Upsert retry succeeded after constraint recovery");
        }
        catch (PostgresException retryEx) when (retryEx.SqlState == "23505")
        {
            _logger.LogError(
                "Upsert still failing after recovery attempt. Constraint: {Constraint}. " +
                "This may indicate multiple conflicting constraints or data issues.",
                retryEx.ConstraintName);
            throw;
        }
    }

    /// <summary>
    /// Get the column names involved in a unique constraint or index.
    /// </summary>
    private async Task<List<string>> GetConstraintColumnsAsync(
        NpgsqlConnection conn,
        string tableName,
        string constraintName)
    {
        // First try to find it as an index
        var indexQuery = @"
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
            WHERE c.relname = @constraintName
              AND t.relname = @tableName
            ORDER BY array_position(i.indkey, a.attnum)";

        var columns = (await conn.QueryAsync<string>(
            indexQuery,
            new { constraintName, tableName },
            commandTimeout: _commandTimeout)).ToList();

        if (columns.Any())
        {
            return columns;
        }

        // If not found as index, try as a constraint
        var constraintQuery = @"
            SELECT a.attname as column_name
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.conname = @constraintName
              AND t.relname = @tableName
            ORDER BY array_position(c.conkey, a.attnum)";

        columns = (await conn.QueryAsync<string>(
            constraintQuery,
            new { constraintName, tableName },
            commandTimeout: _commandTimeout)).ToList();

        return columns;
    }

    /// <summary>
    /// Delete rows from target table that would conflict with rows in staging
    /// based on the specified constraint columns.
    /// </summary>
    private async Task<long> DeleteConflictingRowsAsync(
        NpgsqlConnection conn,
        string stagingTable,
        string targetTable,
        List<string> constraintColumns)
    {
        // Build the join condition on constraint columns
        var joinCondition = string.Join(" AND ",
            constraintColumns.Select(c => $"t.\"{c.ToLower()}\" = s.\"{c.ToLower()}\""));

        var deleteSql = $@"
            DELETE FROM ""{targetTable}"" t
            USING ""{stagingTable}"" s
            WHERE {joinCondition}";

        var deleted = await conn.ExecuteAsync(deleteSql, commandTimeout: _commandTimeout);
        return deleted;
    }

    /// <summary>
    /// Reset sequences for all columns that have associated sequences on the TARGET database.
    /// Queries the target directly using pg_get_serial_sequence() to find all sequence-backed columns.
    /// </summary>
    private async Task ResetSequencesAsync(NpgsqlConnection conn, string targetTableName)
    {
        try
        {
            // BulkDataCopier targets use unqualified table names (public schema)
            var schema = "public";
            var table = targetTableName;
            var qualifiedName = $"{schema}.{table}";

            // Query target DB for all columns that have associated sequences
            var sql = @"
                SELECT c.column_name, pg_get_serial_sequence(@qualifiedName, c.column_name) AS sequence_name
                FROM information_schema.columns c
                WHERE c.table_schema = @schema AND c.table_name = @table
                  AND pg_get_serial_sequence(@qualifiedName, c.column_name) IS NOT NULL";

            var sequences = (await conn.QueryAsync(sql,
                new { qualifiedName, schema, table },
                commandTimeout: _commandTimeout)).ToList();

            if (!sequences.Any())
                return;

            foreach (var seq in sequences)
            {
                try
                {
                    string columnName = seq.column_name;
                    string sequenceName = seq.sequence_name;

                    // Get max value from table
                    var maxValue = await conn.ExecuteScalarAsync<long>(
                        $"SELECT COALESCE(MAX(\"{columnName}\"), 0) FROM \"{targetTableName}\"",
                        commandTimeout: _commandTimeout);

                    // Reset sequence: setval with false means next nextval() returns maxValue + 1
                    await conn.ExecuteAsync(
                        $"SELECT setval('{sequenceName}', {maxValue + 1}, false)",
                        commandTimeout: _commandTimeout);

                    _logger.LogInformation(
                        "Reset sequence {Sequence} to {Value} for {Table}.{Column}",
                        sequenceName, maxValue + 1, targetTableName, columnName);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "Failed to reset sequence for {Table}.{Column}",
                        targetTableName, (string)seq.column_name);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to query sequences for {Table}", targetTableName);
            // Don't fail the entire sync if sequence reset fails
        }
    }

    /// <summary>
    /// Sync deletes - delete rows from target that don't exist in source
    /// </summary>
    private async Task<long> SyncDeletesAsync(
        SqlConnection sourceConn,
        NpgsqlConnection targetConn,
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> pkColumns,
        string? sourceFilter)
    {
        _logger.LogInformation("Syncing deletes: comparing primary keys...");

        var sourceAnalyzer = new SqlServerSchemaAnalyzer(
            _sourceConnectionString,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlServerSchemaAnalyzer>.Instance,
            _commandTimeout);
        
        var sourcePks = await sourceAnalyzer.GetPrimaryKeyValuesAsync(sourceTableName, pkColumns, sourceFilter);
        _logger.LogDebug("Found {Count:N0} rows in source", sourcePks.Count);

        var targetAnalyzer = new PostgreSqlSchemaAnalyzer(
            _targetConnectionString,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<PostgreSqlSchemaAnalyzer>.Instance,
            _commandTimeout);
        
        var targetPks = await targetAnalyzer.GetPrimaryKeyValuesAsync(targetTableName, pkColumns, null);
        _logger.LogDebug("Found {Count:N0} rows in target", targetPks.Count);

        var pksToDelete = targetPks.Except(sourcePks).ToList();

        if (!pksToDelete.Any())
        {
            _logger.LogDebug("No rows to delete");
            return 0;
        }

        _logger.LogInformation("Deleting {Count:N0} rows from target", pksToDelete.Count);

        return await targetAnalyzer.DeleteByPrimaryKeysAsync(targetTableName, pkColumns, pksToDelete);
    }
}
