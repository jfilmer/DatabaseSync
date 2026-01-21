using Dapper;
using DatabaseSync.Configuration;
using DatabaseSync.Enums;
using DatabaseSync.Models;
using DatabaseSync.PostgreSql;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DatabaseSync.Services;

/// <summary>
/// High-performance bulk data copier for PostgreSQL to PostgreSQL sync
/// Uses staging tables and COPY protocol for high performance
/// </summary>
public class PostgreSqlBulkDataCopier
{
    private readonly string _sourceConnectionString;
    private readonly string _targetConnectionString;
    private readonly ILogger<PostgreSqlBulkDataCopier> _logger;
    private readonly int _commandTimeout;

    /// <summary>
    /// Optional callback for progress updates (rowsProcessed)
    /// Called every 100,000 rows during bulk load
    /// </summary>
    public Action<long>? ProgressCallback { get; set; }

    /// <summary>
    /// Batch size for reading rows from source database.
    /// When set > 0, source data is read in batches using LIMIT/OFFSET.
    /// Set to 0 to disable batching.
    /// Default: 100000 (100K rows per batch)
    /// </summary>
    public int SourceBatchSize { get; set; } = 100000;

    public PostgreSqlBulkDataCopier(
        string sourceConnectionString,
        string targetConnectionString,
        ILogger<PostgreSqlBulkDataCopier> logger,
        int commandTimeout = 3600)
    {
        _sourceConnectionString = sourceConnectionString;
        _targetConnectionString = targetConnectionString;
        _logger = logger;
        _commandTimeout = commandTimeout;
    }

    /// <summary>
    /// Parse schema and table name from a potentially schema-qualified table name
    /// </summary>
    private (string schema, string table) ParseTableName(string tableName)
    {
        if (tableName.Contains('.'))
        {
            var parts = tableName.Split('.', 2);
            return (parts[0], parts[1]);
        }
        return ("public", tableName);
    }

    /// <summary>
    /// Format table name for SQL queries with proper schema qualification
    /// </summary>
    private string FormatTableNameForSql(string tableName)
    {
        var (schema, table) = ParseTableName(tableName);
        return $"\"{schema}\".\"{table}\"";
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

        // Include ALL columns for insert (including identity columns to preserve source values)
        var insertColumns = columns.ToList();
        var updateColumns = columns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}";
        if (stagingTableName.Length > 63) stagingTableName = stagingTableName[..63];

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        try
        {
            // Create staging table
            _logger.LogDebug("Creating staging table {Staging}", stagingTableName);
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName);

            // Build source query
            var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
            var formattedSourceTable = FormatTableNameForSql(sourceTableName);
            var sourceQuery = $"SELECT {sourceColumnList} FROM {formattedSourceTable}";

            if (!string.IsNullOrEmpty(config.SourceFilter))
            {
                sourceQuery += $" WHERE {config.SourceFilter}";
            }

            // Bulk load to staging
            _logger.LogInformation("Loading data from PostgreSQL source to staging table...");
            var targetColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, targetColumnList, insertColumns);

            _logger.LogInformation("Loaded {Rows:N0} rows to staging table", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            // Get count before upsert
            var formattedTargetTable = FormatTableNameForSql(targetTableName);
            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {formattedTargetTable}");

            // Execute upsert
            _logger.LogInformation("Executing upsert to target table...");
            await ExecuteUpsertAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns);

            // Calculate stats
            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {formattedTargetTable}");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

            // Reset sequences for identity columns to prevent future conflicts
            await ResetSequencesAsync(targetConn, targetTableName, columns);

            // Handle synchronized deletes (unless skipDelete is true for two-phase sync)
            if (config.DeleteMode == DeleteMode.Sync && !skipDelete)
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
    /// Perform delete sync only - delete rows from target that don't exist in source.
    /// Used for two-phase sync where deletes run in reverse priority order.
    /// </summary>
    public async Task<long> PerformDeleteSyncAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        string? sourceFilter)
    {
        var pkColumns = columns.Where(c => c.IsPrimaryKey).ToList();
        if (!pkColumns.Any())
        {
            _logger.LogWarning("Table {Table} has no primary key. Cannot perform delete sync.", sourceTableName);
            return 0;
        }

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        return await SyncDeletesAsync(sourceConn, targetConn, sourceTableName, targetTableName, pkColumns, sourceFilter);
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

        // Include ALL columns for insert (including identity columns to preserve source values)
        var insertColumns = columns.ToList();
        var updateColumns = columns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}";
        if (stagingTableName.Length > 63) stagingTableName = stagingTableName[..63];

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        try
        {
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName);

            // Build incremental query
            var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
            var timestampCol = config.TimestampColumn!.ToLower();

            // Use COALESCE if FallbackTimestampColumn is specified
            string timestampExpression;
            if (!string.IsNullOrEmpty(config.FallbackTimestampColumn))
            {
                var fallbackCol = config.FallbackTimestampColumn.ToLower();
                timestampExpression = $"COALESCE(\"{timestampCol}\", \"{fallbackCol}\")";
            }
            else
            {
                timestampExpression = $"\"{timestampCol}\"";
            }

            var whereClause = $"{timestampExpression} > @lastSyncTime";

            if (!string.IsNullOrEmpty(config.SourceFilter))
            {
                whereClause += $" AND ({config.SourceFilter})";
            }

            var formattedSourceTable = FormatTableNameForSql(sourceTableName);
            var sourceQuery = $"SELECT {sourceColumnList} FROM {formattedSourceTable} WHERE {whereClause}";

            _logger.LogInformation("Loading rows changed since {LastSync}...", lastSyncTime);

            var targetColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, targetColumnList,
                insertColumns, new { lastSyncTime });

            _logger.LogInformation("Found {Rows:N0} changed rows", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            var formattedTargetTable = FormatTableNameForSql(targetTableName);
            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {formattedTargetTable}");

            await ExecuteUpsertAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns);

            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {formattedTargetTable}");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

            // Reset sequences for identity columns to prevent future conflicts
            await ResetSequencesAsync(targetConn, targetTableName, columns);

            // Handle synchronized deletes (full PK comparison)
            // For incremental sync, only perform deletes if SyncAllDeletes is enabled
            // Skip if skipDelete is true (for two-phase sync where deletes run separately)
            if (config.DeleteMode == DeleteMode.Sync && config.SyncAllDeletes && !skipDelete)
            {
                result.RowsDeleted = await SyncDeletesAsync(
                    sourceConn, targetConn, sourceTableName, targetTableName, pkColumns, config.SourceFilter);
            }
            else if (config.DeleteMode == DeleteMode.Sync && !config.SyncAllDeletes && !skipDelete)
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
        var formattedTargetTable = FormatTableNameForSql(targetTableName);
        await conn.ExecuteAsync($@"
            CREATE TEMP TABLE ""{stagingTableName}""
            (LIKE {formattedTargetTable} INCLUDING DEFAULTS)",
            commandTimeout: _commandTimeout);
    }

    private async Task<long> BulkLoadToStagingAsync(
        NpgsqlConnection sourceConn,
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

        // Use text mode COPY for better type compatibility between PostgreSQL versions
        await using var writer = await targetConn.BeginTextImportAsync(
            $"COPY \"{stagingTableName}\" ({targetColumnList}) FROM STDIN (FORMAT TEXT, NULL '\\N')");

        while (await reader.ReadAsync())
        {
            var values = new string[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                var value = reader.GetValue(i);

                if (value == DBNull.Value || value == null)
                {
                    values[i] = "\\N";  // NULL representation in COPY text format
                }
                else if (value is DateTime dt)
                {
                    // Format timestamp for PostgreSQL
                    values[i] = dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                }
                else if (value is DateTimeOffset dto)
                {
                    // Format timestamptz for PostgreSQL
                    values[i] = dto.ToString("yyyy-MM-dd HH:mm:ss.ffffffzzz");
                }
                else if (value is bool b)
                {
                    values[i] = b ? "t" : "f";
                }
                else if (value is byte[] bytes)
                {
                    // Bytea in hex format
                    values[i] = "\\\\x" + BitConverter.ToString(bytes).Replace("-", "");
                }
                else if (value is System.Text.Json.JsonDocument jsonDoc)
                {
                    // Handle JSONB columns
                    var strValue = jsonDoc.RootElement.GetRawText();
                    strValue = strValue.Replace("\\", "\\\\")
                                      .Replace("\t", "\\t")
                                      .Replace("\n", "\\n")
                                      .Replace("\r", "\\r");
                    values[i] = strValue;
                }
                else if (value.GetType().FullName?.Contains("NpgsqlTsVector") == true ||
                         value.GetType().Name == "NpgsqlTsVector")
                {
                    // Handle tsvector - use ToString() which returns proper tsvector format
                    values[i] = value.ToString() ?? "";
                }
                else if (value.GetType().FullName?.Contains("Npgsql") == true ||
                         value.GetType().FullName?.Contains("Json") == true)
                {
                    // Handle other JSON/Npgsql types by converting to string
                    var strValue = System.Text.Json.JsonSerializer.Serialize(value);
                    strValue = strValue.Replace("\\", "\\\\")
                                      .Replace("\t", "\\t")
                                      .Replace("\n", "\\n")
                                      .Replace("\r", "\\r");
                    values[i] = strValue;
                }
                else
                {
                    // Escape special characters for COPY text format
                    var strValue = value.ToString() ?? "";
                    strValue = strValue.Replace("\\", "\\\\")
                                      .Replace("\t", "\\t")
                                      .Replace("\n", "\\n")
                                      .Replace("\r", "\\r");
                    values[i] = strValue;
                }
            }

            await writer.WriteLineAsync(string.Join("\t", values));
            rowsLoaded++;

            if (rowsLoaded % 100000 == 0)
            {
                _logger.LogDebug("Loaded {Rows:N0} rows to staging...", rowsLoaded);
                ProgressCallback?.Invoke(rowsLoaded);
            }
        }

        return rowsLoaded;
    }

    private async Task ExecuteUpsertAsync(
        NpgsqlConnection conn,
        string stagingTable,
        string targetTable,
        List<ColumnInfo> insertColumns,
        List<ColumnInfo> updateColumns,
        List<ColumnInfo> pkColumns)
    {
        var insertColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
        var pkColumnList = string.Join(", ", pkColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
        var formattedTargetTable = FormatTableNameForSql(targetTable);

        string upsertSql;

        if (updateColumns.Any())
        {
            var updateSetClause = string.Join(", ",
                updateColumns.Select(c => $"\"{c.ColumnName.ToLower()}\" = EXCLUDED.\"{c.ColumnName.ToLower()}\""));

            upsertSql = $@"
                INSERT INTO {formattedTargetTable} ({insertColumnList})
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO UPDATE SET {updateSetClause}";
        }
        else
        {
            upsertSql = $@"
                INSERT INTO {formattedTargetTable} ({insertColumnList})
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO NOTHING";
        }

        await conn.ExecuteAsync(upsertSql, commandTimeout: _commandTimeout);
    }

    /// <summary>
    /// Reset sequences for identity columns to prevent future ID conflicts
    /// </summary>
    private async Task ResetSequencesAsync(
        NpgsqlConnection conn,
        string tableName,
        List<ColumnInfo> columns)
    {
        var (schema, table) = ParseTableName(tableName);
        var identityColumns = columns.Where(c => c.IsIdentity).ToList();

        if (!identityColumns.Any())
            return;

        foreach (var column in identityColumns)
        {
            try
            {
                // Query pg_get_serial_sequence to get the actual sequence name
                var sequenceName = await conn.ExecuteScalarAsync<string>(
                    "SELECT pg_get_serial_sequence(@tableName, @columnName)",
                    new { tableName = $"{schema}.{table}", columnName = column.ColumnName.ToLower() },
                    commandTimeout: _commandTimeout);

                if (!string.IsNullOrEmpty(sequenceName))
                {
                    // Get max value from table
                    var formattedTableName = FormatTableNameForSql(tableName);
                    var maxValueSql = $"SELECT COALESCE(MAX(\"{column.ColumnName.ToLower()}\"), 0) FROM {formattedTableName}";
                    var maxValue = await conn.ExecuteScalarAsync<long>(maxValueSql, commandTimeout: _commandTimeout);

                    // Reset sequence to max + 1
                    var resetSql = $"SELECT setval('{sequenceName}', {maxValue + 1}, false)";
                    await conn.ExecuteAsync(resetSql, commandTimeout: _commandTimeout);

                    _logger.LogDebug(
                        "Reset sequence {Sequence} for column {Column} to {Value}",
                        sequenceName, column.ColumnName, maxValue + 1);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to reset sequence for {Table}.{Column}",
                    tableName, column.ColumnName);
                // Don't fail the entire sync if sequence reset fails
            }
        }
    }

    private async Task<long> SyncDeletesAsync(
        NpgsqlConnection sourceConn,
        NpgsqlConnection targetConn,
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> pkColumns,
        string? sourceFilter)
    {
        _logger.LogInformation("Syncing deletes: comparing primary keys...");

        var sourceAnalyzer = new PostgreSqlSchemaAnalyzer(
            _sourceConnectionString,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<PostgreSqlSchemaAnalyzer>.Instance,
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
