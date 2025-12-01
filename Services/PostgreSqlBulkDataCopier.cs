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
    /// Perform a bulk upsert: insert new rows, update existing rows
    /// </summary>
    public async Task<BulkCopyResult> BulkUpsertAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        TableConfig config)
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

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}"[..63];

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
            var sourceQuery = $"SELECT {sourceColumnList} FROM \"{sourceTableName}\"";

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
            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            // Execute upsert
            _logger.LogInformation("Executing upsert to target table...");
            await ExecuteUpsertAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns);

            // Calculate stats
            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

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
    public async Task<BulkCopyResult> IncrementalUpsertAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        TableConfig config,
        DateTime lastSyncTime)
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

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}"[..63];

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

            var sourceQuery = $"SELECT {sourceColumnList} FROM \"{sourceTableName}\" WHERE {whereClause}";

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

            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            await ExecuteUpsertAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns);

            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM \"{targetTableName}\"");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

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

        await using var writer = await targetConn.BeginBinaryImportAsync(
            $"COPY \"{stagingTableName}\" ({targetColumnList}) FROM STDIN (FORMAT BINARY)");

        while (await reader.ReadAsync())
        {
            await writer.StartRowAsync();

            for (int i = 0; i < columns.Count; i++)
            {
                var value = reader.GetValue(i);

                if (value == DBNull.Value || value == null)
                {
                    await writer.WriteNullAsync();
                }
                else
                {
                    await writer.WriteAsync(value);
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
        List<ColumnInfo> pkColumns)
    {
        var insertColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
        var pkColumnList = string.Join(", ", pkColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));

        string upsertSql;

        if (updateColumns.Any())
        {
            var updateSetClause = string.Join(", ",
                updateColumns.Select(c => $"\"{c.ColumnName.ToLower()}\" = EXCLUDED.\"{c.ColumnName.ToLower()}\""));

            upsertSql = $@"
                INSERT INTO ""{targetTable}"" ({insertColumnList})
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO UPDATE SET {updateSetClause}";
        }
        else
        {
            upsertSql = $@"
                INSERT INTO ""{targetTable}"" ({insertColumnList})
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO NOTHING";
        }

        await conn.ExecuteAsync(upsertSql, commandTimeout: _commandTimeout);
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
