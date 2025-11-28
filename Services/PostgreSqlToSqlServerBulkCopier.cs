using System.Data;
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
/// High-performance bulk data copier for PostgreSQL to SQL Server sync
/// Uses staging tables, SqlBulkCopy, and MERGE for upsert
/// </summary>
public class PostgreSqlToSqlServerBulkCopier
{
    private readonly string _sourceConnectionString;
    private readonly string _targetConnectionString;
    private readonly ILogger<PostgreSqlToSqlServerBulkCopier> _logger;
    private readonly int _commandTimeout;

    public PostgreSqlToSqlServerBulkCopier(
        string sourceConnectionString,
        string targetConnectionString,
        ILogger<PostgreSqlToSqlServerBulkCopier> logger,
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

        var stagingTableName = $"#_staging_{targetTableName}_{Guid.NewGuid():N}"[..50];

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new SqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        try
        {
            // Create staging table
            _logger.LogDebug("Creating staging table {Staging}", stagingTableName);
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName, insertColumns);

            // Build source query - use lowercase column names for PostgreSQL
            var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
            var sourceQuery = $"SELECT {sourceColumnList} FROM \"{sourceTableName}\"";

            if (!string.IsNullOrEmpty(config.SourceFilter))
            {
                sourceQuery += $" WHERE {config.SourceFilter}";
            }

            // Bulk load to staging using SqlBulkCopy
            _logger.LogInformation("Loading data from PostgreSQL to staging table...");

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, insertColumns);

            _logger.LogInformation("Loaded {Rows:N0} rows to staging table", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            // Get count before upsert
            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM [{targetTableName}]");

            // Execute upsert using MERGE
            _logger.LogInformation("Executing MERGE upsert to target table...");
            await ExecuteMergeAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns);

            // Calculate stats
            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM [{targetTableName}]");

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
            // Staging table is a temp table (#), so it's automatically dropped when connection closes
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

        var stagingTableName = $"#_staging_{targetTableName}_{Guid.NewGuid():N}"[..50];

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new SqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        try
        {
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName, insertColumns);

            // Build incremental query - PostgreSQL uses lowercase column names
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

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, insertColumns,
                new { lastSyncTime });

            _logger.LogInformation("Found {Rows:N0} changed rows", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            var countBefore = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM [{targetTableName}]");

            await ExecuteMergeAsync(targetConn, stagingTableName, targetTableName,
                insertColumns, updateColumns, pkColumns);

            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM [{targetTableName}]");

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
            // Temp table auto-dropped
        }

        return result;
    }

    private async Task CreateStagingTableAsync(
        SqlConnection conn,
        string stagingTableName,
        string targetTableName,
        List<ColumnInfo> columns)
    {
        // Create temp table with same structure as target (excluding identity)
        var columnDefs = columns.Select(col =>
        {
            var typeDef = GetSqlServerTypeDef(col);
            var nullable = col.IsNullable ? " NULL" : " NOT NULL";
            return $"[{col.ColumnName}] {typeDef}{nullable}";
        });

        var sql = $@"
            CREATE TABLE [{stagingTableName}] (
                {string.Join(",\n                ", columnDefs)}
            )";

        await conn.ExecuteAsync(sql, commandTimeout: _commandTimeout);
    }

    private async Task<long> BulkLoadToStagingAsync(
        NpgsqlConnection sourceConn,
        SqlConnection targetConn,
        string sourceQuery,
        string stagingTableName,
        List<ColumnInfo> columns,
        object? parameters = null)
    {
        long rowsLoaded = 0;

        // Read from PostgreSQL source
        await using var reader = await sourceConn.ExecuteReaderAsync(
            sourceQuery, parameters, commandTimeout: _commandTimeout);

        var dataTable = new DataTable();
        foreach (var col in columns)
        {
            dataTable.Columns.Add(col.ColumnName, GetClrType(col.DataType));
        }

        while (await reader.ReadAsync())
        {
            var row = dataTable.NewRow();
            for (int i = 0; i < columns.Count; i++)
            {
                var value = reader.GetValue(i);
                row[i] = ConvertPostgreSqlValue(value, columns[i].DataType);
            }
            dataTable.Rows.Add(row);
            rowsLoaded++;

            // Batch insert every 100k rows to manage memory
            if (rowsLoaded % 100000 == 0)
            {
                await BulkInsertDataTableAsync(targetConn, stagingTableName, dataTable, columns);
                _logger.LogDebug("Loaded {Rows:N0} rows to staging...", rowsLoaded);
                dataTable.Clear();
            }
        }

        // Insert remaining rows
        if (dataTable.Rows.Count > 0)
        {
            await BulkInsertDataTableAsync(targetConn, stagingTableName, dataTable, columns);
        }

        return rowsLoaded;
    }

    private object ConvertPostgreSqlValue(object value, string sourceType)
    {
        if (value == DBNull.Value || value == null)
            return DBNull.Value;

        var typeLower = sourceType.ToLower();

        // Handle PostgreSQL boolean to SQL Server bit
        if (typeLower == "boolean" || typeLower == "bool")
        {
            return Convert.ToBoolean(value);
        }

        // Handle UUID to uniqueidentifier
        if (typeLower == "uuid" && value is Guid)
        {
            return value;
        }

        return value;
    }

    private async Task BulkInsertDataTableAsync(
        SqlConnection targetConn,
        string stagingTableName,
        DataTable dataTable,
        List<ColumnInfo> columns)
    {
        using var bulkCopy = new SqlBulkCopy(targetConn)
        {
            DestinationTableName = stagingTableName,
            BatchSize = 10000,
            BulkCopyTimeout = _commandTimeout
        };

        // Map columns
        foreach (var col in columns)
        {
            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        }

        await bulkCopy.WriteToServerAsync(dataTable);
    }

    private async Task ExecuteMergeAsync(
        SqlConnection conn,
        string stagingTable,
        string targetTable,
        List<ColumnInfo> insertColumns,
        List<ColumnInfo> updateColumns,
        List<ColumnInfo> pkColumns)
    {
        var insertColumnList = string.Join(", ", insertColumns.Select(c => $"[{c.ColumnName}]"));
        var sourceColumnList = string.Join(", ", insertColumns.Select(c => $"s.[{c.ColumnName}]"));

        // Build ON clause for matching
        var matchCondition = string.Join(" AND ",
            pkColumns.Select(c => $"t.[{c.ColumnName}] = s.[{c.ColumnName}]"));

        string mergeSql;

        if (updateColumns.Any())
        {
            var updateSetClause = string.Join(", ",
                updateColumns.Select(c => $"t.[{c.ColumnName}] = s.[{c.ColumnName}]"));

            mergeSql = $@"
                MERGE INTO [{targetTable}] AS t
                USING [{stagingTable}] AS s
                ON {matchCondition}
                WHEN MATCHED THEN
                    UPDATE SET {updateSetClause}
                WHEN NOT MATCHED THEN
                    INSERT ({insertColumnList})
                    VALUES ({sourceColumnList});";
        }
        else
        {
            mergeSql = $@"
                MERGE INTO [{targetTable}] AS t
                USING [{stagingTable}] AS s
                ON {matchCondition}
                WHEN NOT MATCHED THEN
                    INSERT ({insertColumnList})
                    VALUES ({sourceColumnList});";
        }

        await conn.ExecuteAsync(mergeSql, commandTimeout: _commandTimeout);
    }

    private async Task<long> SyncDeletesAsync(
        NpgsqlConnection sourceConn,
        SqlConnection targetConn,
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

        var targetAnalyzer = new SqlServerTargetAnalyzer(
            _targetConnectionString,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<SqlServerTargetAnalyzer>.Instance,
            _commandTimeout);

        var targetPks = await targetAnalyzer.GetPrimaryKeyValuesAsync(targetTableName, pkColumns);
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

    private string GetSqlServerTypeDef(ColumnInfo col)
    {
        var dataType = col.MappedDataType?.ToLower() ?? col.DataType.ToLower();

        return dataType switch
        {
            "varchar" or "nvarchar" when col.MaxLength == -1 => $"{dataType}(MAX)",
            "varchar" or "nvarchar" when col.MaxLength > 0 => $"{dataType}({col.MaxLength})",
            "char" or "nchar" when col.MaxLength > 0 => $"{dataType}({col.MaxLength})",
            "decimal" or "numeric" when col.Precision > 0 => $"{dataType}({col.Precision},{col.Scale ?? 0})",
            "varbinary" when col.MaxLength == -1 => "varbinary(MAX)",
            "varbinary" when col.MaxLength > 0 => $"varbinary({col.MaxLength})",
            _ => dataType
        };
    }

    private Type GetClrType(string sqlType)
    {
        var baseType = sqlType.ToLower();
        if (baseType.Contains('('))
            baseType = baseType.Substring(0, baseType.IndexOf('('));

        return baseType switch
        {
            "bigint" or "bigserial" => typeof(long),
            "integer" or "int" or "serial" => typeof(int),
            "smallint" or "smallserial" => typeof(short),
            "boolean" or "bool" => typeof(bool),
            "numeric" or "decimal" or "money" => typeof(decimal),
            "double precision" or "float8" => typeof(double),
            "real" or "float4" => typeof(float),
            "date" or "timestamp" or "timestamp without time zone" => typeof(DateTime),
            "timestamp with time zone" or "timestamptz" => typeof(DateTimeOffset),
            "time" or "time without time zone" => typeof(TimeSpan),
            "uuid" => typeof(Guid),
            "bytea" => typeof(byte[]),
            _ => typeof(string)
        };
    }
}
