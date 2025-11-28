using System.Data;
using Dapper;
using DatabaseSync.Configuration;
using DatabaseSync.Enums;
using DatabaseSync.Models;
using DatabaseSync.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DatabaseSync.Services;

/// <summary>
/// High-performance bulk data copier for SQL Server to SQL Server sync
/// Uses staging tables, SqlBulkCopy, and MERGE for upsert
/// </summary>
public class SqlServerBulkDataCopier
{
    private readonly string _sourceConnectionString;
    private readonly string _targetConnectionString;
    private readonly ILogger<SqlServerBulkDataCopier> _logger;
    private readonly int _commandTimeout;

    public SqlServerBulkDataCopier(
        string sourceConnectionString,
        string targetConnectionString,
        ILogger<SqlServerBulkDataCopier> logger,
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

        // Include ALL columns (including IDENTITY) - we'll use IDENTITY_INSERT to preserve PK values
        var allColumns = columns.ToList();
        var identityColumns = columns.Where(c => c.IsIdentity).ToList();
        var hasIdentity = identityColumns.Any();

        // Log column info for debugging
        _logger.LogDebug("Primary key columns: {PKColumns}", string.Join(", ", pkColumns.Select(c => c.ColumnName)));
        _logger.LogDebug("Identity columns: {IdentityColumns}", string.Join(", ", identityColumns.Select(c => c.ColumnName)));
        _logger.LogDebug("Has identity: {HasIdentity}", hasIdentity);

        // Update columns excludes PK and identity columns
        // SQL Server does NOT allow updating identity columns even with IDENTITY_INSERT ON
        // IDENTITY_INSERT only works for INSERT, not UPDATE
        var updateColumns = columns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"#_staging_{targetTableName}_{Guid.NewGuid():N}"[..50];

        await using var sourceConn = new SqlConnection(_sourceConnectionString);
        await using var targetConn = new SqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        try
        {
            // Create staging table (temp table in SQL Server) - include all columns
            _logger.LogDebug("Creating staging table {Staging}", stagingTableName);
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName, allColumns);

            // Build source query - select all columns including identity
            // For spatial types, use native SQL Server binary format which preserves SRID
            var sourceColumnList = string.Join(", ", allColumns.Select(c =>
                IsSpatialType(c.DataType)
                    ? $"CAST([{c.ColumnName}] AS varbinary(MAX)) AS [{c.ColumnName}]"
                    : $"[{c.ColumnName}]"));
            var sourceQuery = $"SELECT {sourceColumnList} FROM [{sourceTableName}]";

            if (!string.IsNullOrEmpty(config.SourceFilter))
            {
                sourceQuery += $" WHERE {config.SourceFilter}";
            }

            // Bulk load to staging using SqlBulkCopy
            _logger.LogInformation("Loading data from source SQL Server to staging table...");

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, allColumns);

            _logger.LogInformation("Loaded {Rows:N0} rows to staging table", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            // If there are identity columns (non-PK), delete rows where identity values don't match
            // This is required because SQL Server doesn't allow updating identity columns
            long identityMismatchDeletes = 0;
            if (identityColumns.Any())
            {
                identityMismatchDeletes = await DeleteIdentityMismatchRowsAsync(
                    targetConn, stagingTableName, targetTableName, pkColumns, identityColumns);

                if (identityMismatchDeletes > 0)
                {
                    _logger.LogInformation(
                        "Deleted {Count:N0} rows with mismatched identity values (will be re-inserted)",
                        identityMismatchDeletes);
                }
            }

            // Execute upsert using MERGE (with IDENTITY_INSERT if needed)
            _logger.LogInformation("Executing MERGE upsert to target table...");
            var mergeResult = await ExecuteMergeAsync(targetConn, stagingTableName, targetTableName,
                allColumns, updateColumns, pkColumns, hasIdentity);

            // Use actual counts from MERGE OUTPUT
            result.RowsInserted = mergeResult.Inserted;
            result.RowsUpdated = mergeResult.Updated;

            // Handle synchronized deletes using staging table (much faster than loading PKs to memory)
            if (config.DeleteMode == DeleteMode.Sync)
            {
                result.RowsDeleted = await SyncDeletesUsingStagingAsync(
                    targetConn, stagingTableName, targetTableName, pkColumns);
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

        // Include ALL columns (including IDENTITY) - we'll use IDENTITY_INSERT to preserve PK values
        var allColumns = columns.ToList();
        var identityColumns = columns.Where(c => c.IsIdentity).ToList();
        var hasIdentity = identityColumns.Any();

        // Update columns excludes PK and identity columns
        // SQL Server does NOT allow updating identity columns even with IDENTITY_INSERT ON
        var updateColumns = columns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"#_staging_{targetTableName}_{Guid.NewGuid():N}"[..50];

        await using var sourceConn = new SqlConnection(_sourceConnectionString);
        await using var targetConn = new SqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        try
        {
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName, allColumns);

            // Build incremental query - select all columns including identity
            // For spatial types, use native SQL Server binary format which preserves SRID
            var sourceColumnList = string.Join(", ", allColumns.Select(c =>
                IsSpatialType(c.DataType)
                    ? $"CAST([{c.ColumnName}] AS varbinary(MAX)) AS [{c.ColumnName}]"
                    : $"[{c.ColumnName}]"));

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

            var sourceQuery = $"SELECT {sourceColumnList} FROM [{sourceTableName}] WHERE {whereClause}";

            _logger.LogInformation("Loading rows changed since {LastSync}...", lastSyncTime);

            result.RowsProcessed = await BulkLoadToStagingAsync(
                sourceConn, targetConn, sourceQuery, stagingTableName, allColumns,
                new { lastSyncTime });

            _logger.LogInformation("Found {Rows:N0} changed rows", result.RowsProcessed);

            if (result.RowsProcessed == 0)
            {
                return result;
            }

            // If there are identity columns (non-PK), delete rows where identity values don't match
            // This is required because SQL Server doesn't allow updating identity columns
            if (identityColumns.Any())
            {
                var identityMismatchDeletes = await DeleteIdentityMismatchRowsAsync(
                    targetConn, stagingTableName, targetTableName, pkColumns, identityColumns);

                if (identityMismatchDeletes > 0)
                {
                    _logger.LogInformation(
                        "Deleted {Count:N0} rows with mismatched identity values (will be re-inserted)",
                        identityMismatchDeletes);
                }
            }

            var mergeResult = await ExecuteMergeAsync(targetConn, stagingTableName, targetTableName,
                allColumns, updateColumns, pkColumns, hasIdentity);

            // Use actual counts from MERGE OUTPUT
            result.RowsInserted = mergeResult.Inserted;
            result.RowsUpdated = mergeResult.Updated;

            // Handle synchronized deletes using staging table
            if (config.DeleteMode == DeleteMode.Sync)
            {
                result.RowsDeleted = await SyncDeletesUsingStagingAsync(
                    targetConn, stagingTableName, targetTableName, pkColumns);
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
        // For spatial types, use varbinary(MAX) since we're storing WKB (Well-Known Binary)
        var columnDefs = columns.Select(col =>
        {
            var typeDef = IsSpatialType(col.DataType)
                ? "varbinary(MAX)"
                : GetSqlServerTypeDef(col);
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
        SqlConnection sourceConn,
        SqlConnection targetConn,
        string sourceQuery,
        string stagingTableName,
        List<ColumnInfo> columns,
        object? parameters = null)
    {
        long rowsLoaded = 0;

        // Read from source into a DataTable
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
                row[i] = value == DBNull.Value ? DBNull.Value : value;
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

    private async Task<(long Inserted, long Updated)> ExecuteMergeAsync(
        SqlConnection conn,
        string stagingTable,
        string targetTable,
        List<ColumnInfo> insertColumns,
        List<ColumnInfo> updateColumns,
        List<ColumnInfo> pkColumns,
        bool hasIdentity)
    {
        var insertColumnList = string.Join(", ", insertColumns.Select(c => $"[{c.ColumnName}]"));

        // For spatial columns, cast from varbinary back to geometry/geography
        // SQL Server native binary format preserves SRID
        var sourceColumnList = string.Join(", ", insertColumns.Select(c =>
            IsSpatialType(c.DataType)
                ? $"CAST(s.[{c.ColumnName}] AS {c.DataType})"
                : $"s.[{c.ColumnName}]"));

        // Build ON clause for matching
        var matchCondition = string.Join(" AND ",
            pkColumns.Select(c => $"t.[{c.ColumnName}] = s.[{c.ColumnName}]"));

        var sqlBuilder = new System.Text.StringBuilder();

        // Enable IDENTITY_INSERT if the table has an identity column
        if (hasIdentity)
        {
            sqlBuilder.AppendLine($"SET IDENTITY_INSERT [{targetTable}] ON;");
        }

        if (updateColumns.Any())
        {
            // For spatial columns in UPDATE, cast from varbinary back to geometry/geography
            var updateSetClause = string.Join(", ",
                updateColumns.Select(c =>
                    IsSpatialType(c.DataType)
                        ? $"t.[{c.ColumnName}] = CAST(s.[{c.ColumnName}] AS {c.DataType})"
                        : $"t.[{c.ColumnName}] = s.[{c.ColumnName}]"));

            // Build change detection condition - only update if at least one column differs
            // Uses ISNULL/NULLIF pattern to handle NULL comparisons correctly
            // For spatial types, compare the binary representation in staging
            var changeConditions = updateColumns.Select(c =>
            {
                if (IsSpatialType(c.DataType))
                {
                    // Compare spatial as binary - cast target to varbinary for comparison
                    return $"ISNULL(NULLIF(CAST(t.[{c.ColumnName}] AS varbinary(MAX)), s.[{c.ColumnName}]), NULLIF(s.[{c.ColumnName}], CAST(t.[{c.ColumnName}] AS varbinary(MAX)))) IS NOT NULL";
                }
                else
                {
                    // Standard comparison with NULL handling
                    return $"ISNULL(NULLIF(t.[{c.ColumnName}], s.[{c.ColumnName}]), NULLIF(s.[{c.ColumnName}], t.[{c.ColumnName}])) IS NOT NULL";
                }
            });
            var changeDetectionClause = string.Join(" OR ", changeConditions);

            // Use OUTPUT clause to capture actual INSERT/UPDATE counts
            sqlBuilder.AppendLine($@"
                DECLARE @MergeOutput TABLE (Action NVARCHAR(10));

                MERGE INTO [{targetTable}] AS t
                USING [{stagingTable}] AS s
                ON {matchCondition}
                WHEN MATCHED AND ({changeDetectionClause}) THEN
                    UPDATE SET {updateSetClause}
                WHEN NOT MATCHED THEN
                    INSERT ({insertColumnList})
                    VALUES ({sourceColumnList})
                OUTPUT $action INTO @MergeOutput;

                SELECT
                    SUM(CASE WHEN Action = 'INSERT' THEN 1 ELSE 0 END) AS Inserted,
                    SUM(CASE WHEN Action = 'UPDATE' THEN 1 ELSE 0 END) AS Updated
                FROM @MergeOutput;");
        }
        else
        {
            // No columns to update (only PK columns) - just insert if not exists
            sqlBuilder.AppendLine($@"
                DECLARE @MergeOutput TABLE (Action NVARCHAR(10));

                MERGE INTO [{targetTable}] AS t
                USING [{stagingTable}] AS s
                ON {matchCondition}
                WHEN NOT MATCHED THEN
                    INSERT ({insertColumnList})
                    VALUES ({sourceColumnList})
                OUTPUT $action INTO @MergeOutput;

                SELECT
                    SUM(CASE WHEN Action = 'INSERT' THEN 1 ELSE 0 END) AS Inserted,
                    SUM(CASE WHEN Action = 'UPDATE' THEN 1 ELSE 0 END) AS Updated
                FROM @MergeOutput;");
        }

        // Disable IDENTITY_INSERT after the merge
        if (hasIdentity)
        {
            sqlBuilder.AppendLine($"SET IDENTITY_INSERT [{targetTable}] OFF;");
        }

        var result = await conn.QueryFirstOrDefaultAsync<(long Inserted, long Updated)>(
            sqlBuilder.ToString(), commandTimeout: _commandTimeout);
        return result;
    }

    /// <summary>
    /// Delete rows from target where identity column values don't match source values.
    /// This is necessary because SQL Server doesn't allow updating identity columns.
    /// After deletion, the MERGE will re-insert these rows with correct identity values.
    /// </summary>
    private async Task<long> DeleteIdentityMismatchRowsAsync(
        SqlConnection conn,
        string stagingTable,
        string targetTable,
        List<ColumnInfo> pkColumns,
        List<ColumnInfo> identityColumns)
    {
        // Build the join condition on PK columns
        var pkJoinCondition = string.Join(" AND ",
            pkColumns.Select(c => $"t.[{c.ColumnName}] = s.[{c.ColumnName}]"));

        // Build the mismatch condition on identity columns
        // We want to delete rows where ANY identity column doesn't match
        var identityMismatchCondition = string.Join(" OR ",
            identityColumns.Select(c => $"t.[{c.ColumnName}] <> s.[{c.ColumnName}]"));

        // Delete from target where PK matches but identity values don't match
        var sql = $@"
            DELETE t
            FROM [{targetTable}] t
            INNER JOIN [{stagingTable}] s ON {pkJoinCondition}
            WHERE {identityMismatchCondition}";

        var rowsDeleted = await conn.ExecuteAsync(sql, commandTimeout: _commandTimeout);
        return rowsDeleted;
    }

    /// <summary>
    /// Sync deletes using staging table - delete rows from target that don't exist in staging (source).
    /// This is much faster than loading all PKs into memory and comparing in the application.
    /// Uses a single SQL DELETE with LEFT JOIN instead of thousands of batched deletes.
    /// </summary>
    private async Task<long> SyncDeletesUsingStagingAsync(
        SqlConnection conn,
        string stagingTableName,
        string targetTableName,
        List<ColumnInfo> pkColumns)
    {
        _logger.LogInformation("Syncing deletes: comparing with staging table...");

        // Build the join condition on PK columns
        var joinCondition = string.Join(" AND ",
            pkColumns.Select(c => $"t.[{c.ColumnName}] = s.[{c.ColumnName}]"));

        // Count rows to delete first (for logging)
        var countSql = $@"
            SELECT COUNT(*)
            FROM [{targetTableName}] t
            LEFT JOIN [{stagingTableName}] s ON {joinCondition}
            WHERE s.[{pkColumns[0].ColumnName}] IS NULL";

        var deleteCount = await conn.ExecuteScalarAsync<long>(countSql, commandTimeout: _commandTimeout);

        if (deleteCount == 0)
        {
            _logger.LogDebug("No rows to delete");
            return 0;
        }

        _logger.LogInformation("Deleting {Count:N0} rows from target", deleteCount);

        // Delete rows that exist in target but not in staging (source)
        var deleteSql = $@"
            DELETE t
            FROM [{targetTableName}] t
            LEFT JOIN [{stagingTableName}] s ON {joinCondition}
            WHERE s.[{pkColumns[0].ColumnName}] IS NULL";

        var rowsDeleted = await conn.ExecuteAsync(deleteSql, commandTimeout: _commandTimeout);
        return rowsDeleted;
    }

    private string GetSqlServerTypeDef(ColumnInfo col)
    {
        var dataType = col.DataType.ToLower();

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
            "bigint" => typeof(long),
            "int" => typeof(int),
            "smallint" => typeof(short),
            "tinyint" => typeof(byte),
            "bit" => typeof(bool),
            "decimal" or "numeric" or "money" or "smallmoney" => typeof(decimal),
            "float" => typeof(double),
            "real" => typeof(float),
            "date" or "datetime" or "datetime2" or "smalldatetime" => typeof(DateTime),
            "datetimeoffset" => typeof(DateTimeOffset),
            "time" => typeof(TimeSpan),
            "uniqueidentifier" => typeof(Guid),
            "varbinary" or "binary" or "image" or "timestamp" or "rowversion" => typeof(byte[]),
            // Spatial types - store as byte[] (SQL Server native binary format)
            "geometry" or "geography" => typeof(byte[]),
            _ => typeof(string)
        };
    }

    /// <summary>
    /// Check if a SQL type is a spatial type (geometry or geography)
    /// </summary>
    private bool IsSpatialType(string sqlType)
    {
        var baseType = sqlType.ToLower();
        return baseType == "geometry" || baseType == "geography";
    }
}
