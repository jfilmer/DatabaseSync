using Dapper;
using DatabaseSync.Abstractions;
using DatabaseSync.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DatabaseSync.SqlServer;

/// <summary>
/// Schema analyzer for SQL Server databases when used as a target
/// Includes additional methods for delete sync and table creation
/// </summary>
public class SqlServerTargetAnalyzer : ISchemaAnalyzer
{
    private readonly string _connectionString;
    private readonly ILogger<SqlServerTargetAnalyzer> _logger;
    private readonly int _commandTimeout;

    public SqlServerTargetAnalyzer(
        string connectionString,
        ILogger<SqlServerTargetAnalyzer> logger,
        int commandTimeout = 300)
    {
        _connectionString = connectionString;
        _logger = logger;
        _commandTimeout = commandTimeout;
    }

    public async Task<List<string>> FindTablesAsync(string pattern)
    {
        const string sql = @"
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME LIKE @pattern
            ORDER BY TABLE_NAME";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<string>(
            sql,
            new { pattern },
            commandTimeout: _commandTimeout);
        return results.ToList();
    }

    public async Task<List<ColumnInfo>> GetTableSchemaAsync(string tableName)
    {
        const string sql = @"
            SELECT
                c.COLUMN_NAME AS ColumnName,
                c.DATA_TYPE AS DataType,
                CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS IsNullable,
                c.CHARACTER_MAXIMUM_LENGTH AS MaxLength,
                c.NUMERIC_PRECISION AS [Precision],
                c.NUMERIC_SCALE AS Scale,
                c.ORDINAL_POSITION AS OrdinalPosition,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IsPrimaryKey,
                CAST(ISNULL(COLUMNPROPERTY(OBJECT_ID(@tableName), c.COLUMN_NAME, 'IsIdentity'), 0) AS BIT) AS IsIdentity
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                    AND tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                    AND tc.TABLE_NAME = ku.TABLE_NAME
                WHERE tc.TABLE_NAME = @tableName
                    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_NAME = @tableName
            ORDER BY c.ORDINAL_POSITION";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<ColumnInfo>(
            sql,
            new { tableName },
            commandTimeout: _commandTimeout);

        var columns = results.ToList();

        _logger.LogDebug(
            "Found {Count} columns in SQL Server target table {Table}",
            columns.Count, tableName);

        return columns;
    }

    public async Task<bool> TableExistsAsync(string tableName)
    {
        const string sql = @"
            SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = @tableName AND TABLE_TYPE = 'BASE TABLE'";

        await using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(
            sql,
            new { tableName },
            commandTimeout: _commandTimeout) > 0;
    }

    public async Task<long> GetRowCountAsync(string tableName, string? whereClause = null)
    {
        var sql = $"SELECT COUNT(*) FROM [{tableName}]";
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        await using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<long>(sql, commandTimeout: _commandTimeout);
    }

    public async Task<DateTime?> GetMaxTimestampAsync(string tableName, string timestampColumn)
    {
        var sql = $"SELECT MAX([{timestampColumn}]) FROM [{tableName}]";

        await using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<DateTime?>(
            sql,
            commandTimeout: _commandTimeout);
    }

    public async Task<long> GetRecentRowsCountAsync(
        string tableName,
        string timestampColumn,
        string? fallbackTimestampColumn,
        int hoursBack,
        string? sourceFilter = null)
    {
        // Build timestamp expression with optional COALESCE for fallback
        string timestampExpression;
        if (!string.IsNullOrEmpty(fallbackTimestampColumn))
        {
            timestampExpression = $"COALESCE([{timestampColumn}], [{fallbackTimestampColumn}])";
        }
        else
        {
            timestampExpression = $"[{timestampColumn}]";
        }

        var cutoffTime = DateTime.UtcNow.AddHours(-hoursBack);
        var whereClause = $"{timestampExpression} >= @cutoffTime";

        if (!string.IsNullOrEmpty(sourceFilter))
        {
            whereClause = $"({sourceFilter}) AND {whereClause}";
        }

        var sql = $"SELECT COUNT(*) FROM [{tableName}] WHERE {whereClause}";

        await using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<long>(
            sql,
            new { cutoffTime },
            commandTimeout: _commandTimeout);
    }

    /// <summary>
    /// Get all primary key values from a table (for delete detection)
    /// </summary>
    public async Task<HashSet<string>> GetPrimaryKeyValuesAsync(
        string tableName,
        List<ColumnInfo> pkColumns)
    {
        var pkConcat = pkColumns.Count == 1
            ? $"CAST([{pkColumns[0].ColumnName}] AS NVARCHAR(MAX))"
            : $"CONCAT({string.Join(", '|', ", pkColumns.Select(c => $"CAST([{c.ColumnName}] AS NVARCHAR(MAX))"))})";

        var sql = $"SELECT {pkConcat} AS PkValue FROM [{tableName}]";

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<string>(sql, commandTimeout: _commandTimeout);
        return results.ToHashSet();
    }

    /// <summary>
    /// Delete rows by primary key values
    /// </summary>
    public async Task<long> DeleteByPrimaryKeysAsync(
        string tableName,
        List<ColumnInfo> pkColumns,
        IEnumerable<string> pkValuesToDelete)
    {
        var pkList = pkValuesToDelete.ToList();
        if (!pkList.Any())
            return 0;

        await using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        if (pkColumns.Count == 1)
        {
            var pkColumn = pkColumns[0];

            // For single PK, batch delete using IN clause
            long totalDeleted = 0;
            const int batchSize = 1000;

            await using var transaction = connection.BeginTransaction();
            try
            {
                foreach (var batch in pkList.Chunk(batchSize))
                {
                    var paramNames = batch.Select((_, i) => $"@p{i}").ToList();
                    var sql = $"DELETE FROM [{tableName}] WHERE [{pkColumn.ColumnName}] IN ({string.Join(", ", paramNames)})";

                    var parameters = new DynamicParameters();
                    for (int i = 0; i < batch.Length; i++)
                    {
                        var typedValue = pkColumn.DataType.ToLower() switch
                        {
                            "int" or "integer" => (object)int.Parse(batch[i]),
                            "bigint" => long.Parse(batch[i]),
                            "uniqueidentifier" => Guid.Parse(batch[i]),
                            _ => batch[i]
                        };
                        parameters.Add($"p{i}", typedValue);
                    }

                    totalDeleted += await connection.ExecuteAsync(sql, parameters, transaction, _commandTimeout);
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }

            return totalDeleted;
        }
        else
        {
            // For composite PKs, delete row by row
            long totalDeleted = 0;
            const int batchSize = 1000;

            await using var transaction = connection.BeginTransaction();
            try
            {
                foreach (var batch in pkList.Chunk(batchSize))
                {
                    foreach (var pkValue in batch)
                    {
                        var parts = pkValue.Split('|');
                        var conditions = pkColumns.Select((col, idx) =>
                            $"[{col.ColumnName}] = @p{idx}").ToList();

                        var parameters = new DynamicParameters();
                        for (int i = 0; i < parts.Length; i++)
                        {
                            parameters.Add($"p{i}", parts[i]);
                        }

                        var sql = $"DELETE FROM [{tableName}] WHERE {string.Join(" AND ", conditions)}";
                        totalDeleted += await connection.ExecuteAsync(
                            sql, parameters, transaction, _commandTimeout);
                    }
                }

                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }

            return totalDeleted;
        }
    }

    /// <summary>
    /// Create a target table with the given schema
    /// </summary>
    public async Task CreateTableAsync(string tableName, List<ColumnInfo> columns)
    {
        var columnDefs = columns.Select(col =>
        {
            var typeDef = GetSqlServerTypeDef(col);
            var nullable = col.IsNullable ? " NULL" : " NOT NULL";
            var identity = col.IsIdentity ? " IDENTITY(1,1)" : "";
            return $"    [{col.ColumnName}] {typeDef}{identity}{nullable}";
        });

        var pkColumns = columns.Where(c => c.IsPrimaryKey).ToList();
        var pkConstraint = pkColumns.Any()
            ? $",\n    CONSTRAINT [PK_{tableName}] PRIMARY KEY ({string.Join(", ", pkColumns.Select(c => $"[{c.ColumnName}]"))})"
            : "";

        var sql = $@"
CREATE TABLE [{tableName}] (
{string.Join(",\n", columnDefs)}{pkConstraint}
)";

        await using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(sql, commandTimeout: _commandTimeout);

        _logger.LogInformation("Created target table [{Table}]", tableName);
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
}
