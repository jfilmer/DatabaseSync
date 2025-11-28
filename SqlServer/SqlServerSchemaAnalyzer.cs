using Dapper;
using DatabaseSync.Abstractions;
using DatabaseSync.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace DatabaseSync.SqlServer;

/// <summary>
/// Schema analyzer for SQL Server databases
/// </summary>
public class SqlServerSchemaAnalyzer : ISchemaAnalyzer
{
    private readonly string _connectionString;
    private readonly ILogger<SqlServerSchemaAnalyzer> _logger;
    private readonly int _commandTimeout;

    public SqlServerSchemaAnalyzer(
        string connectionString, 
        ILogger<SqlServerSchemaAnalyzer> logger,
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
            "Found {Count} columns in SQL Server table {Table}", 
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
        List<ColumnInfo> pkColumns,
        string? whereClause = null)
    {
        var pkConcat = pkColumns.Count == 1
            ? $"CAST([{pkColumns[0].ColumnName}] AS NVARCHAR(MAX))"
            : $"CONCAT({string.Join(", '|', ", pkColumns.Select(c => $"CAST([{c.ColumnName}] AS NVARCHAR(MAX))"))})";

        var sql = $"SELECT {pkConcat} AS PkValue FROM [{tableName}]";
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        await using var connection = new SqlConnection(_connectionString);
        var results = await connection.QueryAsync<string>(sql, commandTimeout: _commandTimeout);
        return results.ToHashSet();
    }
}
