using Dapper;
using DatabaseSync.Abstractions;
using DatabaseSync.Models;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DatabaseSync.PostgreSql;

/// <summary>
/// Schema analyzer for PostgreSQL databases
/// </summary>
public class PostgreSqlSchemaAnalyzer : ISchemaAnalyzer
{
    private readonly string _connectionString;
    private readonly ILogger<PostgreSqlSchemaAnalyzer> _logger;
    private readonly int _commandTimeout;

    public PostgreSqlSchemaAnalyzer(
        string connectionString, 
        ILogger<PostgreSqlSchemaAnalyzer> logger,
        int commandTimeout = 300)
    {
        _connectionString = connectionString;
        _logger = logger;
        _commandTimeout = commandTimeout;
    }

    public async Task<List<string>> FindTablesAsync(string pattern)
    {
        const string sql = @"
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name LIKE @pattern
            ORDER BY table_name";

        await using var connection = new NpgsqlConnection(_connectionString);
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
                c.column_name AS ColumnName,
                c.data_type AS DataType,
                c.is_nullable = 'YES' AS IsNullable,
                c.character_maximum_length AS MaxLength,
                c.numeric_precision AS Precision,
                c.numeric_scale AS Scale,
                c.ordinal_position AS OrdinalPosition,
                COALESCE(pk.is_pk, FALSE) AS IsPrimaryKey,
                COALESCE(c.column_default LIKE 'nextval%', FALSE) AS IsIdentity
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name, TRUE AS is_pk
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = @tableName 
                    AND tc.table_schema = 'public'
                    AND tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_name = @tableName
                AND c.table_schema = 'public'
            ORDER BY c.ordinal_position";

        await using var connection = new NpgsqlConnection(_connectionString);
        var results = await connection.QueryAsync<ColumnInfo>(
            sql, 
            new { tableName }, 
            commandTimeout: _commandTimeout);
        
        var columns = results.ToList();
        
        _logger.LogDebug(
            "Found {Count} columns in PostgreSQL table {Table}", 
            columns.Count, tableName);
        
        return columns;
    }

    public async Task<bool> TableExistsAsync(string tableName)
    {
        const string sql = @"
            SELECT COUNT(1) FROM information_schema.tables 
            WHERE table_name = @tableName 
                AND table_schema = 'public' 
                AND table_type = 'BASE TABLE'";

        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(
            sql, 
            new { tableName }, 
            commandTimeout: _commandTimeout) > 0;
    }

    public async Task<long> GetRowCountAsync(string tableName, string? whereClause = null)
    {
        var sql = $"SELECT COUNT(*) FROM \"{tableName}\"";
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<long>(sql, commandTimeout: _commandTimeout);
    }

    public async Task<DateTime?> GetMaxTimestampAsync(string tableName, string timestampColumn)
    {
        var sql = $"SELECT MAX(\"{timestampColumn.ToLower()}\") FROM \"{tableName}\"";

        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<DateTime?>(
            sql, 
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
            ? $"CAST(\"{pkColumns[0].ColumnName.ToLower()}\" AS TEXT)"
            : $"CONCAT({string.Join(", '|', ", pkColumns.Select(c => $"CAST(\"{c.ColumnName.ToLower()}\" AS TEXT)"))})";

        var sql = $"SELECT {pkConcat} AS pk_value FROM \"{tableName}\"";
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        await using var connection = new NpgsqlConnection(_connectionString);
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

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        if (pkColumns.Count == 1)
        {
            var pkColumn = pkColumns[0];
            var sql = $"DELETE FROM \"{tableName}\" WHERE \"{pkColumn.ColumnName.ToLower()}\" = ANY(@pks)";
            
            object[] typedPks = pkColumn.DataType.ToLower() switch
            {
                "integer" or "int" => pkList.Select(int.Parse).Cast<object>().ToArray(),
                "bigint" => pkList.Select(long.Parse).Cast<object>().ToArray(),
                "uuid" => pkList.Select(Guid.Parse).Cast<object>().ToArray(),
                _ => pkList.Cast<object>().ToArray()
            };

            return await connection.ExecuteAsync(sql, new { pks = typedPks }, commandTimeout: _commandTimeout);
        }
        else
        {
            long totalDeleted = 0;
            const int batchSize = 1000;

            await using var transaction = await connection.BeginTransactionAsync();
            try
            {
                foreach (var batch in pkList.Chunk(batchSize))
                {
                    foreach (var pkValue in batch)
                    {
                        var parts = pkValue.Split('|');
                        var conditions = pkColumns.Select((col, idx) => 
                            $"\"{col.ColumnName.ToLower()}\" = @p{idx}").ToList();
                        
                        var parameters = new DynamicParameters();
                        for (int i = 0; i < parts.Length; i++)
                        {
                            parameters.Add($"p{i}", parts[i]);
                        }

                        var sql = $"DELETE FROM \"{tableName}\" WHERE {string.Join(" AND ", conditions)}";
                        totalDeleted += await connection.ExecuteAsync(
                            sql, parameters, transaction, _commandTimeout);
                    }
                }

                await transaction.CommitAsync();
            }
            catch
            {
                await transaction.RollbackAsync();
                throw;
            }

            return totalDeleted;
        }
    }
}
