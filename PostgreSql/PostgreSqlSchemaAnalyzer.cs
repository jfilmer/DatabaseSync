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

    /// <summary>
    /// When true, set <c>session_replication_role = 'replica'</c> on the connection used for
    /// orphan-delete sync, so deletes aren't blocked by inbound/self-referential foreign keys.
    /// Set this ONLY for the target analyzer of a prod->dev mirror — never for a source (prod)
    /// connection. Requires the user to be a superuser or hold
    /// <c>GRANT SET ON PARAMETER session_replication_role</c> (PostgreSQL 15+).
    /// See devdocs/core-events-sync-stuck-fk-recovery.md.
    /// </summary>
    public bool DisableTriggersDuringLoad { get; set; } = false;

    public PostgreSqlSchemaAnalyzer(
        string connectionString,
        ILogger<PostgreSqlSchemaAnalyzer> logger,
        int commandTimeout = 300)
    {
        _connectionString = connectionString;
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

    public async Task<List<string>> FindTablesAsync(string pattern)
    {
        var (schema, tablePattern) = ParseTableName(pattern);

        const string sql = @"
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = @schemaName
                AND table_type = 'BASE TABLE'
                AND table_name LIKE @pattern
            ORDER BY table_name";

        await using var connection = new NpgsqlConnection(_connectionString);
        var results = await connection.QueryAsync<string>(
            sql,
            new { schemaName = schema, pattern = tablePattern },
            commandTimeout: _commandTimeout);
        return results.ToList();
    }

    public async Task<List<ColumnInfo>> GetTableSchemaAsync(string tableName)
    {
        var (schema, table) = ParseTableName(tableName);

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
                COALESCE(c.column_default LIKE 'nextval%', FALSE) AS IsIdentity,
                (c.is_generated = 'ALWAYS') AS IsGenerated
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name, TRUE AS is_pk
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_name = @tableName
                    AND tc.table_schema = @schemaName
                    AND tc.constraint_type = 'PRIMARY KEY'
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_name = @tableName
                AND c.table_schema = @schemaName
            ORDER BY c.ordinal_position";

        await using var connection = new NpgsqlConnection(_connectionString);
        var results = await connection.QueryAsync<ColumnInfo>(
            sql,
            new { tableName = table, schemaName = schema },
            commandTimeout: _commandTimeout);

        var columns = results.ToList();

        _logger.LogDebug(
            "Found {Count} columns in PostgreSQL table {Table}",
            columns.Count, tableName);

        return columns;
    }

    public async Task<bool> TableExistsAsync(string tableName)
    {
        var (schema, table) = ParseTableName(tableName);

        const string sql = @"
            SELECT COUNT(1) FROM information_schema.tables
            WHERE table_name = @tableName
                AND table_schema = @schemaName
                AND table_type = 'BASE TABLE'";

        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(
            sql,
            new { tableName = table, schemaName = schema },
            commandTimeout: _commandTimeout) > 0;
    }

    public async Task<long> GetRowCountAsync(string tableName, string? whereClause = null)
    {
        var formattedTable = FormatTableNameForSql(tableName);
        var sql = $"SELECT COUNT(*) FROM {formattedTable}";
        if (!string.IsNullOrEmpty(whereClause))
        {
            sql += $" WHERE {whereClause}";
        }

        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<long>(sql, commandTimeout: _commandTimeout);
    }

    public async Task<DateTime?> GetMaxTimestampAsync(string tableName, string timestampColumn)
    {
        var formattedTable = FormatTableNameForSql(tableName);
        var sql = $"SELECT MAX(\"{timestampColumn.ToLower()}\") FROM {formattedTable}";

        await using var connection = new NpgsqlConnection(_connectionString);
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
        var formattedTable = FormatTableNameForSql(tableName);

        // Build timestamp expression with optional COALESCE for fallback
        string timestampExpression;
        if (!string.IsNullOrEmpty(fallbackTimestampColumn))
        {
            timestampExpression = $"COALESCE(\"{timestampColumn.ToLower()}\", \"{fallbackTimestampColumn.ToLower()}\")";
        }
        else
        {
            timestampExpression = $"\"{timestampColumn.ToLower()}\"";
        }

        var cutoffTime = DateTime.UtcNow.AddHours(-hoursBack);
        var whereClause = $"{timestampExpression} >= @cutoffTime";

        if (!string.IsNullOrEmpty(sourceFilter))
        {
            whereClause = $"({sourceFilter}) AND {whereClause}";
        }

        var sql = $"SELECT COUNT(*) FROM {formattedTable} WHERE {whereClause}";

        await using var connection = new NpgsqlConnection(_connectionString);
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
        var formattedTable = FormatTableNameForSql(tableName);
        var pkConcat = pkColumns.Count == 1
            ? $"CAST(\"{pkColumns[0].ColumnName.ToLower()}\" AS TEXT)"
            : $"CONCAT({string.Join(", '|', ", pkColumns.Select(c => $"CAST(\"{c.ColumnName.ToLower()}\" AS TEXT)"))})";

        var sql = $"SELECT {pkConcat} AS pk_value FROM {formattedTable}";
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
        var formattedTable = FormatTableNameForSql(tableName);
        var pkList = pkValuesToDelete.ToList();
        if (!pkList.Any())
            return 0;

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        // Mirror load: optionally suppress FK/RI triggers so orphan deletes aren't blocked
        // by inbound/self-referential foreign keys. Target connection only. PK/UNIQUE still enforced.
        await SetReplicaRoleIfEnabledAsync(connection);

        if (pkColumns.Count == 1)
        {
            var pkColumn = pkColumns[0];
            var sql = $"DELETE FROM {formattedTable} WHERE \"{pkColumn.ColumnName.ToLower()}\" = ANY(@pks)";

            // Use strongly-typed arrays so Npgsql can infer the correct PostgreSQL type
            var dataType = pkColumn.DataType.ToLower();
            if (dataType is "integer" or "int" or "int4")
            {
                var typedPks = pkList.Select(int.Parse).ToArray();
                return await connection.ExecuteAsync(sql, new { pks = typedPks }, commandTimeout: _commandTimeout);
            }
            else if (dataType is "bigint" or "int8")
            {
                var typedPks = pkList.Select(long.Parse).ToArray();
                return await connection.ExecuteAsync(sql, new { pks = typedPks }, commandTimeout: _commandTimeout);
            }
            else if (dataType is "uuid")
            {
                var typedPks = pkList.Select(Guid.Parse).ToArray();
                return await connection.ExecuteAsync(sql, new { pks = typedPks }, commandTimeout: _commandTimeout);
            }
            else if (dataType is "smallint" or "int2")
            {
                var typedPks = pkList.Select(short.Parse).ToArray();
                return await connection.ExecuteAsync(sql, new { pks = typedPks }, commandTimeout: _commandTimeout);
            }
            else
            {
                var typedPks = pkList.ToArray();
                return await connection.ExecuteAsync(sql, new { pks = typedPks }, commandTimeout: _commandTimeout);
            }
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
                            parameters.Add($"p{i}", ConvertPkValue(parts[i], pkColumns[i].DataType));
                        }

                        var sql = $"DELETE FROM {formattedTable} WHERE {string.Join(" AND ", conditions)}";
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

    /// <summary>
    /// When <see cref="DisableTriggersDuringLoad"/> is enabled, set
    /// <c>session_replication_role = 'replica'</c> on the connection so FK/RI triggers are
    /// suppressed for this session. If the user lacks the privilege, the set is skipped with
    /// a warning and the delete proceeds with triggers enabled (pre-existing behavior).
    /// </summary>
    private async Task SetReplicaRoleIfEnabledAsync(NpgsqlConnection connection)
    {
        if (!DisableTriggersDuringLoad)
            return;

        try
        {
            await connection.ExecuteAsync("SET session_replication_role = 'replica'",
                commandTimeout: _commandTimeout);
        }
        catch (PostgresException ex) when (ex.SqlState == "42501") // insufficient_privilege
        {
            _logger.LogWarning(
                "DisableTriggersDuringLoad is enabled but target user lacks privilege to set " +
                "session_replication_role; orphan-delete sync may be blocked by foreign keys. " +
                "Grant on the DEV target only: GRANT SET ON PARAMETER session_replication_role TO <target_user>; " +
                "(PostgreSQL 15+). See devdocs/core-events-sync-stuck-fk-recovery.md");
        }
    }

    private static object ConvertPkValue(string value, string dataType)
    {
        return dataType.ToLower() switch
        {
            "integer" or "int" or "int4" => int.Parse(value),
            "bigint" or "int8" => long.Parse(value),
            "smallint" or "int2" => short.Parse(value),
            "uuid" => Guid.Parse(value),
            "boolean" or "bool" => bool.Parse(value),
            _ => value
        };
    }
}
