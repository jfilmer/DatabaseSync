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

    // Cached target server encoding (detected on first connection)
    private string? _targetServerEncoding;
    private bool _targetEncodingDetected;

    /// <summary>
    /// Optional callback for progress updates (rowsProcessed)
    /// Called every 100,000 rows during bulk load
    /// </summary>
    public Action<long>? ProgressCallback { get; set; }

    /// <summary>
    /// Maximum table row count for automatic unique constraint violation recovery.
    /// If a unique constraint violation occurs and the table has fewer rows than this,
    /// the service will attempt to delete conflicting rows and retry.
    /// Set to 0 to disable automatic recovery.
    /// Default: 300000 (300K rows)
    /// </summary>
    public int MaxRowsForConstraintRecovery { get; set; } = 300000;

    /// <summary>
    /// Batch size for reading rows from source database.
    /// When set > 0, source data is read in batches using LIMIT/OFFSET.
    /// Set to 0 to disable batching.
    /// Default: 100000 (100K rows per batch)
    /// </summary>
    public int SourceBatchSize { get; set; } = 100000;

    /// <summary>
    /// When true, set <c>session_replication_role = 'replica'</c> on the target connection
    /// after each open, suppressing FK/RI triggers so the unique-constraint recovery
    /// delete/insert and cross-table ordering aren't blocked by self-referential or inbound FKs.
    /// PK/UNIQUE indexes remain enforced. For full prod->dev mirror loads only — never a prod target.
    /// Requires the target user to be a superuser or to hold
    /// <c>GRANT SET ON PARAMETER session_replication_role</c> (PostgreSQL 15+).
    /// See devdocs/core-events-sync-stuck-fk-recovery.md.
    /// </summary>
    public bool DisableTriggersDuringLoad { get; set; } = false;

    // Tracks whether we've already logged a warning about lacking privilege to set the GUC,
    // so a non-superuser target user doesn't spam the log once per table.
    private bool _replicaRoleWarningLogged;

    public PostgreSqlBulkDataCopier(
        string sourceConnectionString,
        string targetConnectionString,
        ILogger<PostgreSqlBulkDataCopier> logger,
        int commandTimeout = 3600)
    {
        // Ensure UTF8 encoding is set in connection strings to handle Unicode characters
        _sourceConnectionString = EnsureUtf8Encoding(sourceConnectionString);
        _targetConnectionString = EnsureUtf8Encoding(targetConnectionString);
        _logger = logger;
        _commandTimeout = commandTimeout;
    }

    /// <summary>
    /// Ensures the connection string includes Options to set client_encoding=UTF8
    /// </summary>
    private static string EnsureUtf8Encoding(string connectionString)
    {
        // If already has Options or Encoding setting, don't modify
        if (connectionString.Contains("Options=", StringComparison.OrdinalIgnoreCase) ||
            connectionString.Contains("client_encoding", StringComparison.OrdinalIgnoreCase))
        {
            return connectionString;
        }

        // Append Options to set client_encoding to UTF8
        var separator = connectionString.TrimEnd().EndsWith(";") ? "" : ";";
        return $"{connectionString}{separator}Options=-c client_encoding=UTF8";
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

        // Exclude GENERATED ALWAYS columns (e.g. a STORED tsvector search_vector) —
        // they cannot be inserted into and are recomputed by the target automatically.
        // Include identity columns to preserve source values.
        var syncColumns = columns.Where(c => !c.IsGenerated).ToList();
        var insertColumns = syncColumns.ToList();
        var updateColumns = syncColumns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}";
        if (stagingTableName.Length > 63) stagingTableName = stagingTableName[..63];

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        // Ensure UTF8 encoding for both connections to handle Unicode characters
        await SetUtf8EncodingAsync(sourceConn);
        await SetUtf8EncodingAsync(targetConn);

        // Detect target server encoding for character sanitization
        await DetectTargetEncodingAsync(targetConn);

        // Mirror load: optionally suppress FK/RI triggers on the target so the
        // unique-constraint recovery delete/insert isn't blocked by self-referential
        // or inbound foreign keys. PK/UNIQUE constraints remain enforced.
        await SetReplicaRoleIfEnabledAsync(targetConn);

        try
        {
            // Create staging table
            _logger.LogDebug("Creating staging table {Staging}", stagingTableName);
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName);

            // Build source query
            var sourceColumnList = BuildSourceColumnList(insertColumns);
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
                insertColumns, updateColumns, pkColumns, result.RowsProcessed);

            // Calculate stats
            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {formattedTargetTable}");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

            // Reset sequences to prevent future ID conflicts
            result.SequenceResetWarnings.AddRange(await ResetSequencesAsync(targetConn, targetTableName));

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

        // Ensure UTF8 encoding for both connections to handle Unicode characters
        await SetUtf8EncodingAsync(sourceConn);
        await SetUtf8EncodingAsync(targetConn);

        // Mirror load: optionally suppress FK/RI triggers on the target so deletes in
        // reverse priority order aren't blocked by self-referential or inbound foreign keys.
        await SetReplicaRoleIfEnabledAsync(targetConn);

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

        // Exclude GENERATED ALWAYS columns (e.g. a STORED tsvector search_vector) —
        // they cannot be inserted into and are recomputed by the target automatically.
        // Include identity columns to preserve source values.
        var syncColumns = columns.Where(c => !c.IsGenerated).ToList();
        var insertColumns = syncColumns.ToList();
        var updateColumns = syncColumns.Where(c => !c.IsPrimaryKey && !c.IsIdentity).ToList();

        var stagingTableName = $"_staging_{targetTableName}_{Guid.NewGuid():N}";
        if (stagingTableName.Length > 63) stagingTableName = stagingTableName[..63];

        await using var sourceConn = new NpgsqlConnection(_sourceConnectionString);
        await using var targetConn = new NpgsqlConnection(_targetConnectionString);

        await sourceConn.OpenAsync();
        await targetConn.OpenAsync();

        // Ensure UTF8 encoding for both connections to handle Unicode characters
        await SetUtf8EncodingAsync(sourceConn);
        await SetUtf8EncodingAsync(targetConn);

        // Detect target server encoding for character sanitization
        await DetectTargetEncodingAsync(targetConn);

        // Mirror load: optionally suppress FK/RI triggers on the target so the
        // unique-constraint recovery delete/insert isn't blocked by self-referential
        // or inbound foreign keys. PK/UNIQUE constraints remain enforced.
        await SetReplicaRoleIfEnabledAsync(targetConn);

        try
        {
            await CreateStagingTableAsync(targetConn, stagingTableName, targetTableName);

            // Build incremental query
            var sourceColumnList = BuildSourceColumnList(insertColumns);
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
                insertColumns, updateColumns, pkColumns, result.RowsProcessed);

            var countAfter = await targetConn.ExecuteScalarAsync<long>(
                $"SELECT COUNT(*) FROM {formattedTargetTable}");

            result.RowsInserted = countAfter - countBefore;
            result.RowsUpdated = result.RowsProcessed - result.RowsInserted;

            // Reset sequences to prevent future ID conflicts
            result.SequenceResetWarnings.AddRange(await ResetSequencesAsync(targetConn, targetTableName));

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

    /// <summary>
    /// Set UTF8 encoding for the connection to properly handle Unicode characters
    /// </summary>
    private async Task SetUtf8EncodingAsync(NpgsqlConnection conn)
    {
        // Set encoding explicitly
        await using var setCmd = new NpgsqlCommand("SET client_encoding TO 'UTF8'", conn);
        setCmd.CommandTimeout = _commandTimeout;
        await setCmd.ExecuteNonQueryAsync();

        // Verify encoding was set
        await using var checkCmd = new NpgsqlCommand("SHOW client_encoding", conn);
        checkCmd.CommandTimeout = _commandTimeout;
        var encoding = await checkCmd.ExecuteScalarAsync() as string;
        _logger.LogDebug("PostgreSQL client_encoding set to: {Encoding} on {Host}",
            encoding, conn.Host);

        // Set timezone to UTC for consistent timestamp handling across servers
        // This ensures timestamptz values are interpreted consistently regardless
        // of the server's default timezone setting
        await using var tzCmd = new NpgsqlCommand("SET TIME ZONE 'UTC'", conn);
        tzCmd.CommandTimeout = _commandTimeout;
        await tzCmd.ExecuteNonQueryAsync();
        _logger.LogDebug("PostgreSQL session timezone set to UTC on {Host}", conn.Host);
    }

    /// <summary>
    /// When <see cref="DisableTriggersDuringLoad"/> is enabled, set
    /// <c>session_replication_role = 'replica'</c> on the (target) connection so FK/RI
    /// triggers are suppressed for this session. Must be called after every open because
    /// the setting is per-connection session state and Npgsql resets pooled connections.
    /// If the connection's user lacks the privilege (not superuser and no parameter grant),
    /// the GUC set is skipped with a one-time warning and sync proceeds with triggers enabled.
    /// </summary>
    private async Task SetReplicaRoleIfEnabledAsync(NpgsqlConnection targetConn)
    {
        if (!DisableTriggersDuringLoad)
            return;

        try
        {
            await using var cmd = new NpgsqlCommand("SET session_replication_role = 'replica'", targetConn);
            cmd.CommandTimeout = _commandTimeout;
            await cmd.ExecuteNonQueryAsync();
            _logger.LogDebug(
                "Set session_replication_role = 'replica' on target {Host} (FK/RI triggers suppressed for this session)",
                targetConn.Host);
        }
        catch (PostgresException ex) when (ex.SqlState == "42501") // insufficient_privilege
        {
            if (!_replicaRoleWarningLogged)
            {
                _replicaRoleWarningLogged = true;
                _logger.LogWarning(
                    "DisableTriggersDuringLoad is enabled but target user lacks privilege to set " +
                    "session_replication_role. FK/RI triggers will remain ON and unique-constraint " +
                    "recovery deletes may be blocked by foreign keys. Grant it on the DEV target only: " +
                    "GRANT SET ON PARAMETER session_replication_role TO <target_user>; (PostgreSQL 15+), " +
                    "or point the profile at a superuser role. See devdocs/core-events-sync-stuck-fk-recovery.md");
            }
        }
    }

    /// <summary>
    /// Detect the target server's encoding (cached after first call)
    /// </summary>
    private async Task DetectTargetEncodingAsync(NpgsqlConnection conn)
    {
        if (_targetEncodingDetected)
            return;

        await using var cmd = new NpgsqlCommand("SHOW server_encoding", conn);
        cmd.CommandTimeout = _commandTimeout;
        _targetServerEncoding = await cmd.ExecuteScalarAsync() as string;
        _targetEncodingDetected = true;

        if (!string.Equals(_targetServerEncoding, "UTF8", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogWarning(
                "Target database server encoding is {Encoding}. " +
                "Characters not supported by this encoding will be replaced with '?'.",
                _targetServerEncoding);
        }
    }

    /// <summary>
    /// Sanitize a string value for the target encoding.
    /// For WIN1252 and other single-byte encodings, replaces unsupported Unicode characters.
    /// </summary>
    private string SanitizeForTargetEncoding(string value)
    {
        // If target is UTF8, no sanitization needed
        if (string.Equals(_targetServerEncoding, "UTF8", StringComparison.OrdinalIgnoreCase))
            return value;

        // For WIN1252 and other single-byte encodings, replace characters outside the supported range
        // WIN1252 supports: 0x00-0x7F (ASCII) and 0x80-0xFF (extended Latin with some gaps)
        var result = new System.Text.StringBuilder(value.Length);
        foreach (var c in value)
        {
            if (c <= 0xFF)
            {
                // Character is in the single-byte range
                // WIN1252 has gaps at 0x81, 0x8D, 0x8F, 0x90, 0x9D but these are rarely used
                result.Append(c);
            }
            else
            {
                // Character is outside single-byte range - replace with '?'
                result.Append('?');
            }
        }
        return result.ToString();
    }

    /// <summary>
    /// Builds the SELECT column list for the source query. USER-DEFINED columns
    /// (pgvector 'vector', enums, etc.) are cast to ::text so Npgsql can read them
    /// without a type plugin — the text form (e.g. vector '[1,2,3]', enum label)
    /// round-trips back into the matching target column via COPY's implicit cast.
    /// Without this, reading a non-null 'vector' throws
    /// "Reading as 'System.String' is not supported for fields having DataTypeName 'public.vector'".
    /// </summary>
    private static string BuildSourceColumnList(List<ColumnInfo> columns)
    {
        return string.Join(", ", columns.Select(c =>
        {
            var name = c.ColumnName.ToLower();
            return c.DataType.Equals("USER-DEFINED", StringComparison.OrdinalIgnoreCase)
                ? $"\"{name}\"::text AS \"{name}\""
                : $"\"{name}\"";
        }));
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
        // Note: client_encoding is set to UTF8 at connection open time
        await using var writer = await targetConn.BeginTextImportAsync(
            $"COPY \"{stagingTableName}\" ({targetColumnList}) FROM STDIN (FORMAT TEXT, NULL '\\N')");

        while (await reader.ReadAsync())
        {
            var values = new string[columns.Count];

            for (int i = 0; i < columns.Count; i++)
            {
                object value;
                try
                {
                    value = reader.GetValue(i);
                }
                catch (Exception)
                {
                    // Unsupported types (e.g. pgvector 'vector') - read as text representation
                    if (reader.IsDBNull(i))
                    {
                        values[i] = "\\N";
                        continue;
                    }
                    var strValue = reader.GetFieldValue<string>(i);
                    strValue = SanitizeForTargetEncoding(strValue);
                    strValue = strValue.Replace("\\", "\\\\")
                                      .Replace("\t", "\\t")
                                      .Replace("\n", "\\n")
                                      .Replace("\r", "\\r");
                    values[i] = strValue;
                    continue;
                }

                if (value == DBNull.Value || value == null)
                {
                    values[i] = "\\N";  // NULL representation in COPY text format
                }
                else if (value is DateTime dt)
                {
                    // Format timestamp for PostgreSQL
                    // IMPORTANT: Preserve timezone info for UTC timestamps to prevent
                    // misinterpretation when target server has different timezone settings
                    if (dt.Kind == DateTimeKind.Utc)
                    {
                        // UTC timestamps should include timezone offset to ensure correct storage
                        values[i] = dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + "+00";
                    }
                    else
                    {
                        // Local or Unspecified - write as naive timestamp
                        values[i] = dt.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                    }
                }
                else if (value is DateTimeOffset dto)
                {
                    // Format timestamptz for PostgreSQL with timezone offset
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
                    strValue = SanitizeForTargetEncoding(strValue);
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
                    var strValue = value.ToString() ?? "";
                    values[i] = SanitizeForTargetEncoding(strValue);
                }
                else if (value is Array arrayValue)
                {
                    // Handle PostgreSQL array types (text[], integer[], etc.)
                    values[i] = FormatPostgresArray(arrayValue);
                }
                else if (value.GetType().FullName?.Contains("Npgsql") == true ||
                         value.GetType().FullName?.Contains("Json") == true)
                {
                    // Handle other JSON/Npgsql types by converting to string
                    var strValue = System.Text.Json.JsonSerializer.Serialize(value);
                    strValue = SanitizeForTargetEncoding(strValue);
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
                    strValue = SanitizeForTargetEncoding(strValue);
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
        List<ColumnInfo> pkColumns,
        long rowsInStaging = 0)
    {
        var insertColumnList = string.Join(", ", insertColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
        var pkColumnList = string.Join(", ", pkColumns.Select(c => $"\"{c.ColumnName.ToLower()}\""));
        var formattedTargetTable = FormatTableNameForSql(targetTable);

        string upsertSql = BuildUpsertSql(formattedTargetTable, stagingTable, insertColumnList,
            pkColumnList, updateColumns);

        try
        {
            await conn.ExecuteAsync(upsertSql, commandTimeout: _commandTimeout);
        }
        catch (PostgresException ex) when (ex.SqlState == "23505") // unique_violation
        {
            // Attempt automatic recovery for unique constraint violations
            await HandleUniqueConstraintViolationAsync(
                conn, ex, stagingTable, targetTable, formattedTargetTable,
                insertColumnList, pkColumnList, updateColumns, rowsInStaging);
        }
    }

    private string BuildUpsertSql(
        string formattedTargetTable,
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
                INSERT INTO {formattedTargetTable} ({insertColumnList})
                OVERRIDING SYSTEM VALUE
                SELECT {insertColumnList} FROM ""{stagingTable}""
                ON CONFLICT ({pkColumnList})
                DO UPDATE SET {updateSetClause}";
        }
        else
        {
            return $@"
                INSERT INTO {formattedTargetTable} ({insertColumnList})
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
        string formattedTargetTable,
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
                $"SELECT COUNT(*) FROM {formattedTargetTable}",
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
            conn, stagingTable, formattedTargetTable, constraintColumns);

        _logger.LogInformation(
            "Deleted {Count:N0} conflicting rows from {Table}. Retrying upsert...",
            deletedCount, targetTable);

        // Retry the upsert
        var upsertSql = BuildUpsertSql(formattedTargetTable, stagingTable, insertColumnList,
            pkColumnList, updateColumns);

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
        // Parse schema and table name
        string schemaName = "public";
        string tableNameOnly = tableName;

        if (tableName.Contains('.'))
        {
            var parts = tableName.Split('.');
            schemaName = parts[0];
            tableNameOnly = parts[1];
        }

        // First try to find it as an index
        var indexQuery = @"
            SELECT a.attname as column_name
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
            WHERE c.relname = @constraintName
              AND n.nspname = @schemaName
              AND t.relname = @tableName
            ORDER BY array_position(i.indkey, a.attnum)";

        var columns = (await conn.QueryAsync<string>(
            indexQuery,
            new { constraintName, schemaName, tableName = tableNameOnly },
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
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE c.conname = @constraintName
              AND n.nspname = @schemaName
              AND t.relname = @tableName
            ORDER BY array_position(c.conkey, a.attnum)";

        columns = (await conn.QueryAsync<string>(
            constraintQuery,
            new { constraintName, schemaName, tableName = tableNameOnly },
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
        string formattedTargetTable,
        List<string> constraintColumns)
    {
        // Build the join condition on constraint columns
        var joinCondition = string.Join(" AND ",
            constraintColumns.Select(c => $"t.\"{c.ToLower()}\" = s.\"{c.ToLower()}\""));

        var deleteSql = $@"
            DELETE FROM {formattedTargetTable} t
            USING ""{stagingTable}"" s
            WHERE {joinCondition}";

        var deleted = await conn.ExecuteAsync(deleteSql, commandTimeout: _commandTimeout);
        return deleted;
    }

    /// <summary>
    /// Reset sequences for all columns that have associated sequences on the TARGET database.
    /// Queries the target directly using pg_get_serial_sequence() instead of relying on
    /// source IsIdentity flags, which may not detect all sequence-backed columns.
    /// </summary>
    private async Task<List<string>> ResetSequencesAsync(NpgsqlConnection conn, string tableName)
    {
        var warnings = new List<string>();
        var (schema, table) = ParseTableName(tableName);
        var qualifiedName = $"{schema}.{table}";
        var formattedTableName = FormatTableNameForSql(tableName);

        try
        {
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
                return warnings;

            foreach (var seq in sequences)
            {
                try
                {
                    string columnName = seq.column_name;
                    string sequenceName = seq.sequence_name;

                    // Get max value from table
                    var maxValue = await conn.ExecuteScalarAsync<long>(
                        $"SELECT COALESCE(MAX(\"{columnName}\"), 0) FROM {formattedTableName}",
                        commandTimeout: _commandTimeout);

                    // Reset sequence: setval with false means next nextval() returns maxValue + 1
                    await conn.ExecuteAsync(
                        $"SELECT setval('{sequenceName}', {maxValue + 1}, false)",
                        commandTimeout: _commandTimeout);

                    _logger.LogInformation(
                        "Reset sequence {Sequence} to {Value} for {Table}.{Column}",
                        sequenceName, maxValue + 1, tableName, columnName);
                }
                catch (Exception ex)
                {
                    var warning = $"Sequence reset failed for {tableName}.{(string)seq.column_name}: {ex.Message}";
                    warnings.Add(warning);
                    _logger.LogWarning(ex,
                        "Failed to reset sequence for {Table}.{Column}",
                        tableName, (string)seq.column_name);
                }
            }
        }
        catch (Exception ex)
        {
            var warning = $"Sequence query failed for {tableName}: {ex.Message}";
            warnings.Add(warning);
            _logger.LogWarning(ex, "Failed to query sequences for {Table}", tableName);
        }

        return warnings;
    }

    /// <summary>
    /// Format a C# array as a PostgreSQL array literal for COPY text mode.
    /// PostgreSQL array format: {element1,element2} with quoted elements for special chars.
    /// Inside quoted elements, \ escapes \ and " (as \\ and \").
    /// For COPY TEXT format, we must escape backslashes again (\ -> \\).
    /// </summary>
    private string FormatPostgresArray(Array array)
    {
        if (array.Length == 0)
            return "{}";

        var elements = new List<string>();
        foreach (var element in array)
        {
            if (element == null)
            {
                elements.Add("NULL");
            }
            else
            {
                var str = element.ToString() ?? "";
                // Sanitize for target encoding (handles WIN1252 etc.)
                str = SanitizeForTargetEncoding(str);
                // Check if element needs quoting
                if (str.Contains('"') || str.Contains('\\') || str.Contains('{') ||
                    str.Contains('}') || str.Contains(',') || str.Contains(' ') ||
                    string.IsNullOrWhiteSpace(str))
                {
                    // Step 1: Escape for PostgreSQL array format
                    // \ -> \\ and " -> \"
                    str = str.Replace("\\", "\\\\").Replace("\"", "\\\"");

                    // Step 2: Escape backslashes again for COPY TEXT format
                    // Each \ becomes \\
                    str = str.Replace("\\", "\\\\");

                    elements.Add($"\"{str}\"");
                }
                else
                {
                    elements.Add(str);
                }
            }
        }

        return "{" + string.Join(",", elements) + "}";
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
            _commandTimeout)
        {
            // Suppress FK/RI triggers on the TARGET only (never the source) so orphan deletes
            // aren't blocked by inbound/self-referential foreign keys during a mirror load.
            DisableTriggersDuringLoad = DisableTriggersDuringLoad
        };

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
