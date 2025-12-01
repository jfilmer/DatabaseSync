using System.Collections.Concurrent;
using System.Diagnostics;
using Dapper;
using DatabaseSync.Abstractions;
using DatabaseSync.Configuration;
using DatabaseSync.Enums;
using DatabaseSync.Models;
using DatabaseSync.PostgreSql;
using DatabaseSync.SqlServer;
using Microsoft.Extensions.Logging;
using Npgsql;
using Microsoft.Data.SqlClient;

namespace DatabaseSync.Services;

/// <summary>
/// Orchestrates the sync process for a single profile
/// </summary>
public class SyncOrchestrator
{
    private readonly SyncProfile _profile;
    private readonly ISchemaAnalyzer _sourceAnalyzer;
    private readonly ISchemaAnalyzer _targetAnalyzer;
    private readonly TypeMapper _typeMapper;
    private readonly ISyncHistoryRepository? _historyRepository;
    private readonly ILogger<SyncOrchestrator> _logger;
    private readonly Guid _runId;
    private readonly DatabaseType _sourceDatabaseType;
    private readonly DatabaseType _targetDatabaseType;

    // Bulk copiers - only one will be used based on source/target combination
    private readonly BulkDataCopier? _sqlServerToPostgreSqlCopier;
    private readonly SqlServerBulkDataCopier? _sqlServerToSqlServerCopier;
    private readonly PostgreSqlBulkDataCopier? _postgreSqlToPostgreSqlCopier;
    private readonly PostgreSqlToSqlServerBulkCopier? _postgreSqlToSqlServerCopier;

    // Optional callback for tracking active tables
    private readonly Action<string, bool>? _tableStatusCallback;
    // Optional callback for progress updates (tableName, rowsProcessed, phase)
    private readonly Action<string, long, string>? _progressCallback;

    public SyncOrchestrator(
        SyncProfile profile,
        ILogger<SyncOrchestrator> logger,
        ILogger<SqlServerSchemaAnalyzer> sqlSourceLogger,
        ILogger<PostgreSqlSchemaAnalyzer> pgSourceLogger,
        ILogger<SqlServerTargetAnalyzer> sqlTargetLogger,
        ILogger<BulkDataCopier> sqlToPgCopierLogger,
        ILogger<SqlServerBulkDataCopier> sqlToSqlCopierLogger,
        ILogger<PostgreSqlBulkDataCopier> pgToPgCopierLogger,
        ILogger<PostgreSqlToSqlServerBulkCopier> pgToSqlCopierLogger,
        ILogger<PostgreSqlSyncHistoryRepository> pgHistoryLogger,
        ILogger<SqlServerSyncHistoryRepository> sqlHistoryLogger,
        Action<string, bool>? tableStatusCallback = null,
        Action<string, long, string>? progressCallback = null)
    {
        _profile = profile;
        _logger = logger;
        _tableStatusCallback = tableStatusCallback;
        _progressCallback = progressCallback;
        _runId = Guid.NewGuid();
        _typeMapper = new TypeMapper();
        _sourceDatabaseType = profile.SourceConnection.DatabaseType;
        _targetDatabaseType = profile.TargetConnection.DatabaseType;

        // Initialize source analyzer based on source database type
        if (_sourceDatabaseType == DatabaseType.SqlServer)
        {
            _sourceAnalyzer = new SqlServerSchemaAnalyzer(
                profile.SourceConnection.ConnectionString,
                sqlSourceLogger,
                profile.Options.CommandTimeoutSeconds);
        }
        else if (_sourceDatabaseType == DatabaseType.PostgreSql)
        {
            _sourceAnalyzer = new PostgreSqlSchemaAnalyzer(
                profile.SourceConnection.ConnectionString,
                pgSourceLogger,
                profile.Options.CommandTimeoutSeconds);
        }
        else
        {
            throw new NotSupportedException($"Source database type {_sourceDatabaseType} is not supported");
        }

        // Initialize target analyzer and history repository based on target database type
        if (_targetDatabaseType == DatabaseType.PostgreSql)
        {
            _targetAnalyzer = new PostgreSqlSchemaAnalyzer(
                profile.TargetConnection.ConnectionString,
                pgSourceLogger,
                profile.Options.CommandTimeoutSeconds);

            if (profile.Options.EnableSyncHistory)
            {
                _historyRepository = new PostgreSqlSyncHistoryRepository(
                    profile.TargetConnection.ConnectionString,
                    pgHistoryLogger);
            }
        }
        else if (_targetDatabaseType == DatabaseType.SqlServer)
        {
            _targetAnalyzer = new SqlServerTargetAnalyzer(
                profile.TargetConnection.ConnectionString,
                sqlTargetLogger,
                profile.Options.CommandTimeoutSeconds);

            if (profile.Options.EnableSyncHistory)
            {
                _historyRepository = new SqlServerSyncHistoryRepository(
                    profile.TargetConnection.ConnectionString,
                    sqlHistoryLogger);
            }
        }
        else
        {
            throw new NotSupportedException($"Target database type {_targetDatabaseType} is not supported");
        }

        // Initialize the appropriate bulk copier based on source/target combination
        switch (_sourceDatabaseType, _targetDatabaseType)
        {
            case (DatabaseType.SqlServer, DatabaseType.PostgreSql):
                _sqlServerToPostgreSqlCopier = new BulkDataCopier(
                    profile.SourceConnection.ConnectionString,
                    profile.TargetConnection.ConnectionString,
                    _typeMapper,
                    sqlToPgCopierLogger,
                    profile.Options.CommandTimeoutSeconds);
                break;

            case (DatabaseType.SqlServer, DatabaseType.SqlServer):
                _sqlServerToSqlServerCopier = new SqlServerBulkDataCopier(
                    profile.SourceConnection.ConnectionString,
                    profile.TargetConnection.ConnectionString,
                    sqlToSqlCopierLogger,
                    profile.Options.CommandTimeoutSeconds);
                break;

            case (DatabaseType.PostgreSql, DatabaseType.PostgreSql):
                _postgreSqlToPostgreSqlCopier = new PostgreSqlBulkDataCopier(
                    profile.SourceConnection.ConnectionString,
                    profile.TargetConnection.ConnectionString,
                    pgToPgCopierLogger,
                    profile.Options.CommandTimeoutSeconds);
                break;

            case (DatabaseType.PostgreSql, DatabaseType.SqlServer):
                _postgreSqlToSqlServerCopier = new PostgreSqlToSqlServerBulkCopier(
                    profile.SourceConnection.ConnectionString,
                    profile.TargetConnection.ConnectionString,
                    pgToSqlCopierLogger,
                    profile.Options.CommandTimeoutSeconds);
                break;

            default:
                throw new NotSupportedException(
                    $"Sync from {_sourceDatabaseType} to {_targetDatabaseType} is not supported");
        }
    }

    /// <summary>
    /// Initialize sync (create history table if needed)
    /// </summary>
    public async Task InitializeAsync()
    {
        if (_historyRepository != null)
        {
            await _historyRepository.InitializeAsync();
        }
    }

    /// <summary>
    /// Sync a single table
    /// </summary>
    public async Task<SyncResult> SyncTableAsync(TableConfig tableConfig, bool forceFullRefresh = false)
    {
        var result = new SyncResult { TableName = tableConfig.SourceTable };
        var stopwatch = Stopwatch.StartNew();
        var syncStartTime = DateTime.UtcNow;
        DateTime? maxSourceTimestamp = null;

        // Notify that table sync is starting
        _tableStatusCallback?.Invoke(tableConfig.SourceTable, true);

        try
        {
            _logger.LogInformation(
                "Syncing {Table} from {SourceType} to {TargetType} (Mode: {Mode})",
                tableConfig.SourceTable,
                _sourceDatabaseType,
                _targetDatabaseType,
                forceFullRefresh ? "FullRefresh (forced)" : tableConfig.Mode.ToString());

            // Get the source table name with correct casing for the source database
            var sourceTableNameForCheck = GetEffectiveSourceTableName(tableConfig);

            // Verify source exists
            if (!await _sourceAnalyzer.TableExistsAsync(sourceTableNameForCheck))
            {
                result.Success = false;
                result.Error = $"Source table '{sourceTableNameForCheck}' not found";
                _logger.LogError("Sync failed for {Table}: {Error}", tableConfig.SourceTable, result.Error);
                goto RecordHistory;
            }

            // Get source schema
            var sourceSchema = await _sourceAnalyzer.GetTableSchemaAsync(sourceTableNameForCheck);

            // Map types based on source and target database types
            foreach (var col in sourceSchema)
            {
                col.MappedDataType = _typeMapper.MapType(col, _sourceDatabaseType, _targetDatabaseType);
            }

            // Validate primary keys
            var pkColumns = sourceSchema.Where(c => c.IsPrimaryKey).ToList();
            if (!pkColumns.Any())
            {
                result.Success = false;
                result.Error = "No primary key found - cannot perform upsert";
                _logger.LogError("Sync failed for {Table}: {Error}", tableConfig.SourceTable, result.Error);
                goto RecordHistory;
            }

            // Check/create target table
            // For SQL Server targets, preserve original case; for PostgreSQL, use lowercase
            var targetTableName = GetEffectiveTargetTableName(tableConfig);
            if (!await _targetAnalyzer.TableExistsAsync(targetTableName))
            {
                if (tableConfig.CreateIfMissing)
                {
                    await CreateTargetTableAsync(targetTableName, sourceSchema);
                    _logger.LogInformation("Created target table {Table}", targetTableName);
                }
                else
                {
                    result.Success = false;
                    result.Error = $"Target table '{targetTableName}' not found";
                    _logger.LogError("Sync failed for {Table}: {Error}", tableConfig.SourceTable, result.Error);
                    goto RecordHistory;
                }
            }

            // Get target schema and filter source columns to only those that exist in target
            var targetSchema = await _targetAnalyzer.GetTableSchemaAsync(targetTableName);
            var targetColumnNames = new HashSet<string>(
                targetSchema.Select(c => c.ColumnName),
                StringComparer.OrdinalIgnoreCase);

            var filteredColumns = sourceSchema
                .Where(c => targetColumnNames.Contains(c.ColumnName))
                .ToList();

            // Log any columns that will be skipped
            var skippedColumns = sourceSchema
                .Where(c => !targetColumnNames.Contains(c.ColumnName))
                .Select(c => c.ColumnName)
                .ToList();

            if (skippedColumns.Any())
            {
                _logger.LogWarning(
                    "Skipping {Count} source columns not found in target: {Columns}",
                    skippedColumns.Count,
                    string.Join(", ", skippedColumns));
                result.Warnings.Add($"Skipped columns: {string.Join(", ", skippedColumns)}");
            }

            // Verify we still have primary key columns after filtering
            var filteredPkColumns = filteredColumns.Where(c => c.IsPrimaryKey).ToList();
            if (!filteredPkColumns.Any())
            {
                result.Success = false;
                result.Error = "No primary key columns remain after filtering - cannot perform upsert";
                _logger.LogError("Sync failed for {Table}: {Error}", tableConfig.SourceTable, result.Error);
                goto RecordHistory;
            }

            // Determine effective mode and last sync time
            var effectiveMode = forceFullRefresh ? SyncMode.FullRefresh : tableConfig.Mode;
            DateTime? lastSyncTime = tableConfig.LastSyncTimeOverride;

            // SQL Server minimum date (DateTime.MinValue is 1/1/0001 which causes overflow)
            var sqlMinDate = new DateTime(1753, 1, 1);

            if (effectiveMode == SyncMode.Incremental &&
                lastSyncTime == null &&
                _profile.Options.UseHistoryForIncrementalSync &&
                _historyRepository != null)
            {
                var lastSync = await _historyRepository.GetLastSyncInfoAsync(
                    _profile.EffectiveProfileId, tableConfig.SourceTable);

                if (lastSync?.MaxSourceTimestamp != null)
                {
                    lastSyncTime = lastSync.MaxSourceTimestamp;
                    _logger.LogDebug("Using last sync time from history: {Time}", lastSyncTime);
                }
                else if (tableConfig.LookbackHours > 0)
                {
                    // No history exists - use lookback hours from now for first sync
                    lastSyncTime = DateTime.UtcNow.AddHours(-tableConfig.LookbackHours);
                    _logger.LogInformation("No sync history found - using lookback of {Hours}h from now: {Time}",
                        tableConfig.LookbackHours, lastSyncTime);
                }
                else
                {
                    // No history and no lookback configured - fall back to SQL min date (full sync)
                    lastSyncTime = sqlMinDate;
                    _logger.LogInformation("No sync history found and no lookback configured - syncing all data");
                }
            }

            // Apply lookback hours if configured and we have history (re-sync data from further back to catch late changes)
            if (effectiveMode == SyncMode.Incremental &&
                tableConfig.LookbackHours > 0 &&
                lastSyncTime.HasValue &&
                lastSyncTime.Value > sqlMinDate &&
                lastSyncTime.Value < DateTime.UtcNow.AddHours(-tableConfig.LookbackHours))
            {
                // Only apply lookback if lastSyncTime came from history (not already calculated from lookback)
                var originalTime = lastSyncTime.Value;
                lastSyncTime = lastSyncTime.Value.AddHours(-tableConfig.LookbackHours);

                // Don't go before SQL min date
                if (lastSyncTime < sqlMinDate)
                    lastSyncTime = sqlMinDate;

                _logger.LogDebug("Applied {Hours}h lookback: {Original} -> {Adjusted}",
                    tableConfig.LookbackHours, originalTime, lastSyncTime);
            }

            // Perform sync using the appropriate data copier based on source/target combination
            BulkCopyResult copyResult;

            // Get the source table name with correct casing for the source database
            var sourceTableName = GetEffectiveSourceTableName(tableConfig);

            // Set up progress callback on the appropriate copier
            SetupProgressCallback(tableConfig.SourceTable);

            if (effectiveMode == SyncMode.FullRefresh)
            {
                copyResult = await ExecuteBulkUpsertAsync(sourceTableName, targetTableName, filteredColumns, tableConfig);
            }
            else
            {
                if (string.IsNullOrEmpty(tableConfig.TimestampColumn))
                {
                    throw new InvalidOperationException(
                        $"TimestampColumn required for incremental sync of {tableConfig.SourceTable}");
                }

                copyResult = await ExecuteIncrementalUpsertAsync(
                    sourceTableName, targetTableName, filteredColumns, tableConfig, lastSyncTime ?? sqlMinDate);

                maxSourceTimestamp = await _sourceAnalyzer.GetMaxTimestampAsync(
                    sourceTableName, tableConfig.TimestampColumn);
            }

            result.RowsProcessed = copyResult.RowsProcessed;
            result.RowsInserted = copyResult.RowsInserted;
            result.RowsUpdated = copyResult.RowsUpdated;
            result.RowsDeleted = copyResult.RowsDeleted;
            result.RecentRowsCount = copyResult.RecentRowsCount;
            result.Success = true;

            // Report final progress
            _progressCallback?.Invoke(tableConfig.SourceTable, copyResult.RowsProcessed, "Complete");
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.Error = ex.Message;
            _logger.LogError(ex, "Sync failed for {Table}", tableConfig.SourceTable);

            // Report error status
            _progressCallback?.Invoke(tableConfig.SourceTable, 0, "Error");
        }

        RecordHistory:
        stopwatch.Stop();
        result.Duration = stopwatch.Elapsed;

        // Calculate recent rows count and total source rows if timestamp column is configured
        // This counts rows with timestamp within last 168 hours (7 days)
        long recentRowsCount = 0;
        long totalSourceRows = 0;
        if (result.Success && !string.IsNullOrEmpty(tableConfig.TimestampColumn))
        {
            try
            {
                var sourceTableName = GetEffectiveSourceTableName(tableConfig);

                // Get total source row count (with filter if configured)
                totalSourceRows = await _sourceAnalyzer.GetRowCountAsync(
                    sourceTableName,
                    tableConfig.SourceFilter);

                // Get recent rows count
                recentRowsCount = await _sourceAnalyzer.GetRecentRowsCountAsync(
                    sourceTableName,
                    tableConfig.TimestampColumn,
                    tableConfig.FallbackTimestampColumn,
                    hoursBack: 168,
                    tableConfig.SourceFilter);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Failed to calculate recent rows count for {Table}",
                    tableConfig.SourceTable);
            }
        }

        // Record history (always, even for validation failures)
        if (_historyRepository != null)
        {
            await _historyRepository.RecordSyncAsync(new SyncHistory
            {
                RunId = _runId,
                ProfileName = _profile.EffectiveProfileId,
                SourceTable = tableConfig.SourceTable,
                TargetTable = GetEffectiveTargetTableName(tableConfig),
                SyncStartTime = syncStartTime,
                SyncEndTime = DateTime.UtcNow,
                Success = result.Success,
                RowsProcessed = result.RowsProcessed,
                RowsInserted = result.RowsInserted,
                RowsUpdated = result.RowsUpdated,
                RowsDeleted = result.RowsDeleted,
                ErrorMessage = result.Error,
                MaxSourceTimestamp = maxSourceTimestamp,
                DurationSeconds = result.Duration.TotalSeconds,
                RecentRowsCount = recentRowsCount,
                TotalSourceRows = totalSourceRows
            });
        }

        // Notify that table sync is complete
        _tableStatusCallback?.Invoke(tableConfig.SourceTable, false);

        return result;
    }

    /// <summary>
    /// Sync all tables in the profile
    /// </summary>
    public async Task<SyncRunResult> SyncAllAsync(
        bool forceFullRefresh = false,
        LoadMonitor? loadMonitor = null,
        LoadThrottlingConfig? loadThrottling = null)
    {
        var runResult = new SyncRunResult
        {
            RunId = _runId,
            ProfileName = _profile.ProfileName,
            StartTime = DateTime.UtcNow
        };

        _logger.LogInformation(
            "Starting sync run for profile '{Profile}' with {Count} tables",
            _profile.ProfileName,
            _profile.Tables.Count);

        // Group tables by priority
        var tablesByPriority = _profile.Tables
            .OrderBy(t => t.Priority)
            .GroupBy(t => t.Priority)
            .ToList();

        var results = new ConcurrentBag<SyncResult>();
        var failedTables = new ConcurrentBag<string>();

        foreach (var priorityGroup in tablesByPriority)
        {
            _logger.LogDebug("Processing priority {Priority} with {Count} tables",
                priorityGroup.Key, priorityGroup.Count());

            if (_profile.Options.MaxParallelTables <= 1)
            {
                // Sequential
                foreach (var tableConfig in priorityGroup)
                {
                    if (_profile.Options.StopOnError && failedTables.Any())
                    {
                        _logger.LogWarning("Skipping {Table} due to previous failures",
                            tableConfig.SourceTable);
                        continue;
                    }

                    // Check load before each table (BeforeTable or Both)
                    if (loadMonitor != null && loadThrottling != null &&
                        (loadThrottling.CheckTiming == LoadCheckTiming.BeforeTable ||
                         loadThrottling.CheckTiming == LoadCheckTiming.Both))
                    {
                        await loadMonitor.WaitForLowLoadAsync(
                            _profile.SourceConnection.ConnectionString,
                            _sourceDatabaseType,
                            loadThrottling.MaxCpuPercent,
                            loadThrottling.CheckIntervalSeconds,
                            loadThrottling.MaxWaitMinutes);
                    }

                    var result = await SyncTableAsync(tableConfig, forceFullRefresh);
                    results.Add(result);
                    LogTableResult(result);

                    if (!result.Success)
                        failedTables.Add(tableConfig.SourceTable);
                }
            }
            else
            {
                // Parallel
                var semaphore = new SemaphoreSlim(_profile.Options.MaxParallelTables);
                var tasks = new List<Task>();

                foreach (var tableConfig in priorityGroup)
                {
                    if (_profile.Options.StopOnError && failedTables.Any())
                    {
                        _logger.LogWarning("Skipping {Table} due to previous failures",
                            tableConfig.SourceTable);
                        continue;
                    }

                    var config = tableConfig;
                    // Capture load monitor/throttling for closure
                    var monitor = loadMonitor;
                    var throttling = loadThrottling;

                    tasks.Add(Task.Run(async () =>
                    {
                        await semaphore.WaitAsync();
                        try
                        {
                            // Check load before each table (BeforeTable or Both)
                            if (monitor != null && throttling != null &&
                                (throttling.CheckTiming == LoadCheckTiming.BeforeTable ||
                                 throttling.CheckTiming == LoadCheckTiming.Both))
                            {
                                await monitor.WaitForLowLoadAsync(
                                    _profile.SourceConnection.ConnectionString,
                                    _sourceDatabaseType,
                                    throttling.MaxCpuPercent,
                                    throttling.CheckIntervalSeconds,
                                    throttling.MaxWaitMinutes);
                            }

                            var result = await SyncTableAsync(config, forceFullRefresh);
                            results.Add(result);
                            LogTableResult(result);

                            if (!result.Success)
                                failedTables.Add(config.SourceTable);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }));
                }

                await Task.WhenAll(tasks);
            }
        }

        runResult.TableResults = results.ToList();
        runResult.EndTime = DateTime.UtcNow;

        LogSyncSummary(runResult);

        return runResult;
    }

    private void LogTableResult(SyncResult result)
    {
        var status = result.Success ? "✓" : "✗";
        var rate = result.Duration.TotalSeconds > 0
            ? $"{result.RowsProcessed / result.Duration.TotalSeconds:N0} rows/sec"
            : "N/A";

        _logger.LogInformation(
            "{Status} {Table}: {Processed:N0} processed, {Inserted:N0} inserted, " +
            "{Updated:N0} updated, {Deleted:N0} deleted ({Duration}, {Rate})",
            status, result.TableName, result.RowsProcessed, result.RowsInserted,
            result.RowsUpdated, result.RowsDeleted, FormatDuration(result.Duration), rate);

        if (!string.IsNullOrEmpty(result.Error))
            _logger.LogError("  Error: {Error}", result.Error);

        foreach (var warning in result.Warnings)
            _logger.LogWarning("  Warning: {Warning}", warning);
    }

    private void LogSyncSummary(SyncRunResult result)
    {
        _logger.LogInformation(@"
═══════════════════════════════════════════════════════════════
  Profile: {ProfileName}
  Total Duration: {Duration}
  Tables: {Success}/{Total} successful

  Rows: {Processed:N0} processed, {Inserted:N0} inserted,
        {Updated:N0} updated, {Deleted:N0} deleted

  Throughput: {Rate:N0} rows/second
═══════════════════════════════════════════════════════════════",
            result.ProfileName,
            FormatDuration(result.Duration),
            result.SuccessCount,
            result.TableResults.Count,
            result.TotalRowsProcessed,
            result.TotalRowsInserted,
            result.TotalRowsUpdated,
            result.TotalRowsDeleted,
            result.OverallRowsPerSecond);
    }

    private async Task CreateTargetTableAsync(string tableName, List<ColumnInfo> columns)
    {
        if (_targetDatabaseType == DatabaseType.PostgreSql)
        {
            await CreatePostgreSqlTableAsync(tableName, columns);
        }
        else if (_targetDatabaseType == DatabaseType.SqlServer)
        {
            await CreateSqlServerTableAsync(tableName, columns);
        }
    }

    private async Task CreatePostgreSqlTableAsync(string tableName, List<ColumnInfo> columns)
    {
        var columnDefs = columns.Select(col =>
        {
            var nullable = col.IsNullable ? "" : " NOT NULL";
            var identity = col.IsIdentity ? " GENERATED ALWAYS AS IDENTITY" : "";
            return $"    \"{col.ColumnName.ToLower()}\" {col.MappedDataType}{nullable}{identity}";
        });

        var pkColumns = columns.Where(c => c.IsPrimaryKey)
            .Select(c => $"\"{c.ColumnName.ToLower()}\"");

        var pkConstraint = pkColumns.Any()
            ? $",\n    PRIMARY KEY ({string.Join(", ", pkColumns)})"
            : "";

        var sql = $@"
CREATE TABLE ""{tableName}"" (
{string.Join(",\n", columnDefs)}{pkConstraint}
)";

        await using var connection = new NpgsqlConnection(_profile.TargetConnection.ConnectionString);
        await connection.ExecuteAsync(sql, commandTimeout: _profile.Options.CommandTimeoutSeconds);
    }

    private async Task CreateSqlServerTableAsync(string tableName, List<ColumnInfo> columns)
    {
        // For SQL Server to SQL Server, use original column types (no mapping needed)
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

        await using var connection = new SqlConnection(_profile.TargetConnection.ConnectionString);
        await connection.ExecuteAsync(sql, commandTimeout: _profile.Options.CommandTimeoutSeconds);
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

    /// <summary>
    /// Get effective target table name based on target database type
    /// </summary>
    private string GetEffectiveTargetTableName(TableConfig tableConfig)
    {
        if (!string.IsNullOrEmpty(tableConfig.TargetTable))
        {
            // User specified explicit target name
            return _targetDatabaseType == DatabaseType.PostgreSql
                ? tableConfig.TargetTable.ToLower()
                : tableConfig.TargetTable;
        }

        // Default: use source table name with appropriate casing
        return _targetDatabaseType == DatabaseType.PostgreSql
            ? tableConfig.SourceTable.ToLower()
            : tableConfig.SourceTable;
    }

    /// <summary>
    /// Get effective source table name based on source database type
    /// </summary>
    private string GetEffectiveSourceTableName(TableConfig tableConfig)
    {
        // For PostgreSQL sources, table names are typically lowercase
        // For SQL Server sources, preserve the original case
        return _sourceDatabaseType == DatabaseType.PostgreSql
            ? tableConfig.SourceTable.ToLower()
            : tableConfig.SourceTable;
    }

    /// <summary>
    /// Execute bulk upsert using the appropriate copier for the source/target combination
    /// </summary>
    private async Task<BulkCopyResult> ExecuteBulkUpsertAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        TableConfig config)
    {
        return (_sourceDatabaseType, _targetDatabaseType) switch
        {
            (DatabaseType.SqlServer, DatabaseType.PostgreSql) =>
                await _sqlServerToPostgreSqlCopier!.BulkUpsertAsync(sourceTableName, targetTableName, columns, config),

            (DatabaseType.SqlServer, DatabaseType.SqlServer) =>
                await _sqlServerToSqlServerCopier!.BulkUpsertAsync(sourceTableName, targetTableName, columns, config),

            (DatabaseType.PostgreSql, DatabaseType.PostgreSql) =>
                await _postgreSqlToPostgreSqlCopier!.BulkUpsertAsync(sourceTableName, targetTableName, columns, config),

            (DatabaseType.PostgreSql, DatabaseType.SqlServer) =>
                await _postgreSqlToSqlServerCopier!.BulkUpsertAsync(sourceTableName, targetTableName, columns, config),

            _ => throw new NotSupportedException(
                $"Sync from {_sourceDatabaseType} to {_targetDatabaseType} is not supported")
        };
    }

    /// <summary>
    /// Execute incremental upsert using the appropriate copier for the source/target combination
    /// </summary>
    private async Task<BulkCopyResult> ExecuteIncrementalUpsertAsync(
        string sourceTableName,
        string targetTableName,
        List<ColumnInfo> columns,
        TableConfig config,
        DateTime lastSyncTime)
    {
        return (_sourceDatabaseType, _targetDatabaseType) switch
        {
            (DatabaseType.SqlServer, DatabaseType.PostgreSql) =>
                await _sqlServerToPostgreSqlCopier!.IncrementalUpsertAsync(sourceTableName, targetTableName, columns, config, lastSyncTime),

            (DatabaseType.SqlServer, DatabaseType.SqlServer) =>
                await _sqlServerToSqlServerCopier!.IncrementalUpsertAsync(sourceTableName, targetTableName, columns, config, lastSyncTime),

            (DatabaseType.PostgreSql, DatabaseType.PostgreSql) =>
                await _postgreSqlToPostgreSqlCopier!.IncrementalUpsertAsync(sourceTableName, targetTableName, columns, config, lastSyncTime),

            (DatabaseType.PostgreSql, DatabaseType.SqlServer) =>
                await _postgreSqlToSqlServerCopier!.IncrementalUpsertAsync(sourceTableName, targetTableName, columns, config, lastSyncTime),

            _ => throw new NotSupportedException(
                $"Sync from {_sourceDatabaseType} to {_targetDatabaseType} is not supported")
        };
    }

    /// <summary>
    /// Get sync status for all tables in this profile
    /// </summary>
    public async Task<Dictionary<string, LastSyncInfo>> GetSyncStatusAsync()
    {
        if (_historyRepository == null)
            return new Dictionary<string, LastSyncInfo>();

        return await _historyRepository.GetAllLastSyncInfoAsync(_profile.EffectiveProfileId);
    }

    /// <summary>
    /// Set up the progress callback on the appropriate bulk copier
    /// </summary>
    private void SetupProgressCallback(string tableName)
    {
        if (_progressCallback == null)
            return;

        // Create a callback that wraps the orchestrator's progress callback
        Action<long>? copierCallback = rowsProcessed =>
            _progressCallback(tableName, rowsProcessed, "Loading");

        // Set on the appropriate copier
        if (_sqlServerToPostgreSqlCopier != null)
            _sqlServerToPostgreSqlCopier.ProgressCallback = copierCallback;
        else if (_sqlServerToSqlServerCopier != null)
            _sqlServerToSqlServerCopier.ProgressCallback = copierCallback;
        else if (_postgreSqlToPostgreSqlCopier != null)
            _postgreSqlToPostgreSqlCopier.ProgressCallback = copierCallback;
        else if (_postgreSqlToSqlServerCopier != null)
            _postgreSqlToSqlServerCopier.ProgressCallback = copierCallback;
    }

    /// <summary>
    /// Format a TimeSpan as h:mm:ss.ff (e.g., 1:23:45.67)
    /// For durations under 1 hour: mm:ss.ff (e.g., 10:58.37)
    /// For durations under 1 minute: ss.ff (e.g., 45.23s)
    /// </summary>
    private static string FormatDuration(TimeSpan duration)
    {
        var totalSeconds = duration.TotalSeconds;
        var fractionalSeconds = totalSeconds - Math.Floor(totalSeconds);
        var centiseconds = (int)(fractionalSeconds * 100);

        if (duration.TotalHours >= 1)
        {
            return $"{(int)duration.TotalHours}h {duration.Minutes:D2}m {duration.Seconds:D2}.{centiseconds:D2}s";
        }
        else if (duration.TotalMinutes >= 1)
        {
            return $"{(int)duration.TotalMinutes}m {duration.Seconds:D2}.{centiseconds:D2}s";
        }
        else
        {
            return $"{duration.TotalSeconds:F2}s";
        }
    }
}
