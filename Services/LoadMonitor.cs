using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DatabaseSync.Services;

/// <summary>
/// Monitors database server load and determines if sync should be throttled
/// </summary>
public class LoadMonitor
{
    private readonly ILogger<LoadMonitor> _logger;
    private bool _permissionWarningLogged = false;

    public LoadMonitor(ILogger<LoadMonitor> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Get current CPU utilization from SQL Server
    /// Returns null if unable to query (permissions, connection issues, etc.)
    /// </summary>
    public async Task<int?> GetSqlServerCpuPercentAsync(string connectionString)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            // Query the ring buffer for recent CPU usage
            // This requires VIEW SERVER STATE permission
            const string query = @"
                SELECT TOP 1
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
                    record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization
                FROM (
                    SELECT CONVERT(XML, record) AS record
                    FROM sys.dm_os_ring_buffers
                    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                    AND record LIKE '%<SystemHealth>%'
                ) AS x
                ORDER BY record.value('(./Record/@id)[1]', 'int') DESC";

            await using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 10; // Short timeout for monitoring query

            await using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var systemIdle = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                var sqlUtilization = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);

                // Total CPU usage = 100 - idle (this includes SQL Server + other processes)
                // Or just use SQL Server's own utilization
                var totalCpu = 100 - systemIdle;

                _logger.LogDebug("SQL Server CPU: {SqlCpu}%, System Total: {TotalCpu}%, Idle: {Idle}%",
                    sqlUtilization, totalCpu, systemIdle);

                return totalCpu; // Return total system CPU, not just SQL Server
            }

            return null;
        }
        catch (SqlException ex) when (ex.Number == 297 || ex.Message.Contains("VIEW SERVER STATE"))
        {
            // Permission denied - log once and return null
            if (!_permissionWarningLogged)
            {
                _logger.LogWarning(
                    "Cannot monitor SQL Server CPU: VIEW SERVER STATE permission required. " +
                    "Load throttling will be disabled for this source. " +
                    "Grant permission with: GRANT VIEW SERVER STATE TO [username]");
                _permissionWarningLogged = true;
            }
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error querying SQL Server CPU utilization");
            return null;
        }
    }

    /// <summary>
    /// Get active connection count from SQL Server
    /// Returns null if unable to query
    /// </summary>
    public async Task<int?> GetSqlServerActiveQueriesAsync(string connectionString)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            const string query = @"
                SELECT COUNT(*)
                FROM sys.dm_exec_requests
                WHERE status = 'running'
                AND session_id != @@SPID";

            await using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 10;

            var result = await command.ExecuteScalarAsync();
            return result as int?;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error querying SQL Server active queries");
            return null;
        }
    }

    /// <summary>
    /// Get current load from PostgreSQL (active connections)
    /// Returns null if unable to query
    /// </summary>
    public async Task<int?> GetPostgreSqlActiveConnectionsAsync(string connectionString)
    {
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            const string query = @"
                SELECT COUNT(*)
                FROM pg_stat_activity
                WHERE state = 'active'
                AND pid != pg_backend_pid()";

            await using var command = new NpgsqlCommand(query, connection);
            command.CommandTimeout = 10;

            var result = await command.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Error querying PostgreSQL active connections");
            return null;
        }
    }

    /// <summary>
    /// Check if the source database is under heavy load and sync should wait
    /// </summary>
    public async Task<LoadCheckResult> CheckSourceLoadAsync(
        string connectionString,
        Enums.DatabaseType databaseType,
        int maxCpuPercent = 60,
        int maxActiveQueries = 50)
    {
        if (databaseType == Enums.DatabaseType.SqlServer)
        {
            var cpu = await GetSqlServerCpuPercentAsync(connectionString);

            if (cpu == null)
            {
                // Can't check - allow sync to proceed
                return new LoadCheckResult
                {
                    CanProceed = true,
                    Reason = "Unable to check server load (permissions or connectivity)",
                    CpuPercent = null
                };
            }

            if (cpu > maxCpuPercent)
            {
                return new LoadCheckResult
                {
                    CanProceed = false,
                    Reason = $"Server CPU at {cpu}% (threshold: {maxCpuPercent}%)",
                    CpuPercent = cpu
                };
            }

            return new LoadCheckResult
            {
                CanProceed = true,
                Reason = $"Server CPU at {cpu}%",
                CpuPercent = cpu
            };
        }
        else if (databaseType == Enums.DatabaseType.PostgreSql)
        {
            var activeConnections = await GetPostgreSqlActiveConnectionsAsync(connectionString);

            if (activeConnections == null)
            {
                return new LoadCheckResult
                {
                    CanProceed = true,
                    Reason = "Unable to check server load",
                    CpuPercent = null
                };
            }

            if (activeConnections > maxActiveQueries)
            {
                return new LoadCheckResult
                {
                    CanProceed = false,
                    Reason = $"Server has {activeConnections} active queries (threshold: {maxActiveQueries})",
                    ActiveQueries = activeConnections
                };
            }

            return new LoadCheckResult
            {
                CanProceed = true,
                Reason = $"Server has {activeConnections} active queries",
                ActiveQueries = activeConnections
            };
        }

        return new LoadCheckResult { CanProceed = true, Reason = "Unknown database type" };
    }

    /// <summary>
    /// Wait until server load is below threshold, with timeout
    /// </summary>
    public async Task<bool> WaitForLowLoadAsync(
        string connectionString,
        Enums.DatabaseType databaseType,
        int maxCpuPercent = 60,
        int checkIntervalSeconds = 30,
        int maxWaitMinutes = 30,
        CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var maxWait = TimeSpan.FromMinutes(maxWaitMinutes);
        var checkInterval = TimeSpan.FromSeconds(checkIntervalSeconds);
        var waitLogged = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await CheckSourceLoadAsync(connectionString, databaseType, maxCpuPercent);

            if (result.CanProceed)
            {
                if (waitLogged)
                {
                    _logger.LogInformation("Server load decreased - resuming sync. {Reason}", result.Reason);
                }
                return true;
            }

            // Check timeout
            if (DateTime.UtcNow - startTime > maxWait)
            {
                _logger.LogWarning(
                    "Server load still high after {Minutes} minutes - proceeding anyway. {Reason}",
                    maxWaitMinutes, result.Reason);
                return false; // Timed out, but let sync proceed
            }

            if (!waitLogged)
            {
                _logger.LogWarning(
                    "Server under heavy load - pausing sync. {Reason}. Will retry every {Interval}s (max wait: {MaxWait}m)",
                    result.Reason, checkIntervalSeconds, maxWaitMinutes);
                waitLogged = true;
            }

            await Task.Delay(checkInterval, cancellationToken);
        }

        return false;
    }
}

/// <summary>
/// Result of a load check
/// </summary>
public class LoadCheckResult
{
    public bool CanProceed { get; set; }
    public string Reason { get; set; } = string.Empty;
    public int? CpuPercent { get; set; }
    public int? ActiveQueries { get; set; }
}
