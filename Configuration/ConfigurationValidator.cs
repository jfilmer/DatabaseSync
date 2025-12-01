namespace DatabaseSync.Configuration;

/// <summary>
/// Validates SyncServiceConfig and returns a list of errors/warnings
/// </summary>
public static class ConfigurationValidator
{
    public static ConfigurationValidationResult Validate(SyncServiceConfig config)
    {
        var result = new ConfigurationValidationResult();

        // Check if config was loaded at all
        if (config == null)
        {
            result.Errors.Add("Configuration failed to load. Check appsettings.json syntax.");
            return result;
        }

        // Check HTTP port
        if (config.HttpPort <= 0 || config.HttpPort > 65535)
        {
            result.Errors.Add($"Invalid HttpPort: {config.HttpPort}. Must be between 1 and 65535.");
        }

        // Check profiles
        if (!config.Profiles.Any())
        {
            result.Warnings.Add("No profiles configured. Add profiles to appsettings.json to enable sync.");
        }
        else
        {
            // Validate each profile
            foreach (var profile in config.Profiles)
            {
                ValidateProfile(profile, result);
            }

            // Check for duplicate profile names
            var duplicates = config.Profiles
                .GroupBy(p => p.ProfileName, StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            foreach (var dup in duplicates)
            {
                result.Errors.Add($"Duplicate profile name: '{dup}'");
            }
        }

        // Validate blackout window
        if (config.BlackoutWindow?.Enabled == true)
        {
            if (string.IsNullOrEmpty(config.BlackoutWindow.StartTime))
            {
                result.Errors.Add("BlackoutWindow.StartTime is required when BlackoutWindow is enabled.");
            }
            else if (!TimeOnly.TryParse(config.BlackoutWindow.StartTime, out _))
            {
                result.Errors.Add($"BlackoutWindow.StartTime '{config.BlackoutWindow.StartTime}' is not a valid time format (use HH:mm).");
            }

            if (string.IsNullOrEmpty(config.BlackoutWindow.EndTime))
            {
                result.Errors.Add("BlackoutWindow.EndTime is required when BlackoutWindow is enabled.");
            }
            else if (!TimeOnly.TryParse(config.BlackoutWindow.EndTime, out _))
            {
                result.Errors.Add($"BlackoutWindow.EndTime '{config.BlackoutWindow.EndTime}' is not a valid time format (use HH:mm).");
            }
        }

        // Validate load throttling
        if (config.LoadThrottling?.Enabled == true)
        {
            if (config.LoadThrottling.MaxCpuPercent <= 0 || config.LoadThrottling.MaxCpuPercent > 100)
            {
                result.Errors.Add($"LoadThrottling.MaxCpuPercent must be between 1 and 100, got {config.LoadThrottling.MaxCpuPercent}.");
            }
        }

        return result;
    }

    private static void ValidateProfile(SyncProfile profile, ConfigurationValidationResult result)
    {
        var prefix = $"Profile '{profile.ProfileName}'";

        // Profile name
        if (string.IsNullOrWhiteSpace(profile.ProfileName))
        {
            result.Errors.Add("A profile has an empty or missing ProfileName.");
            prefix = "A profile";
        }

        // Source connection
        if (profile.SourceConnection == null)
        {
            result.Errors.Add($"{prefix}: SourceConnection is missing.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(profile.SourceConnection.ConnectionString))
            {
                result.Errors.Add($"{prefix}: SourceConnection.ConnectionString is missing.");
            }
            else
            {
                ValidateConnectionString(profile.SourceConnection, $"{prefix} SourceConnection", result);
            }
        }

        // Target connection
        if (profile.TargetConnection == null)
        {
            result.Errors.Add($"{prefix}: TargetConnection is missing.");
        }
        else
        {
            if (string.IsNullOrWhiteSpace(profile.TargetConnection.ConnectionString))
            {
                result.Errors.Add($"{prefix}: TargetConnection.ConnectionString is missing.");
            }
            else
            {
                ValidateConnectionString(profile.TargetConnection, $"{prefix} TargetConnection", result);
            }
        }

        // Tables
        if (!profile.Tables.Any())
        {
            result.Warnings.Add($"{prefix}: No tables configured.");
        }
        else
        {
            foreach (var table in profile.Tables)
            {
                ValidateTable(table, prefix, result);
            }

            // Check for duplicate table names
            var duplicates = profile.Tables
                .GroupBy(t => t.SourceTable, StringComparer.OrdinalIgnoreCase)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            foreach (var dup in duplicates)
            {
                result.Warnings.Add($"{prefix}: Duplicate table '{dup}' (will sync multiple times).");
            }
        }

        // Schedule
        if (profile.Schedule?.Enabled == true)
        {
            if (profile.Schedule.IntervalMinutes <= 0)
            {
                result.Errors.Add($"{prefix}: Schedule.IntervalMinutes must be greater than 0.");
            }

            if (!string.IsNullOrEmpty(profile.Schedule.StartTime) && !TimeOnly.TryParse(profile.Schedule.StartTime, out _))
            {
                result.Errors.Add($"{prefix}: Schedule.StartTime '{profile.Schedule.StartTime}' is not a valid time format (use HH:mm).");
            }
        }

        // Options
        if (profile.Options != null)
        {
            if (profile.Options.MaxParallelTables <= 0)
            {
                result.Errors.Add($"{prefix}: Options.MaxParallelTables must be greater than 0.");
            }

            if (profile.Options.CommandTimeoutSeconds <= 0)
            {
                result.Errors.Add($"{prefix}: Options.CommandTimeoutSeconds must be greater than 0.");
            }
        }
    }

    private static void ValidateConnectionString(ConnectionConfig connection, string prefix, ConfigurationValidationResult result)
    {
        var connStr = connection.ConnectionString;
        var dbType = connection.DatabaseType;

        // Determine database type from connection string if Type not specified
        var isSqlServer = dbType == Enums.DatabaseType.SqlServer ||
                          connStr.Contains("Server=", StringComparison.OrdinalIgnoreCase) ||
                          connStr.Contains("Data Source=", StringComparison.OrdinalIgnoreCase);

        var isPostgreSql = dbType == Enums.DatabaseType.PostgreSql ||
                           connStr.Contains("Host=", StringComparison.OrdinalIgnoreCase);

        if (isSqlServer)
        {
            ValidateSqlServerConnectionString(connStr, prefix, result);
        }
        else if (isPostgreSql)
        {
            ValidatePostgreSqlConnectionString(connStr, prefix, result);
        }
    }

    private static void ValidateSqlServerConnectionString(string connStr, string prefix, ConfigurationValidationResult result)
    {
        // Parse connection string into key-value pairs
        var parts = connStr.Split(';', StringSplitOptions.RemoveEmptyEntries);
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var keyValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var part in parts)
        {
            var idx = part.IndexOf('=');
            if (idx > 0)
            {
                var key = part.Substring(0, idx).Trim();
                var value = part.Substring(idx + 1).Trim();
                keys.Add(key);
                keyValues[key] = value;
            }
        }

        // Check for required components
        var hasServer = keys.Contains("Server") || keys.Contains("Data Source");
        var hasDatabase = keys.Contains("Database") || keys.Contains("Initial Catalog");
        var hasAuth = keys.Contains("User Id") || keys.Contains("User ID") ||
                      keys.Contains("Integrated Security") || keys.Contains("Trusted_Connection");

        if (!hasServer)
        {
            result.Errors.Add($"{prefix}: Missing 'Server' or 'Data Source' in connection string.");
        }

        if (!hasDatabase)
        {
            result.Errors.Add($"{prefix}: Missing 'Database' or 'Initial Catalog' in connection string.");
        }

        if (!hasAuth)
        {
            result.Errors.Add($"{prefix}: Missing authentication. Use 'User Id' and 'Password', or 'Integrated Security=True'.");
        }

        // Check for common typos in SQL Server connection strings
        var validSqlServerKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Server", "Data Source", "Database", "Initial Catalog",
            "User Id", "User ID", "UID", "Password", "PWD",
            "Integrated Security", "Trusted_Connection",
            "TrustServerCertificate", "Trust Server Certificate",
            "Encrypt", "Connection Timeout", "Connect Timeout", "Command Timeout",
            "Application Name", "Workstation ID", "Packet Size",
            "MultipleActiveResultSets", "MARS", "Max Pool Size", "Min Pool Size",
            "Pooling", "MultiSubnetFailover", "ApplicationIntent"
        };

        foreach (var key in keys)
        {
            if (!validSqlServerKeys.Contains(key))
            {
                // Check for likely typos
                if (key.Contains("Id", StringComparison.OrdinalIgnoreCase) &&
                    !key.Equals("User Id", StringComparison.OrdinalIgnoreCase) &&
                    !key.Equals("User ID", StringComparison.OrdinalIgnoreCase) &&
                    !key.Equals("UID", StringComparison.OrdinalIgnoreCase) &&
                    !key.Equals("Workstation ID", StringComparison.OrdinalIgnoreCase))
                {
                    result.Errors.Add($"{prefix}: Unrecognized key '{key}' in connection string. Did you mean 'User Id'?");
                }
                else
                {
                    result.Warnings.Add($"{prefix}: Unrecognized key '{key}' in connection string.");
                }
            }
        }

        // Check if User Id is present but Password is missing (and not using integrated auth)
        var hasUserId = keys.Contains("User Id") || keys.Contains("User ID") || keys.Contains("UID");
        var hasPassword = keys.Contains("Password") || keys.Contains("PWD");
        var hasIntegratedAuth = keys.Contains("Integrated Security") || keys.Contains("Trusted_Connection");

        if (hasUserId && !hasPassword && !hasIntegratedAuth)
        {
            result.Errors.Add($"{prefix}: 'User Id' specified but 'Password' is missing.");
        }
    }

    private static void ValidatePostgreSqlConnectionString(string connStr, string prefix, ConfigurationValidationResult result)
    {
        // Parse connection string into key-value pairs
        var parts = connStr.Split(';', StringSplitOptions.RemoveEmptyEntries);
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var part in parts)
        {
            var idx = part.IndexOf('=');
            if (idx > 0)
            {
                var key = part.Substring(0, idx).Trim();
                keys.Add(key);
            }
        }

        // Check for required components
        var hasHost = keys.Contains("Host") || keys.Contains("Server");
        var hasDatabase = keys.Contains("Database");
        var hasUsername = keys.Contains("Username") || keys.Contains("User Id");

        if (!hasHost)
        {
            result.Errors.Add($"{prefix}: Missing 'Host' in connection string.");
        }

        if (!hasDatabase)
        {
            result.Errors.Add($"{prefix}: Missing 'Database' in connection string.");
        }

        if (!hasUsername)
        {
            result.Errors.Add($"{prefix}: Missing 'Username' in connection string.");
        }

        // Check for common typos
        var validPostgresKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "Host", "Server", "Port", "Database", "Username", "User Id",
            "Password", "SSL Mode", "SslMode", "Trust Server Certificate",
            "Timeout", "Command Timeout", "Connection Idle Lifetime",
            "Pooling", "Minimum Pool Size", "Maximum Pool Size",
            "Application Name", "Search Path", "Include Error Detail"
        };

        foreach (var key in keys)
        {
            if (!validPostgresKeys.Contains(key))
            {
                result.Warnings.Add($"{prefix}: Unrecognized key '{key}' in connection string.");
            }
        }
    }

    private static void ValidateTable(TableConfig table, string profilePrefix, ConfigurationValidationResult result)
    {
        var prefix = $"{profilePrefix}, table '{table.SourceTable}'";

        if (string.IsNullOrWhiteSpace(table.SourceTable))
        {
            result.Errors.Add($"{profilePrefix}: A table has an empty or missing SourceTable.");
            return;
        }

        // Incremental mode requires timestamp column
        if (table.Mode == Enums.SyncMode.Incremental)
        {
            if (string.IsNullOrWhiteSpace(table.TimestampColumn))
            {
                result.Errors.Add($"{prefix}: TimestampColumn is required for Incremental mode.");
            }
        }

        // LookbackHours validation
        if (table.LookbackHours < 0)
        {
            result.Errors.Add($"{prefix}: LookbackHours cannot be negative.");
        }

        // SyncAllDeletes warning for chained syncs
        if (table.SyncAllDeletes && table.Mode == Enums.SyncMode.Incremental)
        {
            // This is valid but worth noting
        }

        // Priority validation
        if (table.Priority < 0)
        {
            result.Warnings.Add($"{prefix}: Priority is negative, which may cause unexpected ordering.");
        }
    }
}

/// <summary>
/// Result of configuration validation
/// </summary>
public class ConfigurationValidationResult
{
    public List<string> Errors { get; } = new();
    public List<string> Warnings { get; } = new();

    public bool IsValid => !Errors.Any();
    public bool HasWarnings => Warnings.Any();
    public bool HasIssues => Errors.Any() || Warnings.Any();
}
