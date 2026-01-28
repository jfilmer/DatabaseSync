using Dapper;
using DatabaseSync.Configuration;
using DatabaseSync.Enums;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DatabaseSync.Services;

/// <summary>
/// Generates complete sync profiles by analyzing source database schema
/// and FK relationships to determine optimal sync order (priorities).
/// </summary>
public class ProfileGenerator
{
    private readonly ILogger<ProfileGenerator> _logger;
    private readonly int _commandTimeout;

    public ProfileGenerator(ILogger<ProfileGenerator> logger, int commandTimeout = 60)
    {
        _logger = logger;
        _commandTimeout = commandTimeout;
    }

    /// <summary>
    /// Generate a complete profile based on source database schema.
    /// Preserves existing table configurations and adds new tables with FK-based priorities.
    /// </summary>
    public async Task<GeneratedProfileResult> GenerateProfileAsync(
        SyncProfile existingProfile,
        string outputDirectory)
    {
        var result = new GeneratedProfileResult
        {
            ProfileName = existingProfile.ProfileName,
            GeneratedFiles = new List<string>()
        };

        try
        {
            // Get all schemas and tables from source
            var schemas = await GetSchemasAndTablesAsync(existingProfile.SourceConnection);

            _logger.LogInformation("Found {SchemaCount} schemas with {TableCount} total tables",
                schemas.Count, schemas.Sum(s => s.Value.Count));

            // Get FK relationships
            var fkRelationships = await GetForeignKeyRelationshipsAsync(existingProfile.SourceConnection);
            _logger.LogInformation("Found {FkCount} foreign key relationships", fkRelationships.Count);

            // Build existing table config lookup (use first config if duplicates exist)
            var existingTableConfigs = existingProfile.Tables
                .GroupBy(t => t.SourceTable.ToLower())
                .ToDictionary(g => g.Key, g => g.First());

            // Ensure output directory exists
            var generatedDir = Path.Combine(outputDirectory, "_generated");
            Directory.CreateDirectory(generatedDir);

            // Generate a profile for each schema
            foreach (var schema in schemas)
            {
                var schemaName = schema.Key;
                var tables = schema.Value;

                if (!tables.Any())
                    continue;

                // Filter FKs for this schema
                var schemaFks = fkRelationships
                    .Where(fk => fk.SourceSchema.Equals(schemaName, StringComparison.OrdinalIgnoreCase))
                    .ToList();

                // Calculate priorities based on FK dependencies
                var tablePriorities = CalculatePriorities(tables, schemaFks);

                // Build table configurations
                var tableConfigs = new List<TableConfig>();
                foreach (var table in tables)
                {
                    var fullTableName = $"{schemaName}.{table}";
                    var priority = tablePriorities.GetValueOrDefault(table, 1);

                    // Check if we have existing config for this table
                    if (existingTableConfigs.TryGetValue(fullTableName.ToLower(), out var existingConfig))
                    {
                        // Preserve existing config but update priority
                        var clonedConfig = CloneTableConfig(existingConfig);
                        clonedConfig.Priority = priority;
                        tableConfigs.Add(clonedConfig);
                    }
                    else
                    {
                        // Create new config with defaults
                        tableConfigs.Add(new TableConfig
                        {
                            SourceTable = fullTableName,
                            TargetTable = fullTableName,
                            Mode = SyncMode.FullRefresh,
                            DeleteMode = DeleteMode.Sync,
                            SyncAllDeletes = true,
                            Priority = priority
                        });
                    }
                }

                // Sort by priority
                tableConfigs = tableConfigs.OrderBy(t => t.Priority).ThenBy(t => t.SourceTable).ToList();

                // Create the schema-specific profile
                var schemaProfile = new SyncProfile
                {
                    ProfileName = $"{existingProfile.ProfileName}-{schemaName}",
                    Description = $"Auto-generated profile for {schemaName} schema (source: {existingProfile.ProfileName})",
                    SourceConnection = existingProfile.SourceConnection,
                    TargetConnection = existingProfile.TargetConnection,
                    Schedule = new ScheduleConfig
                    {
                        Enabled = false, // Disabled by default for generated profiles
                        StartTime = "04:00",
                        IntervalMinutes = 1440,
                        RunImmediatelyOnStart = false
                    },
                    Options = existingProfile.Options ?? new ProfileOptions
                    {
                        MaxParallelTables = 4,
                        CommandTimeoutSeconds = 300,
                        EnableSyncHistory = true,
                        UseHistoryForIncrementalSync = true,
                        StopOnError = true
                    },
                    Tables = tableConfigs
                };

                // Write to file
                var fileName = $"{existingProfile.ProfileName}-{schemaName}-full.json";
                var filePath = Path.Combine(generatedDir, fileName);

                var jsonOptions = new JsonSerializerOptions
                {
                    WriteIndented = true,
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                    Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) }
                };

                var json = JsonSerializer.Serialize(schemaProfile, jsonOptions);
                await File.WriteAllTextAsync(filePath, json);

                result.GeneratedFiles.Add(filePath);
                result.TablesGenerated += tableConfigs.Count;

                _logger.LogInformation(
                    "Generated profile for schema '{Schema}': {TableCount} tables, max priority {MaxPriority}",
                    schemaName, tableConfigs.Count, tableConfigs.Max(t => t.Priority));
            }

            result.Success = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to generate profile for {ProfileName}", existingProfile.ProfileName);
            result.Success = false;
            result.Error = ex.Message;
        }

        return result;
    }

    /// <summary>
    /// Get all schemas and their tables from the source database.
    /// Excludes system tables and schemas.
    /// </summary>
    private async Task<Dictionary<string, List<string>>> GetSchemasAndTablesAsync(ConnectionConfig connection)
    {
        var schemas = new Dictionary<string, List<string>>();

        if (connection.DatabaseType == DatabaseType.PostgreSql)
        {
            await using var conn = new NpgsqlConnection(connection.ConnectionString);
            await conn.OpenAsync();

            var query = @"
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                  AND table_name NOT LIKE 'pg_%'
                  AND table_name NOT IN ('_sync_history', 'db_environment')
                ORDER BY table_schema, table_name";

            var tables = await conn.QueryAsync<(string Schema, string Table)>(query, commandTimeout: _commandTimeout);

            foreach (var (schema, table) in tables)
            {
                if (!schemas.ContainsKey(schema))
                    schemas[schema] = new List<string>();
                schemas[schema].Add(table);
            }
        }
        else if (connection.DatabaseType == DatabaseType.SqlServer)
        {
            await using var conn = new SqlConnection(connection.ConnectionString);
            await conn.OpenAsync();

            var query = @"
                SELECT s.name AS table_schema, t.name AS table_name
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE t.type = 'U'
                  AND s.name NOT IN ('sys', 'guest', 'INFORMATION_SCHEMA')
                  AND t.name NOT LIKE 'sys%'
                  AND t.name NOT IN ('_sync_history', 'db_environment')
                ORDER BY s.name, t.name";

            var tables = await conn.QueryAsync<(string Schema, string Table)>(query, commandTimeout: _commandTimeout);

            foreach (var (schema, table) in tables)
            {
                if (!schemas.ContainsKey(schema))
                    schemas[schema] = new List<string>();
                schemas[schema].Add(table);
            }
        }

        return schemas;
    }

    /// <summary>
    /// Get foreign key relationships from the source database.
    /// </summary>
    private async Task<List<FkRelationship>> GetForeignKeyRelationshipsAsync(ConnectionConfig connection)
    {
        var relationships = new List<FkRelationship>();

        if (connection.DatabaseType == DatabaseType.PostgreSql)
        {
            await using var conn = new NpgsqlConnection(connection.ConnectionString);
            await conn.OpenAsync();

            var query = @"
                SELECT
                    tc.table_schema AS source_schema,
                    tc.table_name AS source_table,
                    ccu.table_schema AS referenced_schema,
                    ccu.table_name AS referenced_table
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                    AND tc.table_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
                GROUP BY tc.table_schema, tc.table_name, ccu.table_schema, ccu.table_name";

            relationships = (await conn.QueryAsync<FkRelationship>(query, commandTimeout: _commandTimeout)).ToList();
        }
        else if (connection.DatabaseType == DatabaseType.SqlServer)
        {
            await using var conn = new SqlConnection(connection.ConnectionString);
            await conn.OpenAsync();

            var query = @"
                SELECT
                    SCHEMA_NAME(fk.schema_id) AS source_schema,
                    OBJECT_NAME(fk.parent_object_id) AS source_table,
                    SCHEMA_NAME(pk.schema_id) AS referenced_schema,
                    OBJECT_NAME(fk.referenced_object_id) AS referenced_table
                FROM sys.foreign_keys fk
                INNER JOIN sys.tables pk ON fk.referenced_object_id = pk.object_id
                WHERE SCHEMA_NAME(fk.schema_id) NOT IN ('sys', 'guest', 'INFORMATION_SCHEMA')";

            relationships = (await conn.QueryAsync<FkRelationship>(query, commandTimeout: _commandTimeout)).ToList();
        }

        return relationships;
    }

    /// <summary>
    /// Calculate priority levels based on FK dependencies using topological sort.
    /// Tables with no dependencies get priority 1, tables depending on those get priority 2, etc.
    /// </summary>
    private Dictionary<string, int> CalculatePriorities(List<string> tables, List<FkRelationship> fkRelationships)
    {
        var priorities = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var tableDependencies = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

        // Initialize all tables with empty dependencies
        foreach (var table in tables)
        {
            tableDependencies[table] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        // Build dependency graph (table -> tables it depends on)
        foreach (var fk in fkRelationships)
        {
            // Only consider FKs within the same schema for priority calculation
            if (!fk.SourceSchema.Equals(fk.ReferencedSchema, StringComparison.OrdinalIgnoreCase))
                continue;

            if (tableDependencies.ContainsKey(fk.SourceTable) && tables.Contains(fk.ReferencedTable, StringComparer.OrdinalIgnoreCase))
            {
                // Source table depends on referenced table (referenced must sync first)
                // Skip self-references
                if (!fk.SourceTable.Equals(fk.ReferencedTable, StringComparison.OrdinalIgnoreCase))
                {
                    tableDependencies[fk.SourceTable].Add(fk.ReferencedTable);
                }
            }
        }

        // Topological sort to assign priorities
        var assigned = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int currentPriority = 1;

        while (assigned.Count < tables.Count)
        {
            // Find tables whose dependencies are all assigned
            var readyTables = tables
                .Where(t => !assigned.Contains(t))
                .Where(t => tableDependencies[t].All(dep => assigned.Contains(dep)))
                .ToList();

            if (!readyTables.Any())
            {
                // Circular dependency detected - assign remaining tables to current priority
                _logger.LogWarning("Circular FK dependency detected. Assigning remaining tables to priority {Priority}",
                    currentPriority);

                foreach (var table in tables.Where(t => !assigned.Contains(t)))
                {
                    priorities[table] = currentPriority;
                    assigned.Add(table);
                }
                break;
            }

            foreach (var table in readyTables)
            {
                priorities[table] = currentPriority;
                assigned.Add(table);
            }

            currentPriority++;
        }

        return priorities;
    }

    private TableConfig CloneTableConfig(TableConfig source)
    {
        return new TableConfig
        {
            SourceTable = source.SourceTable,
            TargetTable = source.TargetTable,
            Mode = source.Mode,
            TimestampColumn = source.TimestampColumn,
            FallbackTimestampColumn = source.FallbackTimestampColumn,
            LookbackHours = source.LookbackHours,
            Priority = source.Priority,
            DeleteMode = source.DeleteMode,
            SyncAllDeletes = source.SyncAllDeletes,
            CreateIfMissing = source.CreateIfMissing,
            SourceFilter = source.SourceFilter
        };
    }
}

/// <summary>
/// Represents a foreign key relationship between tables.
/// </summary>
public class FkRelationship
{
    public string SourceSchema { get; set; } = "";
    public string SourceTable { get; set; } = "";
    public string ReferencedSchema { get; set; } = "";
    public string ReferencedTable { get; set; } = "";
}

/// <summary>
/// Result of profile generation.
/// </summary>
public class GeneratedProfileResult
{
    public bool Success { get; set; }
    public string ProfileName { get; set; } = "";
    public List<string> GeneratedFiles { get; set; } = new();
    public int TablesGenerated { get; set; }
    public string? Error { get; set; }
}
