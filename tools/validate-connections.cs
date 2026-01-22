#!/usr/bin/env dotnet script

// Simple script to validate database connections
// Usage: dotnet script validate-connections.cs

using System;
using System.IO;
using System.Text.Json;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Npgsql;

Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
Console.WriteLine("║     Database Connection Validation                           ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");
Console.WriteLine();

// Read appsettings.json
var appSettingsPath = Path.Combine(AppContext.BaseDirectory, "DatabaseSync", "appsettings.json");
if (!File.Exists(appSettingsPath))
{
    appSettingsPath = Path.Combine(AppContext.BaseDirectory, "appsettings.json");
}

if (!File.Exists(appSettingsPath))
{
    Console.WriteLine("Error: appsettings.json not found");
    return 1;
}

Console.WriteLine($"Loading configuration from: {appSettingsPath}");
Console.WriteLine();

// Find and read profile JSON files
var profilesDir = Path.Combine(Path.GetDirectoryName(appSettingsPath)!, "profiles");
if (!Directory.Exists(profilesDir))
{
    Console.WriteLine($"Error: Profiles directory not found: {profilesDir}");
    return 1;
}

var profileFiles = Directory.GetFiles(profilesDir, "*.json");
Console.WriteLine($"Found {profileFiles.Length} profile(s) to validate");
Console.WriteLine();

int totalTests = 0;
int passedTests = 0;
int failedTests = 0;

foreach (var profileFile in profileFiles)
{
    var profileName = Path.GetFileNameWithoutExtension(profileFile);
    Console.WriteLine($"═══ Testing Profile: {profileName} ═══");

    try
    {
        var json = File.ReadAllText(profileFile);
        var profile = JsonSerializer.Deserialize<JsonElement>(json);

        // Test source connection
        var sourceConn = profile.GetProperty("SourceConnection");
        var sourceType = sourceConn.GetProperty("Type").GetString();
        var sourceConnStr = sourceConn.GetProperty("ConnectionString").GetString();

        Console.Write($"  [1/2] Testing SOURCE ({sourceType})... ");
        totalTests++;

        if (await TestConnection(sourceType!, sourceConnStr!, true))
        {
            Console.WriteLine("✓ PASS");
            passedTests++;
        }
        else
        {
            Console.WriteLine("✗ FAIL");
            failedTests++;
        }

        // Test target connection
        var targetConn = profile.GetProperty("TargetConnection");
        var targetType = targetConn.GetProperty("Type").GetString();
        var targetConnStr = targetConn.GetProperty("ConnectionString").GetString();

        Console.Write($"  [2/2] Testing TARGET ({targetType})... ");
        totalTests++;

        if (await TestConnection(targetType!, targetConnStr!, false))
        {
            Console.WriteLine("✓ PASS");
            passedTests++;
        }
        else
        {
            Console.WriteLine("✗ FAIL");
            failedTests++;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"✗ ERROR: {ex.Message}");
        failedTests += 2;
        totalTests += 2;
    }

    Console.WriteLine();
}

Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
Console.WriteLine($"║  Summary: {passedTests}/{totalTests} Passed, {failedTests} Failed");
Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

return failedTests == 0 ? 0 : 1;

async Task<bool> TestConnection(string dbType, string connStr, bool isSource)
{
    try
    {
        if (dbType.Equals("SqlServer", StringComparison.OrdinalIgnoreCase) ||
            dbType.Equals("mssql", StringComparison.OrdinalIgnoreCase))
        {
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            // Get database name and version
            var db = conn.Database;
            var version = conn.ServerVersion;

            // Get current user
            await using var cmd = new SqlCommand("SELECT SUSER_NAME()", conn);
            var user = await cmd.ExecuteScalarAsync();

            Console.Write($"({db} @ {conn.DataSource} as {user}) ");
            return true;
        }
        else if (dbType.Equals("PostgreSql", StringComparison.OrdinalIgnoreCase) ||
                 dbType.Equals("postgres", StringComparison.OrdinalIgnoreCase) ||
                 dbType.Equals("pgsql", StringComparison.OrdinalIgnoreCase))
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync();

            // Get database and user
            var db = conn.Database;
            var host = conn.Host;

            await using var cmd = new NpgsqlCommand("SELECT current_user", conn);
            var user = await cmd.ExecuteScalarAsync();

            Console.Write($"({db} @ {host} as {user}) ");
            return true;
        }
        else
        {
            Console.Write($"(Unknown DB type: {dbType}) ");
            return false;
        }
    }
    catch (Exception ex)
    {
        Console.Write($"ERROR: {ex.Message.Split('\n')[0]} ");
        return false;
    }
}
