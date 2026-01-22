using Microsoft.Data.SqlClient;
using Npgsql;
using System.Text.Json;

Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
Console.WriteLine("║     Database Connection Validation Utility                   ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");
Console.WriteLine();

// Find profiles directory - try multiple locations
var currentDir = Directory.GetCurrentDirectory();
string? profilesDir = null;

var searchPaths = new[]
{
    Path.Combine(currentDir, "DatabaseSync", "profiles"),
    Path.Combine(currentDir, "..", "DatabaseSync", "profiles"),
    Path.Combine(currentDir, "profiles"),
    Path.Combine(currentDir, "..", "..", "DatabaseSync", "profiles")
};

foreach (var path in searchPaths)
{
    if (Directory.Exists(path))
    {
        profilesDir = path;
        break;
    }
}

if (profilesDir == null)
{
    Console.WriteLine($"❌ Error: Profiles directory not found");
    Console.WriteLine($"   Searched locations:");
    foreach (var path in searchPaths)
    {
        Console.WriteLine($"     - {Path.GetFullPath(path)}");
    }
    return 1;
}

Console.WriteLine($"📁 Profiles directory: {profilesDir}");
Console.WriteLine();

var profileFiles = Directory.GetFiles(profilesDir, "*.json");

if (profileFiles.Length == 0)
{
    Console.WriteLine("⚠️  No profile JSON files found");
    return 1;
}

Console.WriteLine($"Found {profileFiles.Length} profile(s)\n");

int totalTests = 0;
int passedTests = 0;
var failedConnections = new List<string>();

foreach (var profileFile in profileFiles.OrderBy(f => f))
{
    var profileName = Path.GetFileNameWithoutExtension(profileFile);
    Console.WriteLine($"═══ {profileName} ═══");

    try
    {
        var json = await File.ReadAllTextAsync(profileFile);
        var profile = JsonSerializer.Deserialize<JsonElement>(json);

        // Test source connection
        var sourceConn = profile.GetProperty("SourceConnection");
        var sourceType = sourceConn.GetProperty("Type").GetString() ?? "";
        var sourceConnStr = sourceConn.GetProperty("ConnectionString").GetString() ?? "";

        Console.Write($"  Source ({sourceType}): ");
        totalTests++;

        var (sourceOk, sourceInfo, sourceError) = await TestConnectionAsync(sourceType, sourceConnStr);
        if (sourceOk)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"✓ {sourceInfo}");
            Console.ResetColor();
            passedTests++;
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"✗ FAILED");
            Console.WriteLine($"    {sourceError}");
            Console.ResetColor();
            failedConnections.Add($"{profileName} - Source");
        }

        // Test target connection
        var targetConn = profile.GetProperty("TargetConnection");
        var targetType = targetConn.GetProperty("Type").GetString() ?? "";
        var targetConnStr = targetConn.GetProperty("ConnectionString").GetString() ?? "";

        Console.Write($"  Target ({targetType}): ");
        totalTests++;

        var (targetOk, targetInfo, targetError) = await TestConnectionAsync(targetType, targetConnStr);
        if (targetOk)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"✓ {targetInfo}");
            Console.ResetColor();
            passedTests++;
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"✗ FAILED");
            Console.WriteLine($"    {targetError}");
            Console.ResetColor();
            failedConnections.Add($"{profileName} - Target");
        }
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"✗ ERROR parsing profile: {ex.Message}");
        Console.ResetColor();
        failedConnections.Add($"{profileName} - Parse Error");
    }

    Console.WriteLine();
}

// Summary
Console.WriteLine("╔══════════════════════════════════════════════════════════════╗");
Console.Write("║  ");

if (passedTests == totalTests)
{
    Console.ForegroundColor = ConsoleColor.Green;
    Console.Write($"✓ All {totalTests} connections validated successfully!");
    Console.ResetColor();
}
else
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Write($"Summary: {passedTests}/{totalTests} passed, {totalTests - passedTests} failed");
    Console.ResetColor();
}

// Pad to 60 chars then add the closing
var summaryText = passedTests == totalTests ?
    $"All {totalTests} connections validated successfully!" :
    $"Summary: {passedTests}/{totalTests} passed, {totalTests - passedTests} failed";
var padding = Math.Max(0, 60 - summaryText.Length);
Console.WriteLine(new string(' ', padding) + "║");

Console.WriteLine("╚══════════════════════════════════════════════════════════════╝");

if (failedConnections.Any())
{
    Console.WriteLine();
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine("Failed connections:");
    foreach (var failed in failedConnections)
    {
        Console.WriteLine($"  ✗ {failed}");
    }
    Console.ResetColor();
}

return passedTests == totalTests ? 0 : 1;

static async Task<(bool success, string info, string error)> TestConnectionAsync(string dbType, string connStr)
{
    try
    {
        if (dbType.Equals("SqlServer", StringComparison.OrdinalIgnoreCase) ||
            dbType.Equals("mssql", StringComparison.OrdinalIgnoreCase))
        {
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            await using var cmd = new SqlCommand("SELECT SUSER_NAME(), @@VERSION", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            var user = reader.GetString(0);
            var version = reader.GetString(1).Split('\n')[0].Trim();

            var info = $"{conn.Database} @ {conn.DataSource} (user: {user})";
            return (true, info, "");
        }
        else if (dbType.Equals("PostgreSql", StringComparison.OrdinalIgnoreCase) ||
                 dbType.Equals("postgres", StringComparison.OrdinalIgnoreCase) ||
                 dbType.Equals("pgsql", StringComparison.OrdinalIgnoreCase))
        {
            await using var conn = new NpgsqlConnection(connStr);
            await conn.OpenAsync();

            await using var cmd = new NpgsqlCommand("SELECT current_user, version()", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            var user = reader.GetString(0);
            var version = reader.GetString(1).Split('\n')[0].Trim();

            var info = $"{conn.Database} @ {conn.Host}:{conn.Port} (user: {user})";
            return (true, info, "");
        }
        else
        {
            return (false, "", $"Unknown database type: {dbType}");
        }
    }
    catch (Exception ex)
    {
        // Extract the most relevant error message
        var errorMsg = ex.InnerException?.Message ?? ex.Message;
        errorMsg = errorMsg.Split('\n')[0]; // Take only first line
        if (errorMsg.Length > 100)
        {
            errorMsg = errorMsg.Substring(0, 97) + "...";
        }
        return (false, "", errorMsg);
    }
}
