using DatabaseSync.Configuration;
using DatabaseSync.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Extensions.Logging;

namespace DatabaseSync;

/// <summary>
/// Standalone utility to test all database connections
/// Usage: dotnet run --project DatabaseSync TestConnections
/// </summary>
public class TestConnections
{
    public static async Task<int> Main(string[] args)
    {
        // Setup console logging
        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Information()
            .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}")
            .CreateLogger();

        var loggerFactory = new SerilogLoggerFactory(Log.Logger);
        var logger = loggerFactory.CreateLogger<TestConnections>();

        try
        {
            logger.LogInformation("╔══════════════════════════════════════════════════════════════╗");
            logger.LogInformation("║     Database Connection Validation Utility                   ║");
            logger.LogInformation("╚══════════════════════════════════════════════════════════════╝");
            logger.LogInformation("");

            // Load configuration
            var basePath = AppContext.BaseDirectory;
            var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                ?? "Production";

            logger.LogInformation("Loading configuration from: {BasePath}", basePath);
            logger.LogInformation("Environment: {Environment}", environment);
            logger.LogInformation("");

            var configuration = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
                .AddJsonFile($"appsettings.{environment}.json", optional: true, reloadOnChange: false)
                .Build();

            var config = configuration.GetSection("SyncService").Get<SyncServiceConfig>();

            if (config == null)
            {
                logger.LogError("Failed to load SyncService configuration from appsettings.json");
                return 1;
            }

            // Load external profiles if enabled
            if (config.EnableExternalProfiles)
            {
                var profileLoader = new ProfileLoader(loggerFactory.CreateLogger<ProfileLoader>());
                config = await profileLoader.LoadExternalProfilesAsync(config, basePath, environment);
            }

            logger.LogInformation("Loaded {ProfileCount} profile(s)", config.Profiles.Count);
            logger.LogInformation("");

            // Validate configuration syntax first
            logger.LogInformation("Step 1: Validating configuration syntax...");
            var syntaxValidation = ConfigurationValidator.Validate(config);

            if (syntaxValidation.Errors.Any())
            {
                logger.LogError("Configuration validation failed with {Count} error(s):", syntaxValidation.Errors.Count);
                foreach (var error in syntaxValidation.Errors)
                {
                    logger.LogError("  ✗ {Error}", error);
                }
                logger.LogInformation("");
                return 1;
            }

            if (syntaxValidation.Warnings.Any())
            {
                logger.LogWarning("Configuration has {Count} warning(s):", syntaxValidation.Warnings.Count);
                foreach (var warning in syntaxValidation.Warnings)
                {
                    logger.LogWarning("  ⚠ {Warning}", warning);
                }
            }

            logger.LogInformation("✓ Configuration syntax is valid");
            logger.LogInformation("");

            // Test actual database connections
            logger.LogInformation("Step 2: Testing database connections...");
            logger.LogInformation("");

            var validator = new ConnectionValidator(loggerFactory.CreateLogger<ConnectionValidator>());
            var report = await validator.ValidateAllConnectionsAsync(config);

            logger.LogInformation("");
            report.PrintSummary(logger);

            return report.AllValid ? 0 : 1;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unhandled exception during connection validation");
            return 1;
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}
