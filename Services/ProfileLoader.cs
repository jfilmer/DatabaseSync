using System.Text.Json;
using System.Text.Json.Serialization;
using DatabaseSync.Configuration;
using SerilogLogger = Serilog.ILogger;

namespace DatabaseSync.Services;

/// <summary>
/// Loads sync profiles from external JSON files and merges with inline configuration
/// </summary>
public static class ProfileLoader
{
    /// <summary>
    /// Loads profiles from inline config and external files, with last-wins deduplication
    /// </summary>
    /// <param name="baseConfig">Base configuration with inline profiles</param>
    /// <param name="basePath">Base path for relative directory resolution</param>
    /// <param name="environment">Current environment (e.g., Development, Production)</param>
    /// <param name="logger">Logger for diagnostics</param>
    /// <returns>Combined list of profiles with duplicates resolved</returns>
    public static List<SyncProfile> LoadProfiles(
        SyncServiceConfig baseConfig,
        string basePath,
        string environment,
        SerilogLogger logger)
    {
        var allProfiles = new List<SyncProfile>();

        // Start with inline profiles from appsettings.json (if any)
        if (baseConfig.Profiles?.Count > 0)
        {
            logger.Information("Loaded {Count} inline profiles from appsettings.json", baseConfig.Profiles.Count);
            allProfiles.AddRange(baseConfig.Profiles);
        }

        // Load external profiles if enabled
        if (baseConfig.EnableExternalProfiles)
        {
            logger.Information("External profiles enabled. ProfilesDirectory: {Directory}", baseConfig.ProfilesDirectory);

            // Load base profiles from profiles/ directory
            var baseProfilesDir = Path.Combine(basePath, baseConfig.ProfilesDirectory);
            var baseProfiles = LoadFromDirectory(baseProfilesDir, logger, "base");
            allProfiles.AddRange(baseProfiles);

            // Load environment-specific profiles from profiles.{Environment}/ directory
            var envProfilesDir = Path.Combine(basePath, $"{baseConfig.ProfilesDirectory}.{environment}");
            var envProfiles = LoadFromDirectory(envProfilesDir, logger, environment);
            allProfiles.AddRange(envProfiles);
        }
        else
        {
            logger.Information("External profiles disabled. Using inline profiles only.");
        }

        // Deduplicate profiles (last-loaded wins)
        var deduplicated = DeduplicateProfiles(allProfiles, logger);

        logger.Information("Total profiles loaded: {Count}", deduplicated.Count);
        return deduplicated;
    }

    /// <summary>
    /// Loads profiles from a directory of JSON files
    /// </summary>
    private static List<SyncProfile> LoadFromDirectory(string directory, SerilogLogger logger, string source)
    {
        var profiles = new List<SyncProfile>();

        if (!Directory.Exists(directory))
        {
            logger.Warning("Profile directory not found: {Directory} (source: {Source})", directory, source);
            return profiles;
        }

        var jsonFiles = Directory.GetFiles(directory, "*.json");

        if (jsonFiles.Length == 0)
        {
            logger.Warning("No profile files found in {Directory} (source: {Source})", directory, source);
            return profiles;
        }

        logger.Information("Scanning {Directory} for profile files (source: {Source})", directory, source);

        foreach (var filePath in jsonFiles)
        {
            try
            {
                var fileName = Path.GetFileName(filePath);
                var json = File.ReadAllText(filePath);

                var options = new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    ReadCommentHandling = JsonCommentHandling.Skip,
                    AllowTrailingCommas = true
                };
                options.Converters.Add(new JsonStringEnumConverter());

                var profile = JsonSerializer.Deserialize<SyncProfile>(json, options);

                if (profile == null)
                {
                    logger.Error("Failed to deserialize profile from {File}: result was null", fileName);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(profile.ProfileName))
                {
                    logger.Error("Profile in {File} is missing ProfileName property", fileName);
                    continue;
                }

                logger.Information("Loaded profile '{ProfileName}' from {File} (source: {Source})",
                    profile.ProfileName, fileName, source);
                profiles.Add(profile);
            }
            catch (JsonException ex)
            {
                logger.Error(ex, "Invalid JSON in profile file {File}: {Error}",
                    Path.GetFileName(filePath), ex.Message);
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Error loading profile from {File}: {Error}",
                    Path.GetFileName(filePath), ex.Message);
            }
        }

        return profiles;
    }

    /// <summary>
    /// Removes duplicate profiles, keeping the last occurrence of each ProfileName
    /// </summary>
    private static List<SyncProfile> DeduplicateProfiles(List<SyncProfile> profiles, SerilogLogger logger)
    {
        var profilesByName = new Dictionary<string, SyncProfile>(StringComparer.OrdinalIgnoreCase);
        var duplicates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var profile in profiles)
        {
            if (profilesByName.ContainsKey(profile.ProfileName))
            {
                duplicates.Add(profile.ProfileName);
            }

            profilesByName[profile.ProfileName] = profile; // Last wins
        }

        if (duplicates.Count > 0)
        {
            logger.Warning("Found duplicate profile names (last-loaded wins): {Duplicates}",
                string.Join(", ", duplicates));
        }

        return profilesByName.Values.ToList();
    }
}
