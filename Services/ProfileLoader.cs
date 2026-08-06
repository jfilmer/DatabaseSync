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

        // Substitute ${PLACEHOLDER} tokens in connection strings from environment / secrets file.
        // Runs AFTER dedup (so only surviving profiles are resolved) and BEFORE validation in
        // Program.cs, so an unresolved placeholder surfaces as a validation error rather than a
        // connection attempt with a literal "${NAME}" as the password.
        ResolveSecrets(deduplicated, baseConfig, basePath, logger);

        logger.Information("Total profiles loaded: {Count}", deduplicated.Count);
        return deduplicated;
    }

    /// <summary>
    /// Resolves connection-string placeholders for every profile. The raw template stays on
    /// ConnectionString; the resolved value goes to ResolvedConnectionString, which is
    /// [JsonIgnore] and therefore can never be written back to disk.
    /// </summary>
    private static void ResolveSecrets(
        List<SyncProfile> profiles,
        SyncServiceConfig baseConfig,
        string basePath,
        SerilogLogger logger)
    {
        var resolver = SecretResolver.Create(baseConfig.SecretsFile, basePath, logger);
        var profilesWithPlaceholders = 0;

        foreach (var profile in profiles)
        {
            var hadPlaceholder = false;

            foreach (var (connection, role) in new[]
                     {
                         (profile.SourceConnection, "SourceConnection"),
                         (profile.TargetConnection, "TargetConnection")
                     })
            {
                if (connection == null)
                {
                    continue;
                }

                var template = connection.ConnectionString;
                hadPlaceholder |= template.Contains("${", StringComparison.Ordinal);

                connection.SetResolvedConnectionString(
                    resolver.Resolve(template, $"{profile.ProfileName}.{role}"));
            }

            if (hadPlaceholder)
            {
                profilesWithPlaceholders++;
            }
        }

        if (resolver.UnresolvedNames.Count > 0)
        {
            // Names only - never the values.
            var names = string.Join(", ", resolver.UnresolvedNames.OrderBy(n => n));
            logger.Fatal(
                "{Count} secret placeholder(s) could not be resolved: {Names}",
                resolver.UnresolvedNames.Count, names);

            // DELIBERATELY FATAL, and it takes the whole service down rather than just the
            // affected profile. ConfigurationValidator only LOGS errors - startup continues
            // (Program.cs) - so without this the service would stay "active" while connecting
            // with the literal text "${NAME}" as the password. That is precisely the shape of
            // the nine-day silent outage after the 2026-07-16 empdev rotation: profiles failing
            // nightly while `systemctl is-active` still reported healthy.
            //
            // Trade-off accepted: one bad placeholder stops the other profiles too. Chosen
            // because the deployed config is hand-copied (no deploy script), so a half-resolved
            // config is an operator error that must be fixed now, and because systemd's
            // StartLimitBurst then halts the unit visibly instead of letting it limp. AIM #1821.
            throw new InvalidOperationException(
                $"Unresolved secret placeholder(s): {names}. " +
                "Set them as environment variables or add them to the secrets file " +
                "(SyncService:SecretsFile). Refusing to start with unresolved credentials.");
        }

        logger.Information(
            "Secret resolution complete: {WithPlaceholders} of {Total} profile(s) use placeholders, " +
            "{Unresolved} unresolved",
            profilesWithPlaceholders, profiles.Count, resolver.UnresolvedNames.Count);
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

        var jsonFiles = Directory.GetFiles(directory, "*.json")
            .OrderBy(f => Path.GetFileName(f), StringComparer.OrdinalIgnoreCase)
            .ToArray();

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
