using System.Text.RegularExpressions;
using SerilogLogger = Serilog.ILogger;

namespace DatabaseSync.Services;

/// <summary>
/// Resolves ${PLACEHOLDER} tokens in profile connection strings from a protected source,
/// so that profile JSON files never need to contain a plaintext password.
///
/// Resolution order for ${NAME} (first hit wins):
///   1. Process environment variable NAME   (systemd EnvironmentFile= on ubu2, service env on win2)
///   2. Key NAME in the secrets file        (default "secrets.env" beside the executable)
///
/// Unresolved placeholders are left in place and reported. ConfigurationValidator turns them into
/// a validation error, so the service fails loudly at startup instead of attempting a connection
/// with a literal "${NAME}" as the password.
///
/// Secret VALUES are never logged. Only placeholder names and counts are.
/// </summary>
public sealed class SecretResolver
{
    // ${NAME} substitutes; $${NAME} is an escape that emits a literal ${NAME}.
    // A password that genuinely contains the two characters "${" must be written with the
    // escape form. Everything else round-trips untouched.
    private static readonly Regex PlaceholderPattern =
        new(@"\$(\$)?\{([A-Za-z_][A-Za-z0-9_]*)\}", RegexOptions.Compiled);

    private readonly Dictionary<string, string> _fileSecrets;
    private readonly SerilogLogger _logger;

    private SecretResolver(Dictionary<string, string> fileSecrets, SerilogLogger logger)
    {
        _fileSecrets = fileSecrets;
        _logger = logger;
    }

    /// <summary>
    /// Names of placeholders that could not be resolved from any source, across every call
    /// to <see cref="Resolve"/> made on this instance.
    /// </summary>
    public HashSet<string> UnresolvedNames { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Builds a resolver, loading the secrets file if one is configured and present.
    /// </summary>
    /// <param name="secretsFilePath">
    /// Configured path. Relative paths resolve against <paramref name="basePath"/>.
    /// A missing file is not an error — environment variables alone are a valid configuration.
    /// </param>
    public static SecretResolver Create(string? secretsFilePath, string basePath, SerilogLogger logger)
    {
        var secrets = new Dictionary<string, string>(StringComparer.Ordinal);

        if (string.IsNullOrWhiteSpace(secretsFilePath))
        {
            return new SecretResolver(secrets, logger);
        }

        var fullPath = Path.IsPathRooted(secretsFilePath)
            ? secretsFilePath
            : Path.Combine(basePath, secretsFilePath);

        if (!File.Exists(fullPath))
        {
            logger.Information(
                "No secrets file at {Path} - placeholders will resolve from environment variables only",
                fullPath);
            return new SecretResolver(secrets, logger);
        }

        if (!HasSafePermissions(fullPath, logger))
        {
            // Deliberately fatal. A secrets file readable by other accounts is the exact defect
            // this whole mechanism exists to remove (AIM #1821), and the deployed path has no
            // deploy script, so a hand-copy at the default umask silently re-widens it. Failing
            // to start is the only signal that cannot be missed.
            throw new InvalidOperationException(
                $"Secrets file '{fullPath}' is accessible to group or other. " +
                "Fix with: chmod 600 (and chown to the service account), then restart.");
        }

        var lineNumber = 0;
        foreach (var rawLine in File.ReadAllLines(fullPath))
        {
            lineNumber++;
            var line = rawLine.Trim();

            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            // Tolerate "export NAME=value" so the same file can be sourced by a shell.
            if (line.StartsWith("export ", StringComparison.Ordinal))
            {
                line = line["export ".Length..].TrimStart();
            }

            var separator = line.IndexOf('=');
            if (separator <= 0)
            {
                logger.Warning("Ignoring malformed line {Line} in secrets file (expected NAME=value)", lineNumber);
                continue;
            }

            var key = line[..separator].Trim();
            var value = line[(separator + 1)..].Trim();

            // Strip one layer of matching quotes; a password may legitimately contain '#' or spaces,
            // so no comment-stripping is done on the value.
            if (value.Length >= 2 &&
                ((value[0] == '"' && value[^1] == '"') || (value[0] == '\'' && value[^1] == '\'')))
            {
                value = value[1..^1];
            }

            secrets[key] = value;
        }

        logger.Information("Loaded {Count} secret(s) from {Path}", secrets.Count, fullPath);
        return new SecretResolver(secrets, logger);
    }

    /// <summary>
    /// Substitutes every ${NAME} in <paramref name="template"/>. Unresolvable placeholders are
    /// left verbatim and recorded in <see cref="UnresolvedNames"/>.
    /// </summary>
    /// <param name="context">Human-readable location, used only for log messages.</param>
    public string Resolve(string template, string context)
    {
        if (string.IsNullOrEmpty(template) || !template.Contains("${", StringComparison.Ordinal))
        {
            // Fast path: a profile with no placeholders is returned byte-for-byte unchanged.
            // This is what makes the change a no-op for not-yet-migrated profiles.
            return template;
        }

        return PlaceholderPattern.Replace(template, match =>
        {
            var isEscaped = match.Groups[1].Success;
            var name = match.Groups[2].Value;

            if (isEscaped)
            {
                return "${" + name + "}";
            }

            var value = Environment.GetEnvironmentVariable(name);
            if (!string.IsNullOrEmpty(value))
            {
                return value;
            }

            if (_fileSecrets.TryGetValue(name, out var fileValue) && !string.IsNullOrEmpty(fileValue))
            {
                return fileValue;
            }

            UnresolvedNames.Add(name);
            _logger.Error(
                "Secret placeholder ${{{Name}}} referenced by {Context} could not be resolved " +
                "from the environment or the secrets file", name, context);

            return match.Value;
        });
    }

    /// <summary>
    /// True when the file carries no group or other permission bits. Always true on Windows,
    /// where the mode has no meaning and ACLs govern access instead.
    /// </summary>
    private static bool HasSafePermissions(string path, SerilogLogger logger)
    {
        if (OperatingSystem.IsWindows())
        {
            return true;
        }

        const UnixFileMode groupOrOther =
            UnixFileMode.GroupRead | UnixFileMode.GroupWrite | UnixFileMode.GroupExecute |
            UnixFileMode.OtherRead | UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;

        var mode = File.GetUnixFileMode(path);
        var offending = mode & groupOrOther;

        if (offending == 0)
        {
            return true;
        }

        logger.Error(
            "Secrets file {Path} has unsafe permissions {Mode} - group/other bits {Offending} must be cleared",
            path, mode, offending);

        return false;
    }
}
