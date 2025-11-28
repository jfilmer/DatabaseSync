using DatabaseSync.Enums;
using DatabaseSync.Models;
using NpgsqlTypes;

namespace DatabaseSync.Services;

/// <summary>
/// Maps data types between SQL Server and PostgreSQL
/// </summary>
public class TypeMapper
{
    private static readonly Dictionary<string, string> SqlServerToPostgreSql =
        new(StringComparer.OrdinalIgnoreCase)
    {
        // Exact numeric
        { "bigint", "bigint" },
        { "int", "integer" },
        { "smallint", "smallint" },
        { "tinyint", "smallint" },
        { "bit", "boolean" },
        { "decimal", "numeric" },
        { "numeric", "numeric" },
        { "money", "numeric(19,4)" },
        { "smallmoney", "numeric(10,4)" },

        // Approximate numeric
        { "float", "double precision" },
        { "real", "real" },

        // Date/time
        { "date", "date" },
        { "time", "time" },
        { "datetime", "timestamp" },
        { "datetime2", "timestamp" },
        { "smalldatetime", "timestamp" },
        { "datetimeoffset", "timestamptz" },

        // Strings
        { "char", "char" },
        { "varchar", "varchar" },
        { "text", "text" },
        { "nchar", "char" },
        { "nvarchar", "varchar" },
        { "ntext", "text" },

        // Binary
        { "binary", "bytea" },
        { "varbinary", "bytea" },
        { "image", "bytea" },

        // Other
        { "uniqueidentifier", "uuid" },
        { "xml", "xml" },
        { "json", "jsonb" },
        { "hierarchyid", "text" },
        { "geography", "text" },
        { "geometry", "text" },
        { "sql_variant", "text" },
        { "timestamp", "bytea" },
        { "rowversion", "bytea" },
    };

    private static readonly Dictionary<string, string> PostgreSqlToSqlServer =
        new(StringComparer.OrdinalIgnoreCase)
    {
        // Exact numeric
        { "bigint", "bigint" },
        { "integer", "int" },
        { "int", "int" },
        { "smallint", "smallint" },
        { "boolean", "bit" },
        { "bool", "bit" },
        { "numeric", "decimal" },
        { "decimal", "decimal" },

        // Approximate numeric
        { "double precision", "float" },
        { "float8", "float" },
        { "real", "real" },
        { "float4", "real" },

        // Serial types (map to int with identity)
        { "serial", "int" },
        { "bigserial", "bigint" },
        { "smallserial", "smallint" },

        // Date/time
        { "date", "date" },
        { "time", "time" },
        { "time without time zone", "time" },
        { "time with time zone", "time" },
        { "timestamp", "datetime2" },
        { "timestamp without time zone", "datetime2" },
        { "timestamp with time zone", "datetimeoffset" },
        { "timestamptz", "datetimeoffset" },
        { "interval", "varchar(100)" },

        // Strings
        { "char", "char" },
        { "character", "char" },
        { "varchar", "varchar" },
        { "character varying", "varchar" },
        { "text", "nvarchar(MAX)" },
        { "citext", "nvarchar(MAX)" },

        // Binary
        { "bytea", "varbinary(MAX)" },

        // Other
        { "uuid", "uniqueidentifier" },
        { "xml", "xml" },
        { "json", "nvarchar(MAX)" },
        { "jsonb", "nvarchar(MAX)" },
        { "inet", "varchar(50)" },
        { "cidr", "varchar(50)" },
        { "macaddr", "varchar(20)" },
        { "money", "money" },
        { "bit", "binary" },
        { "bit varying", "varbinary" },
        { "point", "varchar(100)" },
        { "line", "varchar(100)" },
        { "lseg", "varchar(100)" },
        { "box", "varchar(100)" },
        { "path", "varchar(MAX)" },
        { "polygon", "varchar(MAX)" },
        { "circle", "varchar(100)" },
        { "tsvector", "nvarchar(MAX)" },
        { "tsquery", "nvarchar(MAX)" },
    };

    /// <summary>
    /// Map a SQL Server column type to PostgreSQL
    /// </summary>
    public string MapType(ColumnInfo column)
    {
        return MapSqlServerToPostgreSql(column);
    }

    /// <summary>
    /// Map column type based on source and target database types
    /// </summary>
    public string MapType(ColumnInfo column, DatabaseType sourceType, DatabaseType targetType)
    {
        // Same database type - no mapping needed, return original type with size info
        if (sourceType == targetType)
        {
            return GetOriginalTypeDef(column);
        }

        return (sourceType, targetType) switch
        {
            (DatabaseType.SqlServer, DatabaseType.PostgreSql) => MapSqlServerToPostgreSql(column),
            (DatabaseType.PostgreSql, DatabaseType.SqlServer) => MapPostgreSqlToSqlServer(column),
            _ => throw new NotSupportedException($"Type mapping from {sourceType} to {targetType} is not supported")
        };
    }

    private string MapSqlServerToPostgreSql(ColumnInfo column)
    {
        var sourceType = column.DataType.ToLower().Trim();

        if (sourceType.Contains('('))
        {
            sourceType = sourceType.Substring(0, sourceType.IndexOf('('));
        }

        if (!SqlServerToPostgreSql.TryGetValue(sourceType, out var targetType))
        {
            throw new NotSupportedException(
                $"No type mapping for SQL Server type: {column.DataType}. Column: {column.ColumnName}");
        }

        return sourceType switch
        {
            "varchar" or "nvarchar" when column.MaxLength == -1 => "text",
            "varchar" or "nvarchar" when column.MaxLength > 0 => $"varchar({column.MaxLength})",
            "char" or "nchar" when column.MaxLength > 0 => $"char({column.MaxLength})",
            "decimal" or "numeric" when column.Precision > 0 =>
                $"numeric({column.Precision},{column.Scale ?? 0})",
            _ => targetType
        };
    }

    private string MapPostgreSqlToSqlServer(ColumnInfo column)
    {
        var sourceType = column.DataType.ToLower().Trim();

        if (sourceType.Contains('('))
        {
            sourceType = sourceType.Substring(0, sourceType.IndexOf('('));
        }

        if (!PostgreSqlToSqlServer.TryGetValue(sourceType, out var targetType))
        {
            throw new NotSupportedException(
                $"No type mapping for PostgreSQL type: {column.DataType}. Column: {column.ColumnName}");
        }

        return sourceType switch
        {
            "varchar" or "character varying" when column.MaxLength > 0 => $"varchar({column.MaxLength})",
            "varchar" or "character varying" => "varchar(MAX)",
            "char" or "character" when column.MaxLength > 0 => $"char({column.MaxLength})",
            "numeric" or "decimal" when column.Precision > 0 =>
                $"decimal({column.Precision},{column.Scale ?? 0})",
            _ => targetType
        };
    }

    private string GetOriginalTypeDef(ColumnInfo column)
    {
        var dataType = column.DataType.ToLower().Trim();

        return dataType switch
        {
            "varchar" or "nvarchar" or "character varying" when column.MaxLength == -1 => $"{dataType}(MAX)",
            "varchar" or "nvarchar" or "character varying" when column.MaxLength > 0 => $"{dataType}({column.MaxLength})",
            "char" or "nchar" or "character" when column.MaxLength > 0 => $"{dataType}({column.MaxLength})",
            "decimal" or "numeric" when column.Precision > 0 => $"{dataType}({column.Precision},{column.Scale ?? 0})",
            "varbinary" when column.MaxLength == -1 => "varbinary(MAX)",
            "varbinary" when column.MaxLength > 0 => $"varbinary({column.MaxLength})",
            _ => dataType
        };
    }

    /// <summary>
    /// Convert a value from SQL Server to PostgreSQL compatible type
    /// </summary>
    public object? ConvertValue(object? value, string sourceType, string targetType)
    {
        if (value == null || value == DBNull.Value)
            return DBNull.Value;

        var sourceTypeLower = sourceType.ToLower();
        
        if (sourceTypeLower.Contains('('))
            sourceTypeLower = sourceTypeLower.Substring(0, sourceTypeLower.IndexOf('('));

        try
        {
            return sourceTypeLower switch
            {
                "uniqueidentifier" => value switch
                {
                    Guid g => g,
                    string s => Guid.Parse(s),
                    byte[] b => new Guid(b),
                    _ => Guid.Parse(value.ToString()!)
                },
                
                "bit" => Convert.ToBoolean(value),
                "tinyint" => Convert.ToInt16(value),
                "money" or "smallmoney" => Convert.ToDecimal(value),
                "float" => Convert.ToDouble(value),
                "real" => Convert.ToSingle(value),
                
                "datetime" or "datetime2" or "smalldatetime" => value switch
                {
                    DateTime dt => DateTime.SpecifyKind(dt, DateTimeKind.Unspecified),
                    _ => value
                },
                
                "datetimeoffset" => value switch
                {
                    DateTimeOffset dto => dto,
                    DateTime dt => new DateTimeOffset(dt),
                    string s => DateTimeOffset.Parse(s),
                    _ => value
                },
                
                "varbinary" or "binary" or "image" or "timestamp" or "rowversion" => 
                    value as byte[] ?? value,
                
                "xml" => value.ToString(),
                "hierarchyid" or "geography" or "geometry" => value.ToString(),
                
                _ => value
            };
        }
        catch (Exception ex)
        {
            throw new InvalidCastException(
                $"Failed to convert from {sourceType} to {targetType}: {ex.Message}. " +
                $"Value type: {value.GetType().Name}",
                ex);
        }
    }

    /// <summary>
    /// Get the NpgsqlDbType for a PostgreSQL type string
    /// </summary>
    public NpgsqlDbType GetNpgsqlDbType(string postgresType)
    {
        var baseType = postgresType.ToLower();
        if (baseType.Contains('('))
            baseType = baseType.Substring(0, baseType.IndexOf('('));

        return baseType switch
        {
            "bigint" => NpgsqlDbType.Bigint,
            "integer" or "int" => NpgsqlDbType.Integer,
            "smallint" => NpgsqlDbType.Smallint,
            "boolean" or "bool" => NpgsqlDbType.Boolean,
            "numeric" or "decimal" => NpgsqlDbType.Numeric,
            "double precision" or "float8" => NpgsqlDbType.Double,
            "real" or "float4" => NpgsqlDbType.Real,
            "date" => NpgsqlDbType.Date,
            "time" => NpgsqlDbType.Time,
            "timestamp" => NpgsqlDbType.Timestamp,
            "timestamptz" => NpgsqlDbType.TimestampTz,
            "char" => NpgsqlDbType.Char,
            "varchar" => NpgsqlDbType.Varchar,
            "text" => NpgsqlDbType.Text,
            "bytea" => NpgsqlDbType.Bytea,
            "uuid" => NpgsqlDbType.Uuid,
            "xml" => NpgsqlDbType.Xml,
            "json" => NpgsqlDbType.Json,
            "jsonb" => NpgsqlDbType.Jsonb,
            _ => NpgsqlDbType.Unknown
        };
    }
}
