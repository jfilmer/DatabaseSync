namespace DatabaseSync.Models;

/// <summary>
/// Represents metadata about a database column
/// </summary>
public class ColumnInfo
{
    /// <summary>
    /// Column name as it appears in the database
    /// </summary>
    public string ColumnName { get; set; } = string.Empty;
    
    /// <summary>
    /// Native data type from the source database (e.g., "nvarchar", "int")
    /// </summary>
    public string DataType { get; set; } = string.Empty;
    
    /// <summary>
    /// Mapped data type for the target database
    /// </summary>
    public string MappedDataType { get; set; } = string.Empty;
    
    /// <summary>
    /// Whether the column allows NULL values
    /// </summary>
    public bool IsNullable { get; set; }
    
    /// <summary>
    /// Maximum character length for string types (-1 for MAX)
    /// </summary>
    public int? MaxLength { get; set; }
    
    /// <summary>
    /// Numeric precision for decimal types
    /// </summary>
    public int? Precision { get; set; }
    
    /// <summary>
    /// Numeric scale for decimal types
    /// </summary>
    public int? Scale { get; set; }
    
    /// <summary>
    /// Whether this column is part of the primary key
    /// </summary>
    public bool IsPrimaryKey { get; set; }
    
    /// <summary>
    /// Whether this column is an identity/auto-increment column
    /// </summary>
    public bool IsIdentity { get; set; }
    
    /// <summary>
    /// Position of the column in the table (1-based)
    /// </summary>
    public int OrdinalPosition { get; set; }
    
    public override string ToString()
    {
        var pk = IsPrimaryKey ? " [PK]" : "";
        var identity = IsIdentity ? " [IDENTITY]" : "";
        return $"{ColumnName} ({DataType} -> {MappedDataType}){pk}{identity}";
    }
}
