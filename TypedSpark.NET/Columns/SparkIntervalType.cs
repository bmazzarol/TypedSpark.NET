namespace TypedSpark.NET.Columns;

/// <summary>
/// Supported Spark interval types, see https://spark.apache.org/docs/latest/sql-ref-datatypes.html
/// </summary>
public enum SparkIntervalType
{
    /// <summary>
    /// YEAR type
    /// </summary>
    Year,

    /// <summary>
    /// MONTH type
    /// </summary>
    Month,

    /// <summary>
    /// WEEKS type
    /// </summary>
    Weeks,

    /// <summary>
    /// DAY type
    /// </summary>
    Day,

    /// <summary>
    /// HOUR type
    /// </summary>
    Hour,

    /// <summary>
    /// MINUTE type
    /// </summary>
    Minute,

    /// <summary>
    /// SECOND type
    /// </summary>
    Second
}
