using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Base type for all Date, Timestamp and Interval columns
/// </summary>
public class TypedTemporalColumn<TThis, TSparkType, TNativeType>
    : TypedOrdColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    /// <summary>
    /// Constructor that enforces that a typed column has a spark data type and underlying untyped column
    /// </summary>
    /// <param name="columnType">spark data type</param>
    /// <param name="column">untyped column</param>
    public TypedTemporalColumn(TSparkType columnType, Column column)
        : base(columnType, column) { }

    /// <summary>
    /// Extracts the year as an integer from a given date/timestamp/string.
    /// </summary>
    /// <returns>Column object</returns>
    [Since("3.0.0")]
    public IntegerColumn Year() => new() { Column = F.Year(Column) };
}
