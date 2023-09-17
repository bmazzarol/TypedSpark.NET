using System;
using System.Globalization;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Day time interval type
/// </summary>
public sealed class DayTimeIntervalType : AtomicType
{
    /// <inheritdoc />
    public override string SimpleString => nameof(DayTimeIntervalType);
}

/// <summary>
/// Interval column
/// </summary>
/// <remarks>
/// The support for interval columns in dotnet spark is poor, these types cannot be used in
/// other complex types or printed in a schema from the dotnet side.
/// </remarks>
public class IntervalColumn
    : TypedTemporalColumn<IntervalColumn, DayTimeIntervalType, TimeSpan>,
        TypedNumericOrIntervalColumn
{
    private IntervalColumn(Column column)
        : base(new DayTimeIntervalType(), column) { }

    /// <summary>
    /// Constructs an empty column
    /// </summary>
    public IntervalColumn()
        : this(F.Col(string.Empty)) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static IntervalColumn New(string name, Column? column = default) =>
        new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>column</returns>
    public static IntervalColumn New(Column column) => new(column);

    /// <summary>
    /// Creates a new interval
    /// </summary>
    /// <param name="lit">timespan</param>
    /// <returns>column</returns>
    [Since("3.0.0")]
    public static IntervalColumn New(TimeSpan lit) =>
        new()
        {
            Column = F.Expr(
                $"make_interval(0,0,0,{lit.Days.ToString(CultureInfo.InvariantCulture)},{lit.Hours.ToString(CultureInfo.InvariantCulture)},{lit.Minutes.ToString(CultureInfo.InvariantCulture)},{lit.Seconds.ToString(CultureInfo.InvariantCulture)})"
            )
        };

    /// <summary>
    /// Creates a new interval
    /// </summary>
    /// <param name="value">interval value</param>
    /// <param name="type">interval type</param>
    /// <returns>column</returns>
    [Since("3.0.0")]
    public static IntervalColumn New(IntegerColumn value, SparkIntervalType type) =>
        new()
        {
            Column = F.Expr(
                type switch
                {
                    SparkIntervalType.Year => $"make_interval({value},0,0,0,0,0,0)",
                    SparkIntervalType.Month => $"make_interval(0,{value},0,0,0,0,0)",
                    SparkIntervalType.Weeks => $"make_interval(0,0,{value},0,0,0,0)",
                    SparkIntervalType.Day => $"make_interval(0,0,0,{value},0,0,0)",
                    SparkIntervalType.Hour => $"make_interval(0,0,0,0,{value},0,0)",
                    SparkIntervalType.Minute => $"make_interval(0,0,0,0,0,{value},0)",
                    SparkIntervalType.Second => $"make_interval(0,0,0,0,0,0,{value})",
                    _ => throw new ArgumentOutOfRangeException(nameof(type), type, message: null)
                }
            )
        };

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator IntervalColumn(TimeSpan lit) => New(lit);
}
