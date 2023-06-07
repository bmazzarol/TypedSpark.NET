using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Day time interval type
/// </summary>
[SuppressMessage("Design", "MA0048:File name must match type name")]
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
public class IntervalColumn : TypedTemporalColumn<IntervalColumn, DayTimeIntervalType, TimeSpan>
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
                $"make_interval(0,0,0,{lit.Days},{lit.Hours},{lit.Minutes},{lit.Seconds})"
            )
        };

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator IntervalColumn(TimeSpan lit) => New(lit);
}
