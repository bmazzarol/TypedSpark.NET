using System;
using System.Globalization;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Timestamp column
/// </summary>
public sealed class TimestampColumn : TypedTemporalColumn<TimestampColumn, TimestampType, Timestamp>
{
    private TimestampColumn(Column column)
        : base(new TimestampType(), column) { }

    public TimestampColumn()
        : this(Col(string.Empty)) { }

    protected internal override object? CoerceToNative() =>
        DateTime.TryParse(
            Column.ToString(),
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal,
            out var b
        )
            ? b
            : null;

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static TimestampColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static TimestampColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator TimestampColumn(Timestamp lit) => New(Lit(lit));

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator TimestampColumn(DateTime lit) => new Timestamp(lit);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator TimestampColumn(DateTimeOffset lit) => lit.UtcDateTime;

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a timestamp.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(TimestampColumn lhs, TimestampColumn rhs) =>
        new() { Column = lhs.Column - rhs.Column };

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(TimestampColumn lhs, Timestamp rhs) =>
        lhs - (TimestampColumn)rhs;

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(Timestamp lhs, TimestampColumn rhs) =>
        (TimestampColumn)lhs - rhs;

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(TimestampColumn lhs, DateTime rhs) =>
        lhs - new Timestamp(rhs);

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(DateTime lhs, TimestampColumn rhs) =>
        new Timestamp(lhs) - rhs;

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(TimestampColumn lhs, DateTimeOffset rhs) =>
        lhs - rhs.UtcDateTime;

    /// <summary>
    /// Subtract 2 timestamps, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(DateTimeOffset lhs, TimestampColumn rhs) =>
        lhs.UtcDateTime - rhs;
}
