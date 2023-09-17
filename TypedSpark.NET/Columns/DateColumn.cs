using System;
using System.Globalization;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Date column
/// </summary>
public sealed class DateColumn
    : TypedTemporalColumn<DateColumn, DateType, Date>,
        TypedNumericOrDateTimeColumn
{
    private DateColumn(Column column)
        : base(new DateType(), column) { }

    /// <summary>
    /// Constructs an empty column
    /// </summary>
    public DateColumn()
        : this(F.Col(string.Empty)) { }

    /// <inheritdoc />
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
    public static DateColumn New(string name, Column? column = default) =>
        new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static DateColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator DateColumn(Date lit) => New(F.Lit(lit));

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator DateColumn(DateTime lit) => new Date(lit);

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a date.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    /// <summary>
    /// Subtract 2 dates, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(DateColumn lhs, DateColumn rhs) =>
        IntervalColumn.New(lhs.Column - rhs.Column);

    /// <summary>
    /// Subtract 2 dates, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(DateColumn lhs, Date rhs) => lhs - (DateColumn)rhs;

    /// <summary>
    /// Subtract 2 dates, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(Date lhs, DateColumn rhs) => (DateColumn)lhs - rhs;

    /// <summary>
    /// Subtract 2 dates, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(DateColumn lhs, DateTime rhs) => lhs - new Date(rhs);

    /// <summary>
    /// Subtract 2 dates, returns an interval
    /// </summary>
    /// <param name="lhs">left hand side</param>
    /// <param name="rhs">right hand side</param>
    /// <returns>interval column</returns>
    public static IntervalColumn operator -(DateTime lhs, DateColumn rhs) => new Date(lhs) - rhs;
}
