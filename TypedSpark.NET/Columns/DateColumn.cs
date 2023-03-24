using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Date column
/// </summary>
public sealed class DateColumn : TypedOrdColumn<DateColumn, DateType, Date>
{
    private DateColumn(string columnName, Column column) : base(columnName, new DateType(), column)
    { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static DateColumn New(string name, Column? column = default) =>
        new(name, column ?? Col(name));

    protected override DateColumn New(Column column, string? name = default) =>
        New(name ?? ColumnName, column);

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a date.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(ColumnName, Column.Cast("string"));
}
