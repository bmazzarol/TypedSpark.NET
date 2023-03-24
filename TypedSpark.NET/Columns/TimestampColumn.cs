using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public class TimestampColumn : TypedOrdColumn<TimestampColumn, TimestampType, Timestamp>
{
    private TimestampColumn(string columnName, Column column)
        : base(columnName, new TimestampType(), column) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static TimestampColumn New(string name, Column? column = default) =>
        new(name, column ?? Col(name));

    protected override TimestampColumn New(Column column, string? name = default) =>
        New(name ?? ColumnName, column);

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a timestamp.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(ColumnName, Column.Cast("string"));
}
