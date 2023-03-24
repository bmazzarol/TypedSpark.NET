using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Extensions;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public class LongColumn : TypedNumericColumn<LongColumn, LongType, long>
{
    private LongColumn(string columnName, Column column) : base(columnName, new LongType(), column)
    { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static LongColumn New(string name, Column? column = default) =>
        new(name, column ?? Col(name));

    protected override LongColumn New(Column column, string? name = default) =>
        New(name ?? ColumnName, column);

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a long.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(ColumnName, Column.Cast("string"));

    public static implicit operator StringColumn(LongColumn column) => column.CastToString();

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public LongColumn IsIn(long first, params long[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
