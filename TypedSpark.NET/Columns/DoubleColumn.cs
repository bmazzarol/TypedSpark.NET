using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Extensions;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public sealed class DoubleColumn : TypedNumericColumn<DoubleColumn, DoubleType, double>
{
    private DoubleColumn(Column column) : base(new DoubleType(), column) { }

    public DoubleColumn() : this(Col(string.Empty)) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static DoubleColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static DoubleColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator DoubleColumn(double lit) => New(Lit(lit));

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a double.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    public static implicit operator StringColumn(DoubleColumn column) => column.CastToString();

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public DoubleColumn IsIn(double first, params double[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
