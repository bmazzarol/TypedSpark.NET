using System.Globalization;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

public sealed class ShortColumn : TypedNumericColumn<ShortColumn, ShortType, short>
{
    private ShortColumn(Column column)
        : base(new ShortType(), column) { }

    public ShortColumn()
        : this(Col(string.Empty)) { }

    protected internal override object? CoerceToNative() =>
        short.TryParse(Column.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var b)
            ? b
            : null;

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static ShortColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static ShortColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator ShortColumn(short lit) => New(Lit((int)lit));

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a long.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    public static implicit operator StringColumn(ShortColumn column) => column.CastToString();

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public ShortColumn IsIn(short first, params short[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
