using System.Globalization;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Float column
/// </summary>
public sealed class FloatColumn : TypedFloatingPointColumn<FloatColumn, FloatType, float>
{
    private FloatColumn(Column column)
        : base(new FloatType(), column) { }

    /// <summary>
    /// Constructs an empty column
    /// </summary>
    public FloatColumn()
        : this(Col(string.Empty)) { }

    /// <inheritdoc />
    protected internal override object? CoerceToNative() =>
        float.TryParse(Column.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var b)
            ? b
            : null;

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static FloatColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static FloatColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator FloatColumn(float lit) => New(Lit((double)lit));

    /// <summary>
    /// Cast to string
    /// </summary>
    public static implicit operator StringColumn(FloatColumn column) => column.CastToString();

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public FloatColumn IsIn(float first, params float[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
