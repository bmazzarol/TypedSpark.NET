using System.Globalization;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Decimal column
/// </summary>
public sealed class DecimalColumn : TypedNumericColumn<DecimalColumn, DecimalType, decimal>
{
    private DecimalColumn(Column column)
        : base(new DecimalType(), column) { }

    /// <summary>
    /// Constructs an empty column
    /// </summary>
    public DecimalColumn()
        : this(Col(string.Empty)) { }

    /// <inheritdoc />
    protected internal override object? CoerceToNative() =>
        decimal.TryParse(
            Column.ToString(),
            NumberStyles.Any,
            CultureInfo.InvariantCulture,
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
    public static DecimalColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static DecimalColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator DecimalColumn(decimal lit) => New(Lit((double)lit));

    /// <summary>
    /// Cast to string
    /// </summary>
    public static implicit operator StringColumn(DecimalColumn column) => column.CastToString();

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public DecimalColumn IsIn(decimal first, params decimal[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
