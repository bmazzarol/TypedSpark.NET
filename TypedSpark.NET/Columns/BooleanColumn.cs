using System.Linq;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Boolean column
/// </summary>
public sealed class BooleanColumn : TypedOrdColumn<BooleanColumn, BooleanType, bool>
{
    private BooleanColumn(Column column)
        : base(new BooleanType(), column) { }

    /// <summary>
    /// Constructs an empty array column
    /// </summary>
    public BooleanColumn()
        : this(F.Col(string.Empty)) { }

    /// <inheritdoc />
    protected internal override object? CoerceToNative() =>
        bool.TryParse(Column.ToString(), out var b) ? b : null;

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns>boolean column</returns>
    public static BooleanColumn New(string name, Column? column = default) =>
        new(column ?? F.Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>boolean column</returns>
    public static BooleanColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>boolean column</returns>
    public static implicit operator BooleanColumn(bool lit) => New(F.Lit(lit));

    /// <summary>
    /// Apply inversion of boolean expression, i.e. NOT
    /// </summary>
    /// <param name="self">Column to apply inversion</param>
    /// <returns>boolean column</returns>
    public static BooleanColumn operator !(BooleanColumn self) => self.Not();

    /// <summary>
    /// Apply inversion of boolean expression, i.e. NOT
    /// </summary>
    /// <returns>boolean column</returns>
    public BooleanColumn Not() => New(!Column);

    /// <summary>
    /// Apply boolean OR operator for the given two columns
    /// </summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>boolean column</returns>
    public static BooleanColumn operator |(BooleanColumn lhs, BooleanColumn rhs) => lhs.Or(rhs);

    /// <summary>
    /// Apply boolean OR operator with the given column
    /// </summary>
    /// <param name="other">Column to apply OR operator</param>
    /// <returns>boolean column</returns>
    public BooleanColumn Or(BooleanColumn other) => New(Column.Or((Column)other));

    /// <summary>
    /// Apply boolean AND operator for the given two columns
    /// </summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>boolean column</returns>
    public static BooleanColumn operator &(BooleanColumn lhs, BooleanColumn rhs) => lhs.And(rhs);

    /// <summary>
    /// Apply boolean AND operator with the given column
    /// </summary>
    /// <param name="other">Column to apply AND operator</param>
    /// <returns>boolean column</returns>
    public BooleanColumn And(BooleanColumn other) => New(Column.And((Column)other));

    /// <summary>
    /// Returns true if at least one value of expr is true
    /// </summary>
    /// <returns>boolean column</returns>
    [Since("3.0.0")]
    public BooleanColumn Any() => New(F.Expr($"any({Column})"));

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a boolean
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    /// <summary>
    /// Cast to string
    /// </summary>
    public static implicit operator StringColumn(BooleanColumn column) => column.CastToString();

    /// <summary>
    /// Casts the column to a byte column
    /// </summary>
    /// <returns>byte column</returns>
    public ByteColumn CastToByte() => ByteColumn.New(Column.Cast("byte"));

    /// <summary>
    /// Casts the column to a integer column
    /// </summary>
    /// <returns>integer column</returns>
    public IntegerColumn CastToInteger() => IntegerColumn.New(Column.Cast("int"));

    /// <summary>
    /// Casts the column to a short column
    /// </summary>
    /// <returns>short column</returns>
    public ShortColumn CastToShort() => ShortColumn.New(Column.Cast("short"));

    /// <summary>
    /// Casts the column to a long column
    /// </summary>
    /// <returns>long column</returns>
    public LongColumn CastToLong() => LongColumn.New(Column.Cast("long"));

    /// <summary>
    /// Casts the column to a float column
    /// </summary>
    /// <returns>float column</returns>
    public FloatColumn CastToFloat() => FloatColumn.New(Column.Cast("float"));

    /// <summary>
    /// Casts the column to a double column
    /// </summary>
    /// <returns>double column</returns>
    public DoubleColumn CastToDouble() => DoubleColumn.New(Column.Cast("double"));

    /// <summary>
    /// Casts the column to a decimal column
    /// </summary>
    /// <returns>decimal column</returns>
    public DecimalColumn CastToDecimal() => DecimalColumn.New(Column.Cast("decimal"));

    /// <summary>
    /// A boolean expression that is evaluated to true if the value of this expression
    /// is contained by the evaluated values of the arguments
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>boolean column</returns>
    public BooleanColumn IsIn(bool first, params bool[] rest) =>
        New(F.Expr($"{Column} IN ({string.Join(",", first.CombinedWith(rest).Distinct())})"));
}
