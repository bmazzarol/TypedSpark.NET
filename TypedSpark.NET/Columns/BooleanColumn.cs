using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Boolean column
/// </summary>
public sealed class BooleanColumn : TypedOrdColumn<BooleanColumn, BooleanType, bool>
{
    private BooleanColumn(Column column)
        : base(new BooleanType(), column) { }

    public BooleanColumn()
        : this(Col(string.Empty)) { }

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="name">name</param>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static BooleanColumn New(string name, Column? column = default) =>
        new(column ?? Col(name));

    /// <summary>
    /// Creates a new column
    /// </summary>
    /// <param name="column">column</param>
    /// <returns></returns>
    public static BooleanColumn New(Column column) => new(column);

    /// <summary>
    /// Convert the dotnet literal value to a column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>typed column</returns>
    public static implicit operator BooleanColumn(bool lit) => New(Lit(lit));

    /// <summary>Apply inversion of boolean expression, i.e. NOT.</summary>
    /// <param name="self">Column to apply inversion</param>
    /// <returns>New column after applying inversion</returns>
    public static BooleanColumn operator !(BooleanColumn self) => self.Not();

    /// <summary>Apply inversion of boolean expression, i.e. NOT.</summary>
    /// <returns>New column after applying inversion</returns>
    public BooleanColumn Not() => New(!Column);

    /// <summary>Apply boolean OR operator for the given two columns.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>New column after applying the operator</returns>
    public static BooleanColumn operator |(BooleanColumn lhs, BooleanColumn rhs) => lhs.Or(rhs);

    /// <summary>Apply boolean OR operator with the given column.</summary>
    /// <param name="other">Column to apply OR operator</param>
    /// <returns>New column after applying the operator</returns>
    public BooleanColumn Or(BooleanColumn other) => New(Column.Or((Column)other));

    /// <summary>Apply boolean AND operator for the given two columns.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>New column after applying the operator</returns>
    public static BooleanColumn operator &(BooleanColumn lhs, BooleanColumn rhs) => lhs.And(rhs);

    /// <summary>Apply boolean AND operator with the given column.</summary>
    /// <param name="other">Column to apply AND operator</param>
    /// <returns>New column after applying the operator</returns>
    public BooleanColumn And(BooleanColumn other) => New(Column.And((Column)other));

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// representation of a boolean.
    /// </summary>
    /// <returns>Column object</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    public static implicit operator StringColumn(BooleanColumn column) => column.CastToString();

    /// <summary>
    /// Casts the column to a byte column
    /// </summary>
    /// <returns>Column object</returns>
    public ByteColumn CastToByte() => ByteColumn.New(Column.Cast("byte"));

    /// <summary>
    /// Casts the column to a integer column
    /// </summary>
    /// <returns>Column object</returns>
    public IntegerColumn CastToInteger() => IntegerColumn.New(Column.Cast("int"));

    /// <summary>
    /// Casts the column to a short column
    /// </summary>
    /// <returns>Column object</returns>
    public ShortColumn CastToShort() => ShortColumn.New(Column.Cast("short"));

    /// <summary>
    /// Casts the column to a long column
    /// </summary>
    /// <returns>Column object</returns>
    public LongColumn CastToLong() => LongColumn.New(Column.Cast("long"));

    /// <summary>
    /// Casts the column to a float column
    /// </summary>
    /// <returns>Column object</returns>
    public FloatColumn CastToFloat() => FloatColumn.New(Column.Cast("float"));

    /// <summary>
    /// Casts the column to a double column
    /// </summary>
    /// <returns>Column object</returns>
    public DoubleColumn CastToDouble() => DoubleColumn.New(Column.Cast("double"));

    /// <summary>
    /// Casts the column to a decimal column
    /// </summary>
    /// <returns>Column object</returns>
    public DecimalColumn CastToDecimal() => DecimalColumn.New(Column.Cast("decimal"));

    /// <summary>
    ///  A boolean expression that is evaluated to true if the value of this expression
    ///  is contained by the evaluated values of the arguments.
    /// </summary>
    /// <param name="first">first value</param>
    /// <param name="rest">rest of the values</param>
    /// <returns>Column object</returns>
    public BooleanColumn IsIn(bool first, params bool[] rest) =>
        New(Column.IsIn(first.CombinedWith(rest)));
}
