using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Numeric operators
/// </summary>
public abstract class TypedNumericColumn<TThis, TSparkType, TNativeType>
    : TypedOrdColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    protected TypedNumericColumn(TSparkType columnType, Column column)
        : base(columnType, column) { }

    /// <summary>Negate the given column.</summary>
    /// <param name="self">Column to negate</param>
    /// <returns>New column after applying negation</returns>
    public static TThis operator -(TypedNumericColumn<TThis, TSparkType, TNativeType> self) =>
        self.Negate();

    /// <summary>Negate the column.</summary>
    /// <returns>New column after applying negation</returns>
    public TThis Negate() => new() { Column = -Column };

    //
    // Plus Operations
    //

    public static TThis operator +(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Plus(rhs);

    public static TThis operator +(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Plus(lhs);

    public static TThis operator +(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Plus(rhs);

    public TThis Plus(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Plus((Column)rhs) };

    public TThis Plus(TNativeType rhs) => new() { Column = Column.Plus(F.Lit(rhs)) };

    //
    // Minus Operations
    //

    public static TThis operator -(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Minus(rhs);

    public static TThis operator -(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Minus(lhs);

    public static TThis operator -(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Minus(rhs);

    public TThis Minus(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Minus((Column)rhs) };

    public TThis Minus(TNativeType rhs) => new() { Column = Column.Minus(F.Lit(rhs)) };

    //
    // Multiply Operations
    //

    public static TThis operator *(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Multiply(rhs);

    public static TThis operator *(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Multiply(lhs);

    public static TThis operator *(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Multiply(rhs);

    public TThis Multiply(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Multiply((Column)rhs) };

    public TThis Multiply(TNativeType rhs) => new() { Column = Column.Multiply(F.Lit(rhs)) };

    //
    // Divide Operations
    //

    public static TThis operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Divide(rhs);

    public static TThis operator /(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => new() { Column = F.Lit(lhs).Divide((Column)rhs) };

    public static TThis operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Divide(rhs);

    public TThis Divide(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Divide((Column)rhs) };

    public TThis Divide(TNativeType rhs) => new() { Column = Column.Divide(F.Lit(rhs)) };

    //
    // Mod Operations
    //

    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Mod(rhs);

    public static TThis operator %(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => new() { Column = F.Lit(lhs).Mod((Column)rhs) };

    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Mod(rhs);

    public TThis Mod(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Mod((Column)rhs) };

    public TThis Mod(TNativeType rhs) => new() { Column = Column.Mod(F.Lit(rhs)) };

    /// <summary>
    /// Casts the column to a string column, using the canonical string
    /// </summary>
    /// <returns>string column</returns>
    public StringColumn CastToString() => StringColumn.New(Column.Cast("string"));

    /// <summary>
    /// Casts the column to a byte column
    /// </summary>
    /// <returns>byte column</returns>
    public ByteColumn CastToByte() => ByteColumn.New(Column.Cast("byte"));

    /// <summary>
    /// Casts the column to a short column
    /// </summary>
    /// <returns>short column</returns>
    public ShortColumn CastToShort() => ShortColumn.New(Column.Cast("short"));

    /// <summary>
    /// Casts the column to a short column
    /// </summary>
    /// <returns>integer column</returns>
    public IntegerColumn CastToInteger() => IntegerColumn.New(Column.Cast("int"));

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
    /// Return true if the column is NaN
    /// </summary>
    /// <returns>boolean column</returns>
    public BooleanColumn IsNaN() => BooleanColumn.New(F.IsNaN(Column));
}
