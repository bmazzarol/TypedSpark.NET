using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Numeric operators
/// </summary>
public abstract class TypedNumericColumn<TThis, TSparkType, TNativeType>
    : TypedOrdColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    protected TypedNumericColumn(TSparkType columnType, Column column) : base(columnType, column)
    { }

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

    public TThis Plus(TNativeType rhs) => new() { Column = Column.Plus(Lit(rhs)) };

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

    public TThis Minus(TNativeType rhs) => new() { Column = Column.Minus(Lit(rhs)) };

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

    public TThis Multiply(TNativeType rhs) => new() { Column = Column.Multiply(Lit(rhs)) };

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
    ) => new() { Column = Lit(lhs).Divide((Column)rhs) };

    public static TThis operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Divide(rhs);

    public TThis Divide(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Divide((Column)rhs) };

    public TThis Divide(TNativeType rhs) => new() { Column = Column.Divide(Lit(rhs)) };

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
    ) => new() { Column = Lit(lhs).Mod((Column)rhs) };

    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Mod(rhs);

    public TThis Mod(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        new() { Column = Column.Mod((Column)rhs) };

    public TThis Mod(TNativeType rhs) => new() { Column = Column.Mod(Lit(rhs)) };
}
