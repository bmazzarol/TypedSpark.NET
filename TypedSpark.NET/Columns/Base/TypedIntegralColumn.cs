using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Integral operations
/// </summary>
public abstract class TypedIntegralColumn<TThis, TSparkType, TNativeType>
    : TypedNumericColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    /// <summary>
    /// Constructor that enforces that a typed column has a spark data type and underlying untyped column
    /// </summary>
    /// <param name="columnType">spark data type</param>
    /// <param name="column">untyped column</param>
    protected TypedIntegralColumn(TSparkType columnType, Column column)
        : base(columnType, column) { }

    private static TThis New(Column column) => new() { Column = column };

    /// <summary>
    /// bitwise OR
    /// </summary>
    public static TThis operator |(
        TypedIntegralColumn<TThis, TSparkType, TNativeType> lhs,
        TypedIntegralColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.BitwiseOR(rhs);

    /// <summary>
    /// bitwise OR
    /// </summary>
    public static TThis operator |(
        TypedIntegralColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.BitwiseOR(rhs);

    /// <summary>
    /// bitwise OR
    /// </summary>
    public static TThis operator |(
        TNativeType lhs,
        TypedIntegralColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).BitwiseOR(rhs.Column));

    /// <summary>
    /// Compute bitwise OR of this expression with another expression
    /// </summary>
    /// <param name="other">column</param>
    /// <returns>column</returns>
    public TThis BitwiseOR(TypedIntegralColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.BitwiseOR(other.Column));

    /// <summary>
    /// Compute bitwise OR of this expression with another expression
    /// </summary>
    /// <param name="other">literal</param>
    /// <returns>column</returns>
    public TThis BitwiseOR(TNativeType other) => New(Column.BitwiseOR(other));

    /// <summary>
    /// bitwise AND
    /// </summary>
    public static TThis operator &(
        TypedIntegralColumn<TThis, TSparkType, TNativeType> lhs,
        TypedIntegralColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.BitwiseAND(rhs);

    /// <summary>
    /// bitwise AND
    /// </summary>
    public static TThis operator &(
        TypedIntegralColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.BitwiseAND(rhs);

    /// <summary>
    /// bitwise AND
    /// </summary>
    public static TThis operator &(
        TNativeType lhs,
        TypedIntegralColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).BitwiseAND(rhs.Column));

    /// <summary>
    /// Compute bitwise AND of this expression with another expression
    /// </summary>
    /// <param name="other">column</param>
    /// <returns>column</returns>
    public TThis BitwiseAND(TypedIntegralColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.BitwiseAND(other.Column));

    /// <summary>
    /// Compute bitwise AND of this expression with another expression
    /// </summary>
    /// <param name="other">literal</param>
    /// <returns>column</returns>
    public TThis BitwiseAND(TNativeType other) => New(Column.BitwiseAND(F.Lit(other)));

    /// <summary>
    /// bitwise exclusive OR
    /// </summary>
    public static TThis operator ^(
        TypedIntegralColumn<TThis, TSparkType, TNativeType> lhs,
        TypedIntegralColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.BitwiseXOR(rhs);

    /// <summary>
    /// bitwise exclusive OR
    /// </summary>
    public static TThis operator ^(
        TypedIntegralColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.BitwiseXOR(rhs);

    /// <summary>
    /// bitwise exclusive OR
    /// </summary>
    public static TThis operator ^(
        TNativeType lhs,
        TypedIntegralColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).BitwiseXOR(rhs.Column));

    /// <summary>
    /// Compute bitwise XOR of this expression with another expression.
    /// </summary>
    /// <param name="other">column</param>
    /// <returns>column</returns>
    public TThis BitwiseXOR(TypedIntegralColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.BitwiseXOR(other.Column));

    /// <summary>
    /// Compute bitwise XOR of this expression with another expression.
    /// </summary>
    /// <param name="other">literal</param>
    /// <returns>column</returns>
    public TThis BitwiseXOR(TNativeType other) => New(Column.BitwiseXOR(F.Lit(other)));

    /// <summary>
    /// Returns the bitwise XOR of all non-null input values, or null if none
    /// </summary>
    /// <returns>column</returns>
    [Since("3.0.0")]
    public TThis BitwiseXOR() => New(F.Expr($"bit_xor({Column})"));

    /// <summary>
    /// Bitwise NOT
    /// </summary>
    public static TThis operator ~(TypedIntegralColumn<TThis, TSparkType, TNativeType> type) =>
        type.BitwiseNot();

    /// <summary>
    /// Computes bitwise NOT of a number
    /// </summary>
    /// <returns>column</returns>
    [Since("3.2.0")]
    public TThis BitwiseNot() => New(F.Bitwise_Not(Column));
}
