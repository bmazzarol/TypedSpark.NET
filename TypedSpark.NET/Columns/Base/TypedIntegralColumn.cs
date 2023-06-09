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
    ) => rhs.BitwiseOR(lhs);

    /// <summary>
    /// Compute bitwise OR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise OR.
    /// </param>
    /// <returns>column</returns>
    public TThis BitwiseOR(TypedIntegralColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.BitwiseOR(other.Column));

    /// <summary>
    /// Compute bitwise OR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise OR.
    /// </param>
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
    ) => rhs.BitwiseAND(lhs);

    /// <summary>
    /// Compute bitwise AND of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise AND.
    /// </param>
    /// <returns>column</returns>
    public TThis BitwiseAND(TypedIntegralColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.BitwiseAND(other.Column));

    /// <summary>
    /// Compute bitwise AND of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise AND.
    /// </param>
    /// <returns>column</returns>
    public TThis BitwiseAND(TNativeType other) => New(Column.BitwiseAND(F.Lit(other)));

    /// <summary>
    /// Compute bitwise XOR of this expression with another expression.
    /// </summary>
    /// <param name="other">
    /// The other column that will be used to compute the bitwise XOR.
    /// </param>
    /// <returns>column</returns>
    public TThis BitwiseXOR(TThis other) => New(Column.BitwiseXOR(other.Column));
}
