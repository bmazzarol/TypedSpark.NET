using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

public abstract class TypedNumericColumn<TThis, TSparkType, TNativeType>
    : TypedOrdColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>
    where TSparkType : DataType
{
    protected TypedNumericColumn(string columnName, TSparkType columnType, Column column)
        : base(columnName, columnType, column) { }

    /// <summary>Negate the given column.</summary>
    /// <param name="self">Column to negate</param>
    /// <returns>New column after applying negation</returns>
    public static TThis operator -(TypedNumericColumn<TThis, TSparkType, TNativeType> self) =>
        self.Negate();

    /// <summary>Negate the column.</summary>
    /// <returns>New column after applying negation</returns>
    public TThis Negate() => New(-Column);

    /// <summary>Apply sum of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the sum operation</returns>
    public static TThis operator +(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Plus(rhs);

    /// <summary>Apply sum of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the sum operation</returns>
    public static TThis operator +(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Plus(rhs);

    /// <summary>Sum of this expression and another expression.</summary>
    /// <param name="rhs">The expression to be summed with</param>
    /// <returns>New column after applying the plus operator</returns>
    public TThis Plus(TNativeType rhs) => New(Column.Plus(Lit(rhs)));

    /// <summary>Sum of this expression and another expression.</summary>
    /// <param name="rhs">The expression to be summed with</param>
    /// <returns>New column after applying the plus operator</returns>
    public TThis Plus(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Plus((Column)rhs));

    /// <summary>Apply subtraction of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the subtraction operation</returns>
    public static TThis operator -(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Minus(rhs);

    /// <summary>Apply subtraction of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the subtraction operation</returns>
    public static TThis operator -(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Minus(rhs);

    /// <summary>
    /// Subtraction. Subtract the other expression from this expression.
    /// </summary>
    /// <param name="rhs">The expression to be subtracted with</param>
    /// <returns>New column after applying the minus operator</returns>
    public TThis Minus(TNativeType rhs) => New(Column.Minus(Lit(rhs)));

    /// <summary>
    /// Subtraction. Subtract the other expression from this expression.
    /// </summary>
    /// <param name="rhs">The expression to be subtracted with</param>
    /// <returns>New column after applying the minus operator</returns>
    public TThis Minus(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Minus((Column)rhs));

    /// <summary>Apply multiplication of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the multiplication operation</returns>
    public static TThis operator *(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Multiply(rhs);

    /// <summary>Apply multiplication of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the multiplication operation</returns>
    public static TThis operator *(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Multiply(rhs);

    /// <summary>
    /// Multiplication of this expression and another expression.
    /// </summary>
    /// <param name="rhs">The expression to be multiplied with</param>
    /// <returns>New column after applying the multiply operator</returns>
    public TThis Multiply(TNativeType rhs) => New(Column.Multiply(Lit(rhs)));

    /// <summary>
    /// Multiplication of this expression and another expression.
    /// </summary>
    /// <param name="rhs">The expression to be multiplied with</param>
    /// <returns>New column after applying the multiply operator</returns>
    public TThis Multiply(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Multiply((Column)rhs));

    /// <summary>Apply division of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the division operation</returns>
    public static TThis operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Divide(rhs);

    /// <summary>Apply division of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the division operation</returns>
    public static TThis operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Divide(rhs);

    /// <summary>Division of this expression by another expression.</summary>
    /// <param name="rhs">The expression to be divided by</param>
    /// <returns>New column after applying the divide operator</returns>
    public TThis Divide(TNativeType rhs) => New(Column.Divide(Lit(rhs)));

    /// <summary>Division of this expression by another expression.</summary>
    /// <param name="rhs">The expression to be divided by</param>
    /// <returns>New column after applying the divide operator</returns>
    public TThis Divide(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Divide((Column)rhs));

    /// <summary>Apply division of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the division operation</returns>
    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Mod(rhs);

    /// <summary>Apply division of two expressions.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Object on the right side of the operator</param>
    /// <returns>New column after applying the division operation</returns>
    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Mod(rhs);

    /// <summary>Modulo (a.k.a remainder) expression.</summary>
    /// <param name="rhs">
    /// The expression to be divided by to get the remainder for.
    /// </param>
    /// <returns>New column after applying the mod operator</returns>
    public TThis Mod(TNativeType rhs) => New(Column.Mod(Lit(rhs)));

    /// <summary>Modulo (a.k.a remainder) expression.</summary>
    /// <param name="rhs">
    /// The expression to be divided by to get the remainder for.
    /// </param>
    /// <returns>New column after applying the mod operator</returns>
    public TThis Mod(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Mod((Column)rhs));
}