using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Columns;

namespace TypedSpark.NET;

public abstract class TypedOrdColumn<TThis, TSparkType, TNativeType>
    : TypedColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    protected TypedOrdColumn(TSparkType columnType, Column column) : base(columnType, column) { }

    /// <summary>
    /// Apply "greater than" operator for the given two columns.
    /// </summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>New column after applying the operator</returns>
    public static BooleanColumn operator >(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Gt(rhs);

    /// <summary>Greater than.</summary>
    /// <param name="rhs">
    /// The object that is in comparison to test if the left hand side is greater.
    /// </param>
    /// <returns>New column after applying the greater than operator</returns>
    public BooleanColumn Gt(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Gt((Column)rhs));

    /// <summary>Apply "less than" operator for the given two columns.</summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>New column after applying the operator</returns>
    public static BooleanColumn operator <(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Lt(rhs);

    /// <summary>Less than.</summary>
    /// <param name="rhs">
    /// The object that is in comparison to test if the left hand side is lesser.
    /// </param>
    /// <returns>New column after applying the less than operator</returns>
    public BooleanColumn Lt(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Lt((Column)rhs));

    /// <summary>
    /// Apply "less than or equal to" operator for the given two columns.
    /// </summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>New column after applying the operator</returns>
    public static BooleanColumn operator <=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Leq(rhs);

    /// <summary>Less than or equal to.</summary>
    /// <param name="rhs">
    /// The object that is in comparison to test if the left hand side is less or equal to.
    /// </param>
    /// <returns>New column after applying the less than or equal to operator</returns>
    public BooleanColumn Leq(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Leq((Column)rhs));

    /// <summary>
    /// Apply "greater than or equal to" operator for the given two columns.
    /// </summary>
    /// <param name="lhs">Column on the left side of the operator</param>
    /// <param name="rhs">Column on the right side of the operator</param>
    /// <returns>New column after applying the operator</returns>
    public static BooleanColumn operator >=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Geq(rhs);

    /// <summary>Greater or equal to.</summary>
    /// <param name="rhs">
    /// The object that is in comparison to test if the left hand side is greater or equal to
    /// </param>
    /// <returns>New column after applying the greater or equal to operator</returns>
    public BooleanColumn Geq(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Geq((Column)rhs));
}
