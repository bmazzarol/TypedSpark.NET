using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Ordering operators
/// </summary>
public abstract class TypedOrdColumn<TThis, TSparkType, TNativeType>
    : TypedColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    protected TypedOrdColumn(TSparkType columnType, Column column) : base(columnType, column) { }

    //
    // Greater Than Operations
    //

    public static BooleanColumn operator >(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Gt(rhs);

    public static BooleanColumn operator >(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Gt(lhs);

    public static BooleanColumn operator >(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Gt(rhs);

    public BooleanColumn Gt(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Gt((Column)rhs));

    public BooleanColumn Gt(TNativeType rhs) => BooleanColumn.New(Column.Gt(Lit(rhs)));

    //
    // Greater Than or Equal To Operations
    //

    public static BooleanColumn operator >=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Geq(rhs);

    public static BooleanColumn operator >=(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Geq(lhs);

    public static BooleanColumn operator >=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Geq(rhs);

    public BooleanColumn Geq(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Geq((Column)rhs));

    public BooleanColumn Geq(TNativeType rhs) => BooleanColumn.New(Column.Geq(Lit(rhs)));

    //
    // Less Than Operations
    //

    public static BooleanColumn operator <(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Lt(rhs);

    public static BooleanColumn operator <(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Lt(lhs);

    public static BooleanColumn operator <(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Lt(rhs);

    public BooleanColumn Lt(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Lt((Column)rhs));

    public BooleanColumn Lt(TNativeType rhs) => BooleanColumn.New(Column.Lt(Lit(rhs)));

    //
    // Less Than or Equal To Operations
    //

    public static BooleanColumn operator <=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Leq(rhs);

    public static BooleanColumn operator <=(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Leq(lhs);

    public static BooleanColumn operator <=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Leq(rhs);

    public BooleanColumn Leq(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Leq((Column)rhs));

    public BooleanColumn Leq(TNativeType rhs) => BooleanColumn.New(Column.Leq(Lit(rhs)));
}
