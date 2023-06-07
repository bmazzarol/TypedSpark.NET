using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Marker interface for all order-able columns
/// </summary>
[SuppressMessage("Naming", "CA1715:Identifiers should have correct prefix")]
[SuppressMessage("Minor Code Smell", "S101:Types should be named in PascalCase")]
public interface TypedOrdColumn { }

/// <summary>
/// Ordering operators
/// </summary>
public abstract class TypedOrdColumn<TThis, TSparkType, TNativeType>
    : TypedColumn<TThis, TSparkType, TNativeType>,
        TypedOrdColumn
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    /// <summary>
    /// Constructor that enforces that a typed column has a spark data type and underlying untyped column
    /// </summary>
    /// <param name="columnType">spark data type</param>
    /// <param name="column">untyped column</param>
    protected TypedOrdColumn(TSparkType columnType, Column column)
        : base(columnType, column) { }

    /// <summary>
    /// Greater Than
    /// </summary>
    public static BooleanColumn operator >(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Gt(rhs);

    /// <summary>
    /// Greater Than
    /// </summary>
    public static BooleanColumn operator >(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Gt(lhs);

    /// <summary>
    /// Greater Than
    /// </summary>
    public static BooleanColumn operator >(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Gt(rhs);

    /// <summary>
    /// Greater Than
    /// </summary>
    public BooleanColumn Gt(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Gt((Column)rhs));

    /// <summary>
    /// Greater Than
    /// </summary>
    public BooleanColumn Gt(TNativeType rhs) => BooleanColumn.New(Column.Gt(Lit(rhs)));

    /// <summary>
    /// Greater Than or Equal To
    /// </summary>
    public static BooleanColumn operator >=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Geq(rhs);

    /// <summary>
    /// Greater Than or Equal To
    /// </summary>
    public static BooleanColumn operator >=(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Geq(lhs);

    /// <summary>
    /// Greater Than or Equal To
    /// </summary>
    public static BooleanColumn operator >=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Geq(rhs);

    /// <summary>
    /// Greater Than or Equal To
    /// </summary>
    public BooleanColumn Geq(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Geq((Column)rhs));

    /// <summary>
    /// Greater Than or Equal To
    /// </summary>
    public BooleanColumn Geq(TNativeType rhs) => BooleanColumn.New(Column.Geq(Lit(rhs)));

    /// <summary>
    /// Less Than
    /// </summary>
    public static BooleanColumn operator <(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Lt(rhs);

    /// <summary>
    /// Less Than
    /// </summary>
    public static BooleanColumn operator <(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Lt(lhs);

    /// <summary>
    /// Less Than
    /// </summary>
    public static BooleanColumn operator <(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Lt(rhs);

    /// <summary>
    /// Less Than
    /// </summary>
    public BooleanColumn Lt(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Lt((Column)rhs));

    /// <summary>
    /// Less Than
    /// </summary>
    public BooleanColumn Lt(TNativeType rhs) => BooleanColumn.New(Column.Lt(Lit(rhs)));

    /// <summary>
    /// Less Than or Equal To
    /// </summary>
    public static BooleanColumn operator <=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Leq(rhs);

    /// <summary>
    /// Less Than or Equal To
    /// </summary>
    public static BooleanColumn operator <=(
        TNativeType lhs,
        TypedOrdColumn<TThis, TSparkType, TNativeType> rhs
    ) => rhs.Leq(lhs);

    /// <summary>
    /// Less Than or Equal To
    /// </summary>
    public static BooleanColumn operator <=(
        TypedOrdColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Leq(rhs);

    /// <summary>
    /// Less Than or Equal To
    /// </summary>
    public BooleanColumn Leq(TypedOrdColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(Column.Leq((Column)rhs));

    /// <summary>
    /// Less Than or Equal To
    /// </summary>
    public BooleanColumn Leq(TNativeType rhs) => BooleanColumn.New(Column.Leq(Lit(rhs)));
}
