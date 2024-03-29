﻿using System.Diagnostics.CodeAnalysis;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET.Columns;

/// <summary>
/// Marker interface that indicates the column is a numeric column
/// </summary>
[SuppressMessage("Minor Code Smell", "S101:Types should be named in PascalCase")]
[SuppressMessage("Naming", "CA1715:Identifiers should have correct prefix")]
[SuppressMessage("ReSharper", "InconsistentNaming")]
public interface TypedNumericColumn { }

/// <summary>
/// Numeric operators
/// </summary>
public abstract class TypedNumericColumn<TThis, TSparkType, TNativeType>
    : TypedOrdColumn<TThis, TSparkType, TNativeType>,
        TypedNumericColumn,
        TypedNumericOrIntervalColumn,
        TypedNumericOrTemporalColumn,
        TypedNumericOrDateTimeColumn
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>, new()
    where TSparkType : DataType
{
    /// <summary>
    /// Constructor that enforces that a typed column has a spark data type and underlying untyped column
    /// </summary>
    /// <param name="columnType">spark data type</param>
    /// <param name="column">untyped column</param>
    protected TypedNumericColumn(TSparkType columnType, Column column)
        : base(columnType, column) { }

    private static TThis New(Column column) => new() { Column = column };

    /// <summary>
    /// Negate the given column
    /// </summary>
    /// <param name="self">column</param>
    /// <returns>column</returns>
    public static TThis operator -(TypedNumericColumn<TThis, TSparkType, TNativeType> self) =>
        self.Negate();

    /// <summary>
    /// Negate the column
    /// </summary>
    /// <returns>column</returns>
    public TThis Negate() => New(-Column);

    /// <summary>
    /// Sum
    /// </summary>
    public static TThis operator +(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Plus(rhs);

    /// <summary>
    /// Sum
    /// </summary>
    public static TThis operator +(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).Plus(rhs.Column));

    /// <summary>
    /// Sum
    /// </summary>
    public static TThis operator +(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Plus(rhs);

    /// <summary>
    /// Sum
    /// </summary>
    public TThis Plus(TypedNumericColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.Plus((Column)other));

    /// <summary>
    /// Sum
    /// </summary>
    public TThis Plus(TNativeType other) => New(Column.Plus(F.Lit(other)));

    /// <summary>
    /// Subtract
    /// </summary>
    public static TThis operator -(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Minus(rhs);

    /// <summary>
    /// Subtract
    /// </summary>
    public static TThis operator -(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).Minus(rhs.Column));

    /// <summary>
    /// Subtract
    /// </summary>
    public static TThis operator -(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Minus(rhs);

    /// <summary>
    /// Subtract
    /// </summary>
    public TThis Minus(TypedNumericColumn<TThis, TSparkType, TNativeType> other) =>
        New(Column.Minus((Column)other));

    /// <summary>
    /// Subtract
    /// </summary>
    public TThis Minus(TNativeType other) => New(Column.Minus(F.Lit(other)));

    /// <summary>
    /// Multiply
    /// </summary>
    public static TThis operator *(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Multiply(rhs);

    /// <summary>
    /// Multiply
    /// </summary>
    public static TThis operator *(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).Multiply(rhs.Column));

    /// <summary>
    /// Multiply
    /// </summary>
    public static TThis operator *(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Multiply(rhs);

    /// <summary>
    /// Multiply
    /// </summary>
    public TThis Multiply(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Multiply((Column)rhs));

    /// <summary>
    /// Multiply
    /// </summary>
    public TThis Multiply(TNativeType rhs) => New(Column.Multiply(F.Lit(rhs)));

    /// <summary>
    /// Divide
    /// </summary>
    public static DoubleColumn operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Divide(rhs);

    /// <summary>
    /// Divide
    /// </summary>
    public static DoubleColumn operator /(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => DoubleColumn.New(F.Lit(lhs).Divide(rhs.Column));

    /// <summary>
    /// Divide
    /// </summary>
    public static DoubleColumn operator /(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Divide(rhs);

    /// <summary>
    /// Divide
    /// </summary>
    public DoubleColumn Divide(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        DoubleColumn.New(Column.Divide((Column)rhs));

    /// <summary>
    /// Divide
    /// </summary>
    public DoubleColumn Divide(TNativeType rhs) => DoubleColumn.New(Column.Divide(F.Lit(rhs)));

    /// <summary>
    /// Mod
    /// </summary>
    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.Mod(rhs);

    /// <summary>
    /// Mod
    /// </summary>
    public static TThis operator %(
        TNativeType lhs,
        TypedNumericColumn<TThis, TSparkType, TNativeType> rhs
    ) => New(F.Lit(lhs).Mod(rhs.Column));

    /// <summary>
    /// Mod
    /// </summary>
    public static TThis operator %(
        TypedNumericColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.Mod(rhs);

    /// <summary>
    /// Mod
    /// </summary>
    public TThis Mod(TypedNumericColumn<TThis, TSparkType, TNativeType> rhs) =>
        New(Column.Mod(rhs.Column));

    /// <summary>
    /// Mod
    /// </summary>
    public TThis Mod(TNativeType rhs) => New(Column.Mod(F.Lit(rhs)));

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
    [Since("1.5.0")]
    public BooleanColumn IsNaN() => BooleanColumn.New(F.IsNaN(Column));

    /// <summary>
    /// Returns inverse sine in radians
    /// </summary>
    /// <returns>double column</returns>
    [Since("1.4.0")]
    public DoubleColumn Asin() => DoubleColumn.New(F.Asin(Column));

    /// <summary>
    /// Returns the inverse hyperbolic sine
    /// </summary>
    /// <returns>double column</returns>
    [Since("3.0.0")]
    public DoubleColumn Asinh() => DoubleColumn.New(F.Asinh(Column));

    /// <summary>
    /// Returns the inverse tangent in radians
    /// </summary>
    /// <returns>double column</returns>
    [Since("1.4.0")]
    public DoubleColumn Atan() => DoubleColumn.New(F.Atan(Column));
}
