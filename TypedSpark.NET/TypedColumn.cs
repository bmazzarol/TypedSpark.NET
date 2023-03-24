﻿#pragma warning disable CS0660, CS0661

using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Expressions;
using Microsoft.Spark.Sql.Types;
using TypedSpark.NET.Columns;
using static Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

/// <summary>
/// Provides a typed version of a spark column
/// </summary>
/// <typeparam name="TThis">current extending type</typeparam>
/// <typeparam name="TSparkType">supported spark type</typeparam>
/// <typeparam name="TNativeType">native dotnet type</typeparam>
public abstract class TypedColumn<TThis, TSparkType, TNativeType>
    where TThis : TypedColumn<TThis, TSparkType, TNativeType>
    where TSparkType : DataType
{
    /// <summary>
    /// Underlying column this definition defines
    /// </summary>
    protected Column Column { get; }

    /// <summary>
    /// Name of the column
    /// </summary>
    public string ColumnName { get; }

    /// <summary>
    /// Underlying spark type of the column
    /// </summary>
    public TSparkType ColumnType { get; }

    protected TypedColumn(string columnName, TSparkType columnType, Column column)
    {
        Column = column;
        ColumnName = columnName;
        ColumnType = columnType;
    }

    public static explicit operator Column(
        TypedColumn<TThis, TSparkType, TNativeType> typedColumn
    ) => typedColumn.Column;

    protected abstract TThis New(Column column, string? name = default);

    /// <summary>Apply equality test on the given two columns.</summary>
    /// <param name="lhs">Column on the left side of equality test</param>
    /// <param name="rhs">Column on the right side of equality test</param>
    /// <returns>New column after applying equality test</returns>
    public static BooleanColumn operator ==(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.EqualTo(rhs);

    /// <summary>Apply equality test on the given two columns.</summary>
    /// <param name="lhs">Column on the left side of equality test</param>
    /// <param name="rhs">Column on the right side of equality test</param>
    /// <returns>New column after applying equality test</returns>
    public static BooleanColumn operator ==(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TypedColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.EqualTo(rhs);

    /// <summary>Equality test.</summary>
    /// <param name="rhs">The right hand side of expression being tested for equality</param>
    /// <returns>New column after applying the equal to operator</returns>
    public BooleanColumn EqualTo(TNativeType rhs) =>
        BooleanColumn.New(ColumnName, Column.EqualTo(Lit(rhs)));

    /// <summary>Equality test.</summary>
    /// <param name="rhs">The right hand side of expression being tested for equality</param>
    /// <returns>New column after applying the equal to operator</returns>
    public BooleanColumn EqualTo(TypedColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(ColumnName, Column.EqualTo((Column)rhs));

    /// <summary>Apply inequality test.</summary>
    /// <param name="lhs">Column on the left side of inequality test</param>
    /// <param name="rhs">Column on the right side of inequality test</param>
    /// <returns>New column after applying inequality test</returns>
    public static BooleanColumn operator !=(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TNativeType rhs
    ) => lhs.NotEqual(rhs);

    /// <summary>Apply inequality test.</summary>
    /// <param name="lhs">Column on the left side of inequality test</param>
    /// <param name="rhs">Column on the right side of inequality test</param>
    /// <returns>New column after applying inequality test</returns>
    public static BooleanColumn operator !=(
        TypedColumn<TThis, TSparkType, TNativeType> lhs,
        TypedColumn<TThis, TSparkType, TNativeType> rhs
    ) => lhs.NotEqual(rhs);

    /// <summary>Inequality test.</summary>
    /// <param name="rhs">
    /// The right hand side of expression being tested for inequality.
    /// </param>
    /// <returns>New column after applying not equal operator</returns>
    public BooleanColumn NotEqual(TNativeType rhs) =>
        BooleanColumn.New(ColumnName, Column.NotEqual(Lit(rhs)));

    /// <summary>Inequality test.</summary>
    /// <param name="rhs">
    /// The right hand side of expression being tested for inequality.
    /// </param>
    /// <returns>New column after applying not equal operator</returns>
    public BooleanColumn NotEqual(TypedColumn<TThis, TSparkType, TNativeType> rhs) =>
        BooleanColumn.New(ColumnName, Column.NotEqual((Column)rhs));

    /// <summary>Apply equality test that is safe for null values.</summary>
    /// <param name="obj">Object to apply equality test</param>
    /// <returns>New column after applying the equality test</returns>
    public BooleanColumn EqNullSafe(TThis obj) =>
        BooleanColumn.New(ColumnName, Column.EqNullSafe((Column)obj));

    /// <summary>
    /// Evaluates a condition and returns one of multiple possible result expressions.
    /// If Otherwise(object) is not defined at the end, null is returned for
    /// unmatched conditions. This method can be chained with other 'when' invocations in case
    /// multiple matches are required.
    /// </summary>
    /// <param name="condition">The condition to check</param>
    /// <param name="value">The value to set if the condition is true</param>
    /// <returns>New column after applying the when method</returns>
    public TThis When(BooleanColumn condition, TNativeType value) =>
        New(Column.When((Column)condition, value));

    /// <summary>
    /// Evaluates a condition and returns one of multiple possible result expressions.
    /// If Otherwise(object) is not defined at the end, null is returned for
    /// unmatched conditions. This method can be chained with other 'when' invocations in case
    /// multiple matches are required.
    /// </summary>
    /// <param name="condition">The condition to check</param>
    /// <param name="value">The value to set if the condition is true</param>
    /// <returns>New column after applying the when method</returns>
    public TThis When(BooleanColumn condition, TThis value) =>
        New(Column.When((Column)condition, (Column)value));

    /// <summary>
    /// Evaluates a list of conditions and returns one of multiple possible result expressions.
    /// If otherwise is not defined at the end, null is returned for unmatched conditions.
    /// This is used when the When(Column, object) method is applied.
    /// </summary>
    /// <param name="value">The value to set</param>
    /// <returns>New column after applying otherwise method</returns>
    public TThis Otherwise(TNativeType value) => New(Column.Otherwise(value));

    /// <summary>
    /// Evaluates a list of conditions and returns one of multiple possible result expressions.
    /// If otherwise is not defined at the end, null is returned for unmatched conditions.
    /// This is used when the When(Column, object) method is applied.
    /// </summary>
    /// <param name="value">The value to set</param>
    /// <returns>New column after applying otherwise method</returns>
    public TThis Otherwise(TThis value) => New(Column.Otherwise((Column)value));

    /// <summary>
    /// True if the current column is between the lower bound and upper bound, inclusive.
    /// </summary>
    /// <param name="lowerBound">The lower bound</param>
    /// <param name="upperBound">The upper bound</param>
    /// <returns>New column after applying the between method</returns>
    public BooleanColumn Between(TNativeType lowerBound, TNativeType upperBound) =>
        BooleanColumn.New(ColumnName, Column.Between(lowerBound, upperBound));

    /// <summary>
    /// True if the current column is between the lower bound and upper bound, inclusive.
    /// </summary>
    /// <param name="lowerBound">The lower bound</param>
    /// <param name="upperBound">The upper bound</param>
    /// <returns>New column after applying the between method</returns>
    public BooleanColumn Between(TThis lowerBound, TNativeType upperBound) =>
        BooleanColumn.New(ColumnName, Column.Between(lowerBound, upperBound));

    /// <summary>
    /// True if the current column is between the lower bound and upper bound, inclusive.
    /// </summary>
    /// <param name="lowerBound">The lower bound</param>
    /// <param name="upperBound">The upper bound</param>
    /// <returns>New column after applying the between method</returns>
    public BooleanColumn Between(TNativeType lowerBound, TThis upperBound) =>
        BooleanColumn.New(ColumnName, Column.Between(lowerBound, (Column)upperBound));

    /// <summary>
    /// True if the current column is between the lower bound and upper bound, inclusive.
    /// </summary>
    /// <param name="lowerBound">The lower bound</param>
    /// <param name="upperBound">The upper bound</param>
    /// <returns>New column after applying the between method</returns>
    public BooleanColumn Between(TThis lowerBound, TThis upperBound) =>
        BooleanColumn.New(ColumnName, Column.Between((Column)lowerBound, (Column)upperBound));

    /// <summary>True if the current expression is null.</summary>
    /// <returns>
    /// New column with values true if the preceding column had a null
    /// value in the same index, and false otherwise.
    /// </returns>
    public BooleanColumn IsNull() => BooleanColumn.New(ColumnName, Column.IsNull());

    /// <summary>True if the current expression is NOT null.</summary>
    /// <returns>
    /// New column with values true if the preceding column had a non-null
    /// value in the same index, and false otherwise.
    /// </returns>
    public BooleanColumn IsNotNull() => BooleanColumn.New(ColumnName, Column.IsNotNull());

    /// <summary>
    /// Casts the column to an untyped column
    /// </summary>
    /// <returns>Column object</returns>
    public Column CastToObject() => Column;

    /// <summary>Gives the column an alias. Same as `As()`.</summary>
    /// <param name="alias">The alias that is given</param>
    /// <returns>New column after applying an alias</returns>
    public TThis Alias(string alias) => New(Column.Alias(alias), alias);

    /// <summary>Gives the column an alias.</summary>
    /// <param name="alias">The alias that is given</param>
    /// <returns>New column after applying the as alias operator</returns>
    public TThis As(string alias) => Alias(alias);

    /// <summary>Gives the column a name (alias).</summary>
    /// <param name="alias">Alias column name</param>
    /// <returns>Column object</returns>
    public TThis Name(string alias) => Alias(alias);

    /// <summary>
    /// Returns a sort expression based on ascending order of the column,
    /// and null values return before non-null values.
    /// </summary>
    /// <returns>New column after applying the descending order operator</returns>
    public TThis Desc() => New(Column.Desc());

    /// <summary>
    /// Returns a sort expression based on the descending order of the column,
    /// and null values appear before non-null values.
    /// </summary>
    /// <returns>Column object</returns>
    public TThis DescNullsFirst() => New(Column.DescNullsFirst());

    /// <summary>
    /// Returns a sort expression based on the descending order of the column,
    /// and null values appear after non-null values.
    /// </summary>
    /// <returns>Column object</returns>
    public TThis DescNullsLast() => New(Column.DescNullsLast());

    /// <summary>
    /// Returns a sort expression based on ascending order of the column.
    /// </summary>
    /// <returns>New column after applying the ascending order operator</returns>
    public TThis Asc() => New(Column.Asc());

    /// <summary>
    /// Returns a sort expression based on ascending order of the column,
    /// and null values return before non-null values.
    /// </summary>
    /// <returns></returns>
    public TThis AscNullsFirst() => New(Column.AscNullsFirst());

    /// <summary>
    /// Returns a sort expression based on ascending order of the column,
    /// and null values appear after non-null values.
    /// </summary>
    /// <returns></returns>
    public TThis AscNullsLast() => New(Column.AscNullsLast());

    /// <summary>Defines a windowing column.</summary>
    /// <param name="window">
    /// A window specification that defines the partitioning, ordering, and frame boundaries.
    /// </param>
    /// <returns>Column object</returns>
    public TThis Over(WindowSpec window) => New(Column.Over(window));

    /// <summary>
    /// Defines an empty analytic clause. In this case the analytic function is applied
    /// and presented for all rows in the result set.
    /// </summary>
    /// <returns>Column object</returns>
    public TThis Over() => New(Column.Over());

    /// <summary>
    /// Prints the expression to the console for debugging purposes.
    /// </summary>
    /// <param name="extended">To print extended version or not</param>
    public void Explain(bool extended) => Column.Explain(extended);

    public override string ToString() => Column.ToString();
}
