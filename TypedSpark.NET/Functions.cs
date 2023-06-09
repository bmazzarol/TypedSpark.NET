﻿using System.Linq;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

/// <summary>
/// Typed versions of the functions.
/// </summary>
public static class Functions
{
    /// <summary>
    /// Null literal for any given typed column
    /// </summary>
    /// <typeparam name="T">column</typeparam>
    /// <returns>typed column</returns>
    public static T Null<T>()
        where T : TypedColumn, new() => new() { Column = F.Expr("NULL") };

    /// <summary>
    /// Evaluates a `condition` and returns one of multiple possible result expressions.
    /// If Otherwise(`value`) is not defined at the end, null is returned for
    /// unmatched conditions. This method can be chained with other 'when' invocations in case
    /// multiple matches are required.
    /// </summary>
    /// <param name="condition">The condition to check</param>
    /// <param name="value">The value to set if the condition is true</param>
    /// <returns>New column after applying the when method</returns>
    public static T When<T>(BooleanColumn condition, T value)
        where T : TypedColumn, new() => new() { Column = F.When((Column)condition, (Column)value) };

    /// <summary>
    /// Returns the first column that is not null, or null if all inputs are null.
    /// </summary>
    /// <param name="columns">Columns to apply</param>
    /// <returns>Column object</returns>
    public static T Coalesce<T>(params T[] columns)
        where T : TypedColumn, new() =>
        new() { Column = F.Coalesce(columns.Select(x => (Column)x).ToArray()) };

    /// <summary>
    /// Concatenates multiple input string columns together into a single string column,
    /// using the given separator.
    /// </summary>
    /// <param name="sep">Separator used for string concatenation</param>
    /// <param name="columns">Columns to apply</param>
    /// <returns>string column</returns>
    public static StringColumn ConcatWs(string sep, params TypedColumn[] columns) =>
        StringColumn.New(F.ConcatWs(sep, columns.Select(x => (Column)x).ToArray()));

    /// <summary>
    /// Returns the approximate number of distinct items in a group
    /// </summary>
    /// <param name="column">column</param>
    /// <param name="rsd">maximum estimation error allowed</param>
    /// <returns>long column</returns>
    public static LongColumn ApproxCountDistinct<T>(this T column, double? rsd = default)
        where T : TypedColumn =>
        LongColumn.New(
            rsd.HasValue
                ? F.ApproxCountDistinct(column.Column, rsd.Value)
                : F.ApproxCountDistinct(column.Column)
        );

    /// <summary>
    /// Returns the approximate `percentile` of the numeric column `col` which
    /// is the smallest value in the ordered `col` values (sorted from least to greatest) such that
    /// no more than `percentage` of `col` values is less than the value or equal to that value.
    /// </summary>
    /// <param name="column">column</param>
    /// <param name="percentage">it must be between 0.0 and 1.0</param>
    /// <param name="accuracy">
    /// Positive numeric literal which controls approximation accuracy at the cost of memory.
    /// Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the
    /// approximation.
    /// </param>
    /// <returns>integer column</returns>
    [Since("2.1.0")]
    public static IntegerColumn ApproxPercentile<T>(
        this T column,
        DoubleColumn percentage,
        IntegerColumn accuracy
    )
        where T : TypedColumn, TypedNumericOrDateTimeColumn, new() =>
        IntegerColumn.New(F.PercentileApprox(column.Column, percentage.Column, accuracy.Column));

    /// <summary>
    /// Returns the approximate `percentile` of the numeric column `col` which
    /// is the smallest value in the ordered `col` values (sorted from least to greatest) such that
    /// no more than `percentage` of `col` values is less than the value or equal to that value.
    /// </summary>
    /// <param name="column">colum</param>
    /// <param name="percentages">percentage array values must be between 0.0 and 1.0</param>
    /// <param name="accuracy">
    /// Positive numeric literal which controls approximation accuracy at the cost of memory.
    /// Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the
    /// approximation.
    /// </param>
    /// <returns>integer column</returns>
    [Since("2.1.0")]
    public static ArrayColumn<IntegerColumn> ApproxPercentile<T>(
        this T column,
        ArrayColumn<DoubleColumn> percentages,
        IntegerColumn accuracy
    )
        where T : TypedColumn, TypedNumericOrDateTimeColumn, new() =>
        ArrayColumn.New<IntegerColumn>(
            F.PercentileApprox(column.Column, percentages.Column, accuracy.Column)
        );

    /// <summary>
    /// Returns null if the condition is true, and throws an exception otherwise
    /// </summary>
    /// <param name="assertion">boolean column</param>
    /// <returns>void column</returns>
    [Since("2.0.0")]
    public static VoidColumn Assert(BooleanColumn assertion) => new(F.AssertTrue(assertion.Column));

    /// <summary>
    /// Computes atan2 for the given `x` and `y`
    /// </summary>
    /// <param name="y">coordinate on y-axis</param>
    /// <param name="x">coordinate on x-axis</param>
    /// <returns>double column</returns>
    [Since("1.4.0")]
    public static DoubleColumn Atan2<TA, TB>(this TA y, TB x)
        where TA : TypedColumn, TypedNumericColumn
        where TB : TypedColumn, TypedNumericColumn => DoubleColumn.New(F.Atan2(y.Column, x.Column));
}
