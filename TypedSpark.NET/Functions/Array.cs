using System;
using Microsoft.Spark;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

public static partial class Functions
{
    /// <summary>
    /// Applies a binary operator to an initial `start` state and all elements in the array `expr`,
    /// and reduces via a `merge` function to a single state.
    /// The final state is converted into the final result by applying a `finish` function.
    /// </summary>
    /// <param name="expr">typed array column</param>
    /// <param name="start">seed</param>
    /// <param name="merge">merge function</param>
    /// <param name="finish">projection function</param>
    /// <returns>aggregated state</returns>
    [Since("2.4.0")]
    public static TC Aggregate<TA, TB, TC>(
        this ArrayColumn<TA> expr,
        TB start,
        Func<TB, TA, TB> merge,
        Func<TB, TC> finish
    )
        where TA : TypedColumn, new()
        where TB : TypedColumn, new()
        where TC : TypedColumn, new() =>
        Expr<TC>(
            $"aggregate({expr.Column},{start.Column},(acc, x) -> {merge(Col<TB>("acc"), Col<TA>("x"))},x -> {finish(Col<TB>("x"))})"
        );

    /// <summary>
    /// Applies a binary operator to an initial `start` state and all elements in the array `expr`,
    /// and reduces via a `merge` function to a single state.
    /// </summary>
    /// <param name="expr">typed array column</param>
    /// <param name="start">seed</param>
    /// <param name="merge">merge function</param>
    /// <returns>aggregated state</returns>
    [Since("2.4.0")]
    public static TB Aggregate<TA, TB>(this ArrayColumn<TA> expr, TB start, Func<TB, TA, TB> merge)
        where TA : TypedColumn, new()
        where TB : TypedColumn, new() => Aggregate<TA, TB, TB>(expr, start, merge, _ => _);

    /// <summary>
    /// Returns the concatenation of `expr1` array of the elements with the `expr2` array
    /// </summary>
    /// <param name="expr1">array column 1</param>
    /// <param name="expr2">array column 2 </param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Concat<T>(this ArrayColumn<T> expr1, ArrayColumn<T> expr2)
        where T : TypedColumn, new() =>
        F.Concat(expr1.Column, expr2.Column).AsTyped<ArrayColumn<T>>();

    /// <summary>
    /// Returns null if the array `expr` is null, true if the array `expr` contains `value`,
    /// and false otherwise
    /// </summary>
    /// <param name="expr">array to test against</param>
    /// <param name="value">Value to check for existence</param>
    /// <returns>boolean column</returns>
    public static BooleanColumn Contains<T>(this ArrayColumn<T> expr, T value)
        where T : TypedColumn, TypedOrdColumn, new() =>
        F.ArrayContains(expr.Column, value.Column).AsTyped<BooleanColumn>();

    /// <summary>
    /// Removes duplicate values from the `expr` array
    /// </summary>
    /// <param name="expr">array column</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Distinct<T>(this ArrayColumn<T> expr)
        where T : TypedColumn, new() => F.ArrayDistinct(expr.Column).AsTyped<ArrayColumn<T>>();

    /// <summary>
    /// Returns an array of the elements in the `expr1` but not in the `expr2`,
    /// without duplicates. The order of elements in the result is nondeterministic.
    /// </summary>
    /// <param name="expr1">array column 1</param>
    /// <param name="expr2">array column 2 </param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Except<T>(this ArrayColumn<T> expr1, ArrayColumn<T> expr2)
        where T : TypedColumn, new() =>
        F.ArrayExcept(expr1.Column, expr2.Column).AsTyped<ArrayColumn<T>>();

    /// <summary>
    /// Returns an array of the elements in the intersection of `expr1` and `expr2`
    /// without duplicates
    /// </summary>
    /// <param name="expr1">array column 1</param>
    /// <param name="expr2">array column 2 </param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static ArrayColumn<T> Intersect<T>(this ArrayColumn<T> expr1, ArrayColumn<T> expr2)
        where T : TypedColumn, new() =>
        F.ArrayIntersect(expr1.Column, expr2.Column).AsTyped<ArrayColumn<T>>();

    /// <summary>
    /// Concatenates the elements of `expr` using the `delimiter`
    /// </summary>
    /// <param name="expr">column</param>
    /// <param name="delimiter">Delimiter for join</param>
    /// <param name="nullReplacement">String to replace null value</param>
    /// <returns>array column</returns>
    [Since("2.4.0")]
    public static StringColumn Join(
        this ArrayColumn<StringColumn> expr,
        string delimiter,
        string? nullReplacement = default
    ) =>
        (
            nullReplacement != null
                ? F.ArrayJoin(expr, delimiter, nullReplacement)
                : F.ArrayJoin(expr, delimiter)
        ).AsTyped<StringColumn>();
}
