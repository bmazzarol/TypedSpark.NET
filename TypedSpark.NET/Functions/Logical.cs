using Microsoft.Spark;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

public static partial class Functions
{
    /// <summary>
    /// Applies the boolean AND operator with between `expr1` and `expr2`
    /// </summary>
    /// <param name="expr1">first boolean column expr</param>
    /// <param name="expr2">second boolean column expr</param>
    /// <returns>boolean column</returns>
    [Since("1.0.0")]
    public static BooleanColumn And(this BooleanColumn expr1, BooleanColumn expr2) =>
        expr1.Column.And(expr2.Column).AsTyped<BooleanColumn>();

    /// <summary>
    /// Returns true if at least one value of `expr` is true
    /// </summary>
    /// <param name="expr">boolean column</param>
    /// <returns>boolean column</returns>
    [Since("3.0.0")]
    public static BooleanColumn Any(this BooleanColumn expr) =>
        Expr<BooleanColumn>($"any({expr.Column})");

    /// <summary>
    /// Returns null if the condition is true, and throws an exception otherwise
    /// </summary>
    /// <param name="assertion">boolean column</param>
    /// <returns>void column</returns>
    [Since("2.0.0")]
    public static VoidColumn Assert(BooleanColumn assertion) => new(F.AssertTrue(assertion.Column));

    /// <summary>
    /// Evaluates a `condition` and returns one of multiple possible result expressions.
    /// If Otherwise(`value`) is not defined at the end, null is returned for
    /// unmatched conditions. This method can be chained with other 'when' invocations in case
    /// multiple matches are required.
    /// </summary>
    /// <param name="condition">The condition to check</param>
    /// <param name="value">The value to set if the condition is true</param>
    /// <returns>New column after applying the when method</returns>
    public static T When<T>(this BooleanColumn condition, T value)
        where T : TypedColumn, new() => F.When(condition.Column, value.Column).AsTyped<T>();
}
