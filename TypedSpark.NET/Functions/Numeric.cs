using Microsoft.Spark;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

public static partial class Functions
{
    /// <summary>
    /// Returns the absolute value of `expr`
    /// </summary>
    /// <param name="expr">typed numeric column</param>
    /// <returns>numeric column</returns>
    [Since("1.2.0")]
    public static T Abs<T>(this T expr)
        where T : TypedColumn, TypedNumericColumn, new() => F.Abs(expr.Column).AsTyped<T>();

    /// <summary>
    /// Returns the inverse cosine (a.k.a. arc cosine) of `expr`
    /// </summary>
    /// <param name="expr">typed numeric column</param>
    /// <returns>double column</returns>
    [Since("1.4.0")]
    public static DoubleColumn Acos<T>(this T expr)
        where T : TypedColumn, TypedNumericColumn, new() =>
        F.Acos(expr.Column).AsTyped<DoubleColumn>();

    /// <summary>
    /// Returns inverse hyperbolic cosine of `expr`
    /// </summary>
    /// <param name="expr">typed numeric column</param>
    /// <returns>double column</returns>
    [Since("3.1.0")]
    public static DoubleColumn Acosh<T>(this T expr)
        where T : TypedColumn, TypedNumericColumn, new() =>
        F.Acosh(expr.Column).AsTyped<DoubleColumn>();

    /// <summary>
    /// Returns the approximate number of distinct items in a group
    /// </summary>
    /// <param name="expr">column</param>
    /// <param name="rsd">maximum estimation error allowed</param>
    /// <returns>long column</returns>
    [Since("1.6.0")]
    public static LongColumn ApproxCountDistinct<T>(this T expr, double? rsd = default)
        where T : TypedColumn =>
        (
            rsd.HasValue
                ? F.ApproxCountDistinct(expr.Column, rsd.Value)
                : F.ApproxCountDistinct(expr.Column)
        ).AsTyped<LongColumn>();

    /// <summary>
    /// Returns the approximate `percentile` of the numeric column `expr` which
    /// is the smallest value in the ordered `expr` values (sorted from least to greatest) such that
    /// no more than `percentage` of `expr` values is less than the value or equal to that value.
    /// </summary>
    /// <param name="expr">typed numeric or date time column</param>
    /// <param name="percentage">it must be between 0.0 and 1.0</param>
    /// <param name="accuracy">
    /// Positive numeric literal which controls approximation accuracy at the cost of memory.
    /// Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the
    /// approximation.
    /// </param>
    /// <returns>integer column</returns>
    [Since("2.1.0")]
    public static IntegerColumn ApproxPercentile<T>(
        this T expr,
        DoubleColumn percentage,
        IntegerColumn accuracy
    )
        where T : TypedColumn, TypedNumericOrDateTimeColumn, new() =>
        F.PercentileApprox(expr.Column, percentage.Column, accuracy.Column)
            .AsTyped<IntegerColumn>();

    /// <summary>
    /// Returns the approximate `percentile` of the numeric column `expr` which
    /// is the smallest value in the ordered `expr` values (sorted from least to greatest) such that
    /// no more than `percentages` of `expr` values is less than the value or equal to that value.
    /// </summary>
    /// <param name="expr">typed numeric or date time column</param>
    /// <param name="percentages">percentage array values must be between 0.0 and 1.0</param>
    /// <param name="accuracy">
    /// Positive numeric literal which controls approximation accuracy at the cost of memory.
    /// Higher value of accuracy yields better accuracy, 1.0/accuracy is the relative error of the
    /// approximation.
    /// </param>
    /// <returns>array integer column</returns>
    [Since("2.1.0")]
    public static ArrayColumn<IntegerColumn> ApproxPercentile<T>(
        this T expr,
        ArrayColumn<DoubleColumn> percentages,
        IntegerColumn accuracy
    )
        where T : TypedColumn, TypedNumericOrDateTimeColumn, new() =>
        F.PercentileApprox(expr.Column, percentages.Column, accuracy.Column)
            .AsTyped<ArrayColumn<IntegerColumn>>();

    /// <summary>
    /// Computes atan2 for the given `x` and `y`
    /// </summary>
    /// <param name="y">coordinate on y-axis</param>
    /// <param name="x">coordinate on x-axis</param>
    /// <returns>double column</returns>
    [Since("1.4.0")]
    public static DoubleColumn Atan2<TA, TB>(this TA y, TB x)
        where TA : TypedColumn, TypedNumericColumn
        where TB : TypedColumn, TypedNumericColumn =>
        F.Atan2(y.Column, x.Column).AsTyped<DoubleColumn>();
}
