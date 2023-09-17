using Microsoft.Spark;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

public static partial class Functions
{
    /// <summary>
    /// Returns the date that is `months` after the current `expr`
    /// </summary>
    /// <param name="expr">typed temporal column</param>
    /// <param name="months">months to add</param>
    /// <returns>temporal column</returns>
    [Since("1.5.0")]
    public static T AddMonths<T>(this T expr, IntegerColumn months)
        where T : TypedColumn, TypedTemporalColumn, new() =>
        F.AddMonths(expr.Column, months).AsTyped<T>();
}
