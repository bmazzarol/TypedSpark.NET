using System.Linq;
using Microsoft.Spark;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

public static partial class Functions
{
    /// <summary>
    /// Returns the concatenation of `expr1` string with the `expr2` string
    /// </summary>
    /// <param name="expr1">string column 1</param>
    /// <param name="expr2">string column 2 </param>
    /// <returns>string column</returns>
    [Since("2.4.0")]
    public static StringColumn Concat(this StringColumn expr1, StringColumn expr2) =>
        F.Concat(expr1.Column, expr2.Column).AsTyped<StringColumn>();

    /// <summary>
    /// Concatenates multiple input string columns together into a single string column,
    /// using the given separator.
    /// </summary>
    /// <param name="sep">Separator used for string concatenation</param>
    /// <param name="columns">Columns to apply</param>
    /// <returns>string column</returns>
    [Since("1.5.0")]
    public static StringColumn ConcatWs(string sep, params StringColumn[] columns) =>
        F.ConcatWs(sep, columns.Select(x => x.Column).ToArray()).AsTyped<StringColumn>();
}
