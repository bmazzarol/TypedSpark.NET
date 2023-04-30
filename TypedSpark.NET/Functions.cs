using System.Linq;
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
}
