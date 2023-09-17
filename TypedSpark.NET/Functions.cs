using System.Linq;
using Microsoft.Spark;
using Microsoft.Spark.Sql;
using TypedSpark.NET.Columns;
using F = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

/// <summary>
/// Typed versions of the Spark SQL functions
/// </summary>
public static partial class Functions
{
    /// <summary>
    /// Null literal for any given typed column
    /// </summary>
    /// <typeparam name="T">column</typeparam>
    /// <returns>typed column</returns>
    public static T Null<T>()
        where T : TypedColumn, new() => Expr<T>("NULL");

    /// <summary>
    /// Returns a new typed column from the provided `column`
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>typed column</returns>
    public static T New<T>(Column column)
        where T : TypedColumn, new() => new() { Column = column };

    /// <summary>
    /// Returns a new typed column from the provided `column`
    /// </summary>
    /// <param name="column">column</param>
    /// <returns>typed column</returns>
    public static T AsTyped<T>(this Column column)
        where T : TypedColumn, new() => New<T>(column);

    /// <summary>
    /// Returns a Column based on the given column `name`
    /// </summary>
    /// <param name="name">column name</param>
    /// <returns>typed column</returns>
    public static T Col<T>(string name)
        where T : TypedColumn, new() => F.Col(name).AsTyped<T>();

    /// <summary>
    /// Parses the `expr` string into the column that it represents
    /// </summary>
    /// <param name="expr">expression to parse</param>
    /// <returns>typed column</returns>
    public static T Expr<T>(string expr)
        where T : TypedColumn, new() => F.Expr(expr).AsTyped<T>();

    /// <summary>
    /// Returns the first column that is not null, or null if all inputs are null.
    /// </summary>
    /// <param name="columns">columns to apply</param>
    /// <returns>column</returns>
    [Since("1.0.0")]
    public static T Coalesce<T>(params T[] columns)
        where T : TypedColumn, new() =>
        F.Coalesce(columns.Select(x => x.Column).ToArray()).AsTyped<T>();
}
