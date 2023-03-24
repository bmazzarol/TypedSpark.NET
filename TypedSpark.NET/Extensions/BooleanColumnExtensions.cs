using System.Diagnostics.Contracts;
using TypedSpark.NET.Columns;

namespace TypedSpark.NET.Extensions;

public static class BooleanColumnExtensions
{
    /// <summary>
    /// Converts the boolean to a named column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <param name="name">column name</param>
    /// <returns>column</returns>
    [Pure]
    public static BooleanColumn As(this bool lit, string name) => BooleanColumn.New(name, lit);

    /// <summary>
    /// Converts the boolean to a literal column
    /// </summary>
    /// <param name="lit">literal</param>
    /// <returns>column</returns>
    [Pure]
    public static BooleanColumn Lit(this bool lit) => BooleanColumn.New(string.Empty, lit);
}
