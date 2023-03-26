using Microsoft.Spark.Sql;
using TypedSpark.NET.Columns;
using UT = Microsoft.Spark.Sql.Functions;

namespace TypedSpark.NET;

/// <summary>
/// Typed versions of the functions.
/// </summary>
public static class Functions
{
    /// <summary>
    /// Computes the character length of a given string or number of bytes of a binary string.
    /// </summary>
    /// <remarks>
    /// The length of character strings includes the trailing spaces. The length of binary
    /// strings includes binary zeros.
    /// </remarks>
    /// <param name="column">Column to apply</param>
    /// <returns>Column object</returns>
    public static IntegerColumn Length(StringColumn column) =>
        IntegerColumn.New(UT.Length((Column)column));
}
