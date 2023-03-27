using System;
using Microsoft.Spark.Sql;

namespace TypedSpark.NET.Extensions;

internal static class ColumnExtensions
{
    // implement this
    internal static Column[] ExtractColumns<T>(this T schema) => Array.Empty<Column>();

    // implement this
    internal static Column ExtractColumn<T>(this T column) => default!;
}
