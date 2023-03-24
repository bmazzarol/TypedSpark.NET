using System;
using Microsoft.Spark.Sql;

namespace TypedSpark.NET.Extensions;

public static class ColumnExtensions
{
    // implement this
    public static Column[] ExtractColumns<T>(this T schema) => Array.Empty<Column>();
}
