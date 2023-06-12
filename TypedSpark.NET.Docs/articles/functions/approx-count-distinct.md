# Approx Count Distinct

> _Since 1.6.0_

approx_count_distinct(`expr`[, `relativeSD`]) - Returns the estimated
cardinality by HyperLogLog++. `relativeSD` defines the maximum relative standard
deviation allowed.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#approx_count_distinct)
* [API Docs](xref:TypedSpark.NET.Functions.ApproxCountDistinct*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ApproxCountDistinct.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ApproxCountDistinct.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ApproxCountDistinct.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ApproxCountDistinct.Case2.md)]
