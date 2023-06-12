# Approx Percentile

> _Since 2.1.0_

approx_percentile(`col`, `percentage` [, `accuracy`]) - Returns the approximate
percentile of the numeric column `col` which is the smallest value in the
ordered `col` values (sorted from least to greatest) such that no more
than `percentage` of `col` values is less than the value or equal to that value.
The value of `percentage` must be between 0.0 and 1.0. The `accuracy`
parameter (default: 10000) is a positive numeric literal which controls
approximation accuracy at the cost of memory. Higher value of `accuracy` yields
better accuracy, 1.0/`accuracy` is the relative error of the approximation.
When `percentage` is an array, each value of the `percentage` array must be
between 0.0 and 1.0. In this case, returns the approximate percentile array of
column `col` at the given `percentage` array.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#approx_percentile)
* [API Docs](xref:TypedSpark.NET.Functions.ApproxPercentile*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ApproxPercentile.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ApproxPercentile.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ApproxPercentile.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ApproxPercentile.Case2.md)]
