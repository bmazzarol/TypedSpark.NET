# Array Max

> _Since 2.4.0_

array_max(`array`) - Returns the maximum value in the `array`. NaN is greater
than any non-NaN elements for double/float type. NULL elements are skipped.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#array_max)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn.Max*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArrayMax.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMax.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArrayMax.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMax.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArrayMax.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMax.Case3.md)]

[!code-csharp[Example4](../../../TypedSpark.NET.Tests/Examples/ArrayMax.cs#Example4)]

[!INCLUDE [Example4](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMax.Case4.md)]
