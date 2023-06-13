# Array Min

> _Since 2.4.0_

array_min(`array`) - Returns the minimum value in the `array`. NaN is greater
than any non-NaN elements for double/float type. NULL elements are skipped.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#array_min)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn.Min*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArrayMin.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMin.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArrayMin.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMin.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArrayMin.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMin.Case3.md)]

[!code-csharp[Example4](../../../TypedSpark.NET.Tests/Examples/ArrayMin.cs#Example4)]

[!INCLUDE [Example4](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayMin.Case4.md)]
