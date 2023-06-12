# Array Intersect

> _Since 2.4.0_

array_intersect(`array1`, `array2`) - Returns an array of the elements in the
intersection of `array1` and `array2`, without duplicates.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#array_intersect)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn`1.op_BitwiseOr*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArrayIntersect.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayIntersect.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArrayIntersect.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayIntersect.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArrayIntersect.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayIntersect.Case3.md)]
