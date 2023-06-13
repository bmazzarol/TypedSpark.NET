# Array Union

> _Since 2.4.0_

array_union(`array1`, `array2`) - Returns an array of the elements in the union
of `array1` and `array2`, without duplicates.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#array_union)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn`1.op_BitwiseAnd*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArrayUnion.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayUnion.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArrayUnion.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayUnion.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArrayUnion.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayUnion.Case3.md)]
