# Arrays Overlap

> _Since 2.4.0_

arrays_overlap(`a1`, `a2`) - Returns true if `a1` contains at least a non-null
element present also in `a2`. If the arrays have no common element and they are
both non-empty and either of them contains a null element null is returned,
false otherwise.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#arrays_overlap)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn`1.Overlaps*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArraysOverlap.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArraysOverlap.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArraysOverlap.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArraysOverlap.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArraysOverlap.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArraysOverlap.Case3.md)]
