# Array Join

> _Since 2.4.0_

array_join(`array`, `delimiter`[, `nullReplacement`]) - Concatenates the
elements of the given `array` using the `delimiter` and an optional string to
replace nulls. If no value is set for `nullReplacement`, any null value is
filtered.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#array_join)
* [API Docs](xref:TypedSpark.NET.Functions.Join*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArrayJoin.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayJoin.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArrayJoin.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayJoin.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArrayJoin.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayJoin.Case3.md)]
