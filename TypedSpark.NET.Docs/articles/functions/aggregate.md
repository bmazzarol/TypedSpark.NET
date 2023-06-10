# Aggregate

> _Since 2.4.0_

aggregate(`expr`, `start`, `merge`, `finish`) - Applies a binary operator to an
initial state and all elements in the array, and reduces this to a single state.
The final state is converted into the final result by applying a `finish`
function.

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#aggregate)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn`1.Aggregate*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/Aggregate.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/Aggregate.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/Aggregate.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/Aggregate.Case2.md)]
