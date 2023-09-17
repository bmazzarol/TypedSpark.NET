# Aggregate

> _Since 2.4.0_

aggregate(`expr`, `start`, `merge`, `finish`) - Applies a binary operator to an
initial `start` state and all elements in the array `expr`, and reduces via
a `merge` function to a single state.
The final state is converted into the final result by applying a `finish`
function.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#aggregate)
* [API Docs](xref:TypedSpark.NET.Functions.Aggregate*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/Aggregate.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/Aggregate.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/Aggregate.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/Aggregate.Case2.md)]
