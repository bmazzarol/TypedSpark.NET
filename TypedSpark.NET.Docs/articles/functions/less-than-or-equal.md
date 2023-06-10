# Less Than or Equal (<=)

> _Since 1.0.0_

`expr1` <= `expr2` - Returns true if `expr1` is less than or equal to `expr2`.

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#_9)
* [API Docs](xref:TypedSpark.NET.Columns.TypedOrdColumn`3.op_LessThanOrEqual*)

> [!NOTE]
> `expr1`, `expr2` - the two expressions must be same type or can be casted to a
> common type, and must be a type that can be used in equality comparison. Map
> type is not supported. For complex types such array/struct, the data types of
> fields must be orderable.

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/LessThanOrEqual.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/LessThanOrEqual.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/LessThanOrEqual.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/LessThanOrEqual.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/LessThanOrEqual.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/LessThanOrEqual.Case3.md)]

[!code-csharp[Example4](../../../TypedSpark.NET.Tests/Examples/LessThanOrEqual.cs#Example4)]

[!INCLUDE [Example4](../../../TypedSpark.NET.Tests/Examples/__examples__/LessThanOrEqual.Case4.md)]

[!code-csharp[Example5](../../../TypedSpark.NET.Tests/Examples/LessThanOrEqual.cs#Example5)]

[!INCLUDE [Example5](../../../TypedSpark.NET.Tests/Examples/__examples__/LessThanOrEqual.Case5.md)]
