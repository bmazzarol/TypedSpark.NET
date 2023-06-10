# Greater Than or Equal (>=)

> _Since 1.0.0_

`expr1` >= `expr2` - Returns true if `expr1` is greater than or equal to `expr2`.

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#_15)
* [API Docs](xref:TypedSpark.NET.Columns.TypedOrdColumn`3.op_GreaterThanOrEqual*)

> [!NOTE]
> `expr1`, `expr2` - the two expressions must be same type or can be casted to a
> common type, and must be a type that can be used in equality comparison. Map
> type is not supported. For complex types such array/struct, the data types of
> fields must be orderable.

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/GreaterThanOrEqual.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/GreaterThanOrEqual.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/GreaterThanOrEqual.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/GreaterThanOrEqual.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/GreaterThanOrEqual.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/GreaterThanOrEqual.Case3.md)]

[!code-csharp[Example4](../../../TypedSpark.NET.Tests/Examples/GreaterThanOrEqual.cs#Example4)]

[!INCLUDE [Example4](../../../TypedSpark.NET.Tests/Examples/__examples__/GreaterThanOrEqual.Case4.md)]

[!code-csharp[Example5](../../../TypedSpark.NET.Tests/Examples/GreaterThanOrEqual.cs#Example5)]

[!INCLUDE [Example5](../../../TypedSpark.NET.Tests/Examples/__examples__/GreaterThanOrEqual.Case5.md)]
