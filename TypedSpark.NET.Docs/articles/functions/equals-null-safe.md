# Equals Null Safe (Known as <=>)

> _Since 1.1.0_

`expr1` <=> `expr2` - Returns same result as the EQUAL(=) operator for non-null
operands, but returns true if both are null, false if one of the them is null.

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#_10)
* [API Docs](xref:TypedSpark.NET.Columns.TypedColumn`2.EqNullSafe*)

> [!NOTE]
> `expr1`, `expr2` - the two expressions must be same type or can be casted to a
> common type, and must be a type that can be used in equality comparison. Map
> type is not supported. For complex types such array/struct, the data types of
> fields must be orderable.

## Examples

[!code-csharp[Example5](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example5)]

[!INCLUDE [Example5](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case5.md)]

[!code-csharp[Example6](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example6)]

[!INCLUDE [Example6](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case6.md)]

[!code-csharp[Example7](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example7)]

[!INCLUDE [Example7](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case7.md)]

[!code-csharp[Example8](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example8)]

[!INCLUDE [Example8](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case8.md)]
