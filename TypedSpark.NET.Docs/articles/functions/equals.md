# Equals (==)

`expr1` == `expr2` - Returns true if `expr1` equals `expr2`, or false otherwise.

Since 1.1.0

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#_13)
* [API Docs](xref:TypedSpark.NET.Columns.TypedColumn`3.op_Equality*)

## Arguments

`expr1`, `expr2` - the two expressions must be same type or can be casted to a
common type, and must be a type that can be used in equality comparison. Map
type is not supported. For complex types such array/struct, the data types of
fields must be orderable.

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case3.md)]

[!code-csharp[Example4](../../../TypedSpark.NET.Tests/Examples/Equals.cs#Example4)]

[!INCLUDE [Example4](../../../TypedSpark.NET.Tests/Examples/__examples__/Equals.Case4.md)]
