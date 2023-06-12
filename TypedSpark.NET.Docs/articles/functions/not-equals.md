# Not Equals (!=)

> _Since 1.0.0_

`expr1` != `expr2` - Returns true if `expr1` is not equal to `expr2`, or false
otherwise.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#_2)
* [API Docs](xref:TypedSpark.NET.Columns.TypedColumn`3.op_Inequality*)

> [!NOTE]
> `expr1`, `expr2` - the two expressions must be same type or can be casted to a
> common type, and must be a type that can be used in equality comparison. Map
> type is not supported. For complex types such array/struct, the data types of
> fields must be orderable.

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/NotEquals.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/NotEquals.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/NotEquals.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/NotEquals.Case2.md)]
