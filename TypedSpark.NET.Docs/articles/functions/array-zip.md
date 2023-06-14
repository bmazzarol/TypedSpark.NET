# Array Zip

> _Since 2.4.0_

arrays_zip(`a1`, `a2`, ...) - Returns a merged array of structs in which the
N-th struct contains all N-th values of input arrays.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#arrays_zip)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn.Zip*)

> [!NOTE]
> Up to 5 generic parameters are supported, if more are required they can be
> done in the same way, or create a PR and add more.

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArrayZip.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayZip.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArrayZip.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArrayZip.Case2.md)]
