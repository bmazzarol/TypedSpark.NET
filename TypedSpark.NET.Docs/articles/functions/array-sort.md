# Array Sort

> _Since 2.4.0_

array_sort(`expr`, `func`) - Sorts the input array. If `func` is omitted, sort
in ascending order. The elements of the input array must be orderable. NaN is
greater than any non-NaN elements for double/float type. Null elements will be
placed at the end of the returned array. Since 3.0.0 this function also sorts
and returns the array based on the given comparator function. The comparator
will take two arguments representing two elements of the array. It returns a
negative integer, 0, or a positive integer as the first element is less than,
equal to, or greater than the second element. If the comparator function returns
null, the function will fail and raise an error.

* [Spark Docs](https://spark.apache.org/docs/3.2.2/api/sql/index.html#array_sort)
* [API Docs](xref:TypedSpark.NET.Columns.ArrayColumn.Sort*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/ArraySort.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/ArraySort.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/ArraySort.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/ArraySort.Case2.md)]

[!code-csharp[Example3](../../../TypedSpark.NET.Tests/Examples/ArraySort.cs#Example3)]

[!INCLUDE [Example3](../../../TypedSpark.NET.Tests/Examples/__examples__/ArraySort.Case3.md)]
