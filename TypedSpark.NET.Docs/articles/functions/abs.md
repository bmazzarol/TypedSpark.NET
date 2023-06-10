# Abs

> _Since 1.2.0_

abs(`expr`) - Returns the absolute value of the numeric value.

> [!WARNING]
> The Spark documentation indicates that abs will work for interval types.
> All the testing I have done indicates that it will not, instead it fails with
> a cast error.
>
> So I have not supported it on the @TypedSpark.NET.Columns.IntervalColumn type.

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#abs)
* [API Docs](xref:TypedSpark.NET.Columns.TypedNumericColumn`3.Abs*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/Abs.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/Abs.Case1.md)]
