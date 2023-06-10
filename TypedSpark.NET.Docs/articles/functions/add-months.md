# Add Months

> _Since 1.5.0_

add_months(`start_date`, `num_months`) - Returns the date that is `num_months`
after `start_date`.

* [Spark Docs](https://spark.apache.org/docs/latest/api/sql/index.html#add_months)
* [API Docs - Date](xref:TypedSpark.NET.Columns.DateColumn.AddMonths*)
* [API Docs - Timestamp](xref:TypedSpark.NET.Columns.TimestampColumn.AddMonths*)

## Examples

[!code-csharp[Example1](../../../TypedSpark.NET.Tests/Examples/AddMonths.cs#Example1)]

[!INCLUDE [Example1](../../../TypedSpark.NET.Tests/Examples/__examples__/AddMonths.Case1.md)]

[!code-csharp[Example2](../../../TypedSpark.NET.Tests/Examples/AddMonths.cs#Example2)]

[!INCLUDE [Example2](../../../TypedSpark.NET.Tests/Examples/__examples__/AddMonths.Case2.md)]
