# [Results](#tab/results)

|array_intersect(array(add_months(DATE '2023-03-10', 1), add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3)), array(add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3), add_months(DATE '2023-03-10', 4), add_months(DATE '2023-03-10', 5), add_months(DATE '2023-03-10', 6)))|array_intersect(array(add_months(DATE '2023-03-10', 1), add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3)), array(add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3), add_months(DATE '2023-03-10', 4), add_months(DATE '2023-03-10', 5), add_months(DATE '2023-03-10', 6)))|
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|[2023-05-10, 2023-06-10]                                                                                                                                                                                                                                                                                     |[2023-05-10, 2023-06-10]                                                                                                                                                                                                                                                                                     |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_intersect(array(add_months(DATE '2023-03-10', 1), add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3)), array(add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3), add_months(DATE '2023-03-10', 4), add_months(DATE '2023-03-10', 5), add_months(DATE '2023-03-10', 6))): array (nullable = false)
 |    |-- element: date (containsNull = false)
 |-- array_intersect(array(add_months(DATE '2023-03-10', 1), add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3)), array(add_months(DATE '2023-03-10', 2), add_months(DATE '2023-03-10', 3), add_months(DATE '2023-03-10', 4), add_months(DATE '2023-03-10', 5), add_months(DATE '2023-03-10', 6))): array (nullable = false)
 |    |-- element: date (containsNull = false)

```
