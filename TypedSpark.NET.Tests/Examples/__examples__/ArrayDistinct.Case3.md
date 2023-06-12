# [Results](#tab/results)

|array_distinct(array(DATE '2023-03-10', DATE '2023-03-10', DATE '2023-03-10'))|
|------------------------------------------------------------------------------|
|[2023-03-10]                                                                  |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_distinct(array(DATE '2023-03-10', DATE '2023-03-10', DATE '2023-03-10')): array (nullable = false)
 |    |-- element: date (containsNull = false)

```
