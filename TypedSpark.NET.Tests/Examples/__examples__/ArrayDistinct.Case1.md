# [Results](#tab/results)

|array_distinct(array(1, 2, 3, NULL, 3))|array_distinct(array(1, 2, 3, NULL, 3))|
|---------------------------------------|---------------------------------------|
|[1, 2, 3, null]                        |[1, 2, 3, null]                        |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_distinct(array(1, 2, 3, NULL, 3)): array (nullable = false)
 |    |-- element: integer (containsNull = true)
 |-- array_distinct(array(1, 2, 3, NULL, 3)): array (nullable = false)
 |    |-- element: integer (containsNull = true)

```
