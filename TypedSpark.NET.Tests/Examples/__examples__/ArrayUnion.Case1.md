# [Results](#tab/results)

|array_union(array(1, 2, 3), array(1, 3, 5))|array_union(array(1, 2, 3), array(1, 3, 5))|
|-------------------------------------------|-------------------------------------------|
|[1, 2, 3, 5]                               |[1, 2, 3, 5]                               |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_union(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)
 |-- array_union(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)

```
