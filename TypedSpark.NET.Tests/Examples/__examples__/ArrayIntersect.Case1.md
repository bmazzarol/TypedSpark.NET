# [Results](#tab/results)

|array_intersect(array(1, 2, 3), array(1, 3, 5))|array_intersect(array(1, 2, 3), array(1, 3, 5))|array_intersect(array(1, 2, 3), array(1, 3, 5))|
|-----------------------------------------------|-----------------------------------------------|-----------------------------------------------|
|[1, 3]                                         |[1, 3]                                         |[1, 3]                                         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_intersect(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)
 |-- array_intersect(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)
 |-- array_intersect(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)

```
