# [Results](#tab/results)

|array_intersect(array(a, b, c), array(a, c, d))|array_intersect(array(a, b, c), array(a, c, d))|
|-----------------------------------------------|-----------------------------------------------|
|[a, c]                                         |[a, c]                                         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_intersect(array(a, b, c), array(a, c, d)): array (nullable = false)
 |    |-- element: string (containsNull = false)
 |-- array_intersect(array(a, b, c), array(a, c, d)): array (nullable = false)
 |    |-- element: string (containsNull = false)

```
