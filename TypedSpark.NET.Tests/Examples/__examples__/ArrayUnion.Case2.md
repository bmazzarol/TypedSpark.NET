# [Results](#tab/results)

|array_union(array(a, b, c), array(a, c, d))|array_union(array(a, b, c), array(a, c, d))|
|-------------------------------------------|-------------------------------------------|
|[a, b, c, d]                               |[a, b, c, d]                               |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_union(array(a, b, c), array(a, c, d)): array (nullable = false)
 |    |-- element: string (containsNull = false)
 |-- array_union(array(a, b, c), array(a, c, d)): array (nullable = false)
 |    |-- element: string (containsNull = false)

```
