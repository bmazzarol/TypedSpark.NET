# [Results](#tab/results)

|array_distinct(array(a, b, a, c, NULL, NULL, c, b))|
|---------------------------------------------------|
|[a, b, c, null]                                    |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_distinct(array(a, b, a, c, NULL, NULL, c, b)): array (nullable = false)
 |    |-- element: string (containsNull = true)

```
