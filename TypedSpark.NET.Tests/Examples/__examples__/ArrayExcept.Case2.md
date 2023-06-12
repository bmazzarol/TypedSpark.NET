# [Results](#tab/results)

|array_except(array(a, b, c), array(a, c, d))|
|--------------------------------------------|
|[b]                                         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_except(array(a, b, c), array(a, c, d)): array (nullable = false)
 |    |-- element: string (containsNull = false)

```
