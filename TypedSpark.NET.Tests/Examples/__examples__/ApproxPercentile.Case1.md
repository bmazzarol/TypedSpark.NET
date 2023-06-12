# [Results](#tab/results)

|percentile_approx(x, array(0.5, 0.4, 0.1), 100)|
|-----------------------------------------------|
|[1, 1, 0]                                      |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- percentile_approx(x, array(0.5, 0.4, 0.1), 100): array (nullable = true)
 |    |-- element: integer (containsNull = false)

```
