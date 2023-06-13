# [Results](#tab/results)

|concat(array(a, NULL, b, c), array(a, c, NULL, d))|concat(array(a, NULL, b, c), array(a, c, NULL, d))|
|--------------------------------------------------|--------------------------------------------------|
|[a, null, b, c, a, c, null, d]                    |[a, null, b, c, a, c, null, d]                    |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- concat(array(a, NULL, b, c), array(a, c, NULL, d)): array (nullable = false)
 |    |-- element: string (containsNull = true)
 |-- concat(array(a, NULL, b, c), array(a, c, NULL, d)): array (nullable = false)
 |    |-- element: string (containsNull = true)

```
