# [Results](#tab/results)

|concat(array(1, 2, 3), array(1, 3, 5))|concat(array(1, 2, 3), array(1, 3, 5))|
|--------------------------------------|--------------------------------------|
|[1, 2, 3, 1, 3, 5]                    |[1, 2, 3, 1, 3, 5]                    |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- concat(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)
 |-- concat(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)

```
