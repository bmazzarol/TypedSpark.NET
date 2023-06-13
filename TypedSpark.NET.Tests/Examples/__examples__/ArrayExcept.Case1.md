# [Results](#tab/results)

|array_except(array(1, 2, 3), array(1, 3, 5))|array_except(array(1, 2, 3), array(1, 3, 5))|
|--------------------------------------------|--------------------------------------------|
|[2]                                         |[2]                                         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_except(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)
 |-- array_except(array(1, 2, 3), array(1, 3, 5)): array (nullable = false)
 |    |-- element: integer (containsNull = false)

```
