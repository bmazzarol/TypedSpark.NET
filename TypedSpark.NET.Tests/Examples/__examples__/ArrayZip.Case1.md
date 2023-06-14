# [Results](#tab/results)

|arrays_zip(array(1, 2, 3), array(2, 3, 4))|
|------------------------------------------|
|[{1, 2}, {2, 3}, {3, 4}]                  |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- arrays_zip(array(1, 2, 3), array(2, 3, 4)): array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- 0: integer (nullable = true)
 |    |    |-- 1: integer (nullable = true)

```
