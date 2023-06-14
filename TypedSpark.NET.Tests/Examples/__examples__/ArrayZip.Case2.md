# [Results](#tab/results)

|arrays_zip(array(1, 2, 3, 4), array(2.0, 3.2, 4.5), array(a, b))|
|----------------------------------------------------------------|
|[{1, 2.0, a}, {2, 3.2, b}, {3, 4.5, null}, {4, null, null}]     |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- arrays_zip(array(1, 2, 3, 4), array(2.0, 3.2, 4.5), array(a, b)): array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- 0: integer (nullable = true)
 |    |    |-- 1: double (nullable = true)
 |    |    |-- 2: string (nullable = true)

```
