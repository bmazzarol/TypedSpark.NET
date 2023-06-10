# [Results](#tab/results)

|(2.0 >= CAST(2.1 AS DOUBLE))|(2.0 >= CAST(2.1 AS DOUBLE))|
|----------------------------|----------------------------|
|false                       |false                       |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (2.0 >= CAST(2.1 AS DOUBLE)): boolean (nullable = true)
 |-- (2.0 >= CAST(2.1 AS DOUBLE)): boolean (nullable = true)

```
