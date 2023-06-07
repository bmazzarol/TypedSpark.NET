# [Results](#tab/results)

|(NOT (1 = 1))|(NOT (CAST(1 AS STRING) = 1))|
|-------------|-----------------------------|
|false        |false                        |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (NOT (1 = 1)): boolean (nullable = false)
 |-- (NOT (CAST(1 AS STRING) = 1)): boolean (nullable = false)

```
