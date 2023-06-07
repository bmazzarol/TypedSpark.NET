# [Results](#tab/results)

|(NOT (1 = 2))|(NOT (CAST(1 AS STRING) = 2))|
|-------------|-----------------------------|
|true         |true                         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (NOT (1 = 2)): boolean (nullable = false)
 |-- (NOT (CAST(1 AS STRING) = 2)): boolean (nullable = false)

```
