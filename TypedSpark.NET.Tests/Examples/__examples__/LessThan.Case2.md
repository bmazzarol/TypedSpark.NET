# [Results](#tab/results)

|(CAST(1.1 AS STRING) < 1)|(CAST(1.1 AS STRING) < 1)|
|-------------------------|-------------------------|
|false                    |false                    |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (CAST(1.1 AS STRING) < 1): boolean (nullable = false)
 |-- (CAST(1.1 AS STRING) < 1): boolean (nullable = false)

```
