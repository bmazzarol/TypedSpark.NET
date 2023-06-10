# [Results](#tab/results)

|1  |1  |(1 <=> CAST(1 AS INT))|
|---|---|----------------------|
|1  |1  |true                  |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- 1: integer (nullable = false)
 |-- 1: string (nullable = false)
 |-- (1 <=> CAST(1 AS INT)): boolean (nullable = false)

```
