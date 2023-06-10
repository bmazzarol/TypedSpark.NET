# [Results](#tab/results)

|(NOT (1 = 2))|(NOT (CAST(1 AS STRING) = 2))|(NOT (1 = 2))|left literal|right literal|
|-------------|-----------------------------|-------------|------------|-------------|
|true         |true                         |true         |true        |true         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (NOT (1 = 2)): boolean (nullable = false)
 |-- (NOT (CAST(1 AS STRING) = 2)): boolean (nullable = false)
 |-- (NOT (1 = 2)): boolean (nullable = false)
 |-- left literal: boolean (nullable = false)
 |-- right literal: boolean (nullable = false)

```
