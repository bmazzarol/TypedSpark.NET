# [Results](#tab/results)

|3  |2  |(3 / 2)|(3 / 2)|left literal|right literal|
|---|---|-------|-------|------------|-------------|
|3  |2  |1.5    |1.5    |1.5         |1.5          |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- 3: integer (nullable = false)
 |-- 2: integer (nullable = false)
 |-- (3 / 2): double (nullable = true)
 |-- (3 / 2): double (nullable = true)
 |-- left literal: double (nullable = true)
 |-- right literal: double (nullable = true)

```
