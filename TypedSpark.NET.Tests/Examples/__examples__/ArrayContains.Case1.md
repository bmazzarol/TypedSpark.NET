# [Results](#tab/results)

|array_contains(array(1, 2, 3), 2)|array_contains(array(1, 2, 3), 5)|array_contains(array(1, 2, 3), 3)|
|---------------------------------|---------------------------------|---------------------------------|
|true                             |false                            |true                             |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_contains(array(1, 2, 3), 2): boolean (nullable = false)
 |-- array_contains(array(1, 2, 3), 5): boolean (nullable = false)
 |-- array_contains(array(1, 2, 3), 3): boolean (nullable = false)

```
