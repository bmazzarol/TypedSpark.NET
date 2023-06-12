# [Results](#tab/results)

|array_contains(array(a, b, c), a)|array_contains(array(a, b, c), d)|
|---------------------------------|---------------------------------|
|true                             |false                            |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_contains(array(a, b, c), a): boolean (nullable = false)
 |-- array_contains(array(a, b, c), d): boolean (nullable = false)

```
