# [Results](#tab/results)

|array(make_interval(0, 0, 0, 1, 0, 0, 0), make_interval(0, 0, 0, 2, 0, 0, 0), make_interval(0, 0, 0, 3, 0, 0, 0), make_interval(0, 0, 0, 4, 0, 0, 0))|
|-----------------------------------------------------------------------------------------------------------------------------------------------------|
|[1 days, 2 days, 3 days, 4 days]                                                                                                                     |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array(make_interval(0, 0, 0, 1, 0, 0, 0), make_interval(0, 0, 0, 2, 0, 0, 0), make_interval(0, 0, 0, 3, 0, 0, 0), make_interval(0, 0, 0, 4, 0, 0, 0)): array (nullable = false)
 |    |-- element: interval (containsNull = true)

```
