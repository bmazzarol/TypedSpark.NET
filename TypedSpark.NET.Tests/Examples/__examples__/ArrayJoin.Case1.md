# [Results](#tab/results)

|array_join(array(hello, world),  )|array_join(array(hello, world), ;)|
|----------------------------------|----------------------------------|
|hello world                       |hello;world                       |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_join(array(hello, world),  ): string (nullable = false)
 |-- array_join(array(hello, world), ;): string (nullable = false)

```
