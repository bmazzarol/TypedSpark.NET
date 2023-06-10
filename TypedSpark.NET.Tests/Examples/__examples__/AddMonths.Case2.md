# [Results](#tab/results)

|TIMESTAMP '2016-08-31 12:00:00'|add_months(TIMESTAMP '2016-08-31 12:00:00', 1)|-1 |add_months(TIMESTAMP '2016-08-31 12:00:00', -1)|
|-------------------------------|----------------------------------------------|---|-----------------------------------------------|
|2016-08-31 12:00:00            |2016-09-30                                    |-1 |2016-07-31                                     |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- TIMESTAMP '2016-08-31 12:00:00': timestamp (nullable = false)
 |-- add_months(TIMESTAMP '2016-08-31 12:00:00', 1): date (nullable = false)
 |-- -1: integer (nullable = false)
 |-- add_months(TIMESTAMP '2016-08-31 12:00:00', -1): date (nullable = false)

```
