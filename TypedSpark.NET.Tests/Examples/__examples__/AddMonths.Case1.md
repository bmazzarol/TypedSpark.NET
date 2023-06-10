# [Results](#tab/results)

|DATE '2016-08-31'|add_months(DATE '2016-08-31', 1)|-1 |add_months(DATE '2016-08-31', -1)|
|-----------------|--------------------------------|---|---------------------------------|
|2016-08-31       |2016-09-30                      |-1 |2016-07-31                       |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- DATE '2016-08-31': date (nullable = false)
 |-- add_months(DATE '2016-08-31', 1): date (nullable = false)
 |-- -1: integer (nullable = false)
 |-- add_months(DATE '2016-08-31', -1): date (nullable = false)

```
