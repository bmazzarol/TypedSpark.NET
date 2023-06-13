# [Results](#tab/results)

|array_sort(array(5, 6, 1), lambdafunction(CASE WHEN (namedlambdavariable() < namedlambdavariable()) THEN -1 WHEN (namedlambdavariable() > namedlambdavariable()) THEN 1 ELSE 0 END, namedlambdavariable(), namedlambdavariable()))|
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|[1, 5, 6]                                                                                                                                                                                                                         |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_sort(array(5, 6, 1), lambdafunction(CASE WHEN (namedlambdavariable() < namedlambdavariable()) THEN -1 WHEN (namedlambdavariable() > namedlambdavariable()) THEN 1 ELSE 0 END, namedlambdavariable(), namedlambdavariable())): array (nullable = false)
 |    |-- element: integer (containsNull = false)

```
