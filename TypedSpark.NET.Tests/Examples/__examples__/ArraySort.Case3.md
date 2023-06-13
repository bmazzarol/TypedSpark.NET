# [Results](#tab/results)

|array_sort(array(b, d, NULL, c, a), lambdafunction((IF(((namedlambdavariable() IS NULL) AND (namedlambdavariable() IS NULL)), 0, (IF((namedlambdavariable() IS NULL), 1, (IF((namedlambdavariable() IS NULL), -1, (IF((namedlambdavariable() < namedlambdavariable()), -1, (IF((namedlambdavariable() > namedlambdavariable()), 1, 0)))))))))), namedlambdavariable(), namedlambdavariable()))|
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|[a, b, c, d, null]                                                                                                                                                                                                                                                                                                                                                                            |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- array_sort(array(b, d, NULL, c, a), lambdafunction((IF(((namedlambdavariable() IS NULL) AND (namedlambdavariable() IS NULL)), 0, (IF((namedlambdavariable() IS NULL), 1, (IF((namedlambdavariable() IS NULL), -1, (IF((namedlambdavariable() < namedlambdavariable()), -1, (IF((namedlambdavariable() > namedlambdavariable()), 1, 0)))))))))), namedlambdavariable(), namedlambdavariable())): array (nullable = false)
 |    |-- element: string (containsNull = true)

```
