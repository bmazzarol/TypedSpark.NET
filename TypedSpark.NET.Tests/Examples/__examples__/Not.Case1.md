# [Results](#tab/results)

|(NOT true)|(NOT true)|
|----------|----------|
|false     |false     |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (NOT true): boolean (nullable = false)
 |-- (NOT true): boolean (nullable = false)

```

# [Plan](#tab/plan)

```shell
== Parsed Logical Plan ==
'Project [unresolvedalias(NOT true, Some(org.apache.spark.sql.Column$$Lambda$1254/325807458@45bcd0d2)), unresolvedalias(NOT true, Some(org.apache.spark.sql.Column$$Lambda$1254/325807458@45bcd0d2))]
+- LocalRelation

== Analyzed Logical Plan ==
(NOT true): boolean, (NOT true): boolean
Project [NOT true AS (NOT true)#1, NOT true AS (NOT true)#2]
+- LocalRelation

== Optimized Logical Plan ==
LocalRelation [(NOT true)#1, (NOT true)#2]

== Physical Plan ==
LocalTableScan [(NOT true)#1, (NOT true)#2]

```
