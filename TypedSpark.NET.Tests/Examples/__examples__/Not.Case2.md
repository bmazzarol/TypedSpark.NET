# [Results](#tab/results)

|(NOT false)|(NOT false)|
|-----------|-----------|
|true       |true       |

_(top = 20)_

# [Schema](#tab/schema)

```shell
root
 |-- (NOT false): boolean (nullable = false)
 |-- (NOT false): boolean (nullable = false)

```

# [Plan](#tab/plan)

```shell
== Parsed Logical Plan ==
'Project [unresolvedalias(NOT false, Some(org.apache.spark.sql.Column$$Lambda$1254/325807458@45bcd0d2)), unresolvedalias(NOT false, Some(org.apache.spark.sql.Column$$Lambda$1254/325807458@45bcd0d2))]
+- LocalRelation

== Analyzed Logical Plan ==
(NOT false): boolean, (NOT false): boolean
Project [NOT false AS (NOT false)#1, NOT false AS (NOT false)#2]
+- LocalRelation

== Optimized Logical Plan ==
LocalRelation [(NOT false)#1, (NOT false)#2]

== Physical Plan ==
LocalTableScan [(NOT false)#1, (NOT false)#2]

```
