# Databricks notebook source
rdd= spark.sparkContext.parallelize(
(
    ("apple", 50),
  ("banana", 40),
  ("orange", 60)
)
)
rdd.collect()

# COMMAND ----------

df= spark.createDataFrame(rdd).toDF("fruit", "price")
df.show()

# COMMAND ----------


