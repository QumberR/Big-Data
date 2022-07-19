# Databricks notebook source
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

# COMMAND ----------

data

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('/FileStore/tables/Fish.csv').getOrCreate()

# COMMAND ----------

txt_file = spark.read.text("/FileStore/tables/example.txt")

# COMMAND ----------

json_file = spark.read.json("/FileStore/tables/sample.json", multiLine=True)

# COMMAND ----------


