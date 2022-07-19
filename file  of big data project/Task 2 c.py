# Databricks notebook source
import pandas as pd
import numpy as np

# COMMAND ----------

                                   Correctly size the number of partitions in a DF, including the size of each partition 

# COMMAND ----------

df=spark.read.format("csv").option("inferSchema", True).option("header",True).option("sep",",").load("/FileStore/tables/airlines-3.csv")
display(df)

# COMMAND ----------

print(df.count())       #Number of records in dataframe

# COMMAND ----------

#Defualt partition count
print(df.rdd.getNumPartitions())

# COMMAND ----------

#Number of records per partition

from pyspark.sql.functions import spark_partition_id
df.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()

# COMMAND ----------

#Repetation the dataframe to 5

df_5=df.select(df.Name,df.Alias,df.IATA,df.ICAO,df.Callsign,df.Country).repartition(5)

# COMMAND ----------

#check no of partition
print(df_5.rdd.getNumPartitions())

# COMMAND ----------

#GET NUMBER OF RECORD PER PARTITION
df_5.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()
