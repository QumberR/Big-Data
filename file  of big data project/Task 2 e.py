# Databricks notebook source
                                                    #Mix SQL and DataFrame queries

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df=spark.read.format("csv").option("inferSchema", True).option("header",True).option("sep",",").load("/FileStore/tables/airlines-4.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

query = """
SELECT *
FROM df
WHERE Value IS NOT NULL;
"""
display(df)

# COMMAND ----------

query = """
SELECT Airline ID from df AVG(Value) as AVG

from airlines-4.csv
"""
display(df)


# COMMAND ----------

df

# COMMAND ----------

query = """
SELECT *
FROM  airlines-4.csv
ORDER BY Value DESC;
"""


# COMMAND ----------

query = """
SELECT DISTINCT Airlines, Alis, Country
FROM df;
"""
print(df.count())

# COMMAND ----------


