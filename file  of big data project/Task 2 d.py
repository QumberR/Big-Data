# Databricks notebook source
                              #        Use the Spark CSV library from Spark Packages to read structured files

# COMMAND ----------

                                                                 # Read csv files

df=spark.read.format("csv").option("inferSchema", True).option("header",True).option("sep",",").load("/FileStore/tables/airlines-3.csv")
display(df)

# COMMAND ----------

                                                                       # Read 2 Csv files
    
df_1=spark.read.format("csv").option("inferSchema", True).option("header",True).option("sep",",").load(["/FileStore/tables/airlines-3.csv","/FileStore/tables/Fish-3.csv"])


# COMMAND ----------

display(df_1)

print(df_1.count())


# COMMAND ----------

                                                        # READ the file from a folder
df2=spark.read.format("csv").option("inferSchema", True).option("header",True).option("sep",",").load("/FileStore/tables/")

# COMMAND ----------

display(df2)
print(df2.count())

# COMMAND ----------

df.printSchema()

# COMMAND ----------


