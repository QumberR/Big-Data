# Databricks notebook source
                                            Write a user-defined function (UDF)

# COMMAND ----------

#Create simple dataframe
employee_data =[(10,"Qumber Raza","100",2400),
               (20,"Ahmad Ali","200",2000),
               (30," Zohib Ahmad","600",6000),
               (40,"Shahid ","700",2000),
               (50,"Zair Raza","300",6000),
               (60,"Qumber Raza","500",6200),
               (70,"Qumber Raza","900",4200),
               (70,"Qumber Raza","400",2600),
               (90,"Qumber Raza","500",2500),
               (100,"Qumber Raza","700",4200)]
employee_schema =["employee_id","Name","employee_dept_id","salary"]

empDF= spark.createDataFrame(data=employee_data, schema=employee_schema)

display(empDF)

# COMMAND ----------

Define UDF to Rename Column

# COMMAND ----------

import pyspark.sql.functions as f
def rename_columns(rename_df):
    for column in rename_df.columns:
        new_column = "Col_" +column
        rename_df = rename_df.withColumnRenamed(column, new_column)
        return rename_df

# COMMAND ----------

Execute DF

# COMMAND ----------

renamed_df = rename_columns(empDF)
display(renamed_df)

# COMMAND ----------

Execute UDF

# COMMAND ----------

renamed_df = rename_columns(empDF)
display(renamed_df)

# COMMAND ----------

UDF to convert name into upper case

# COMMAND ----------

from pyspark.sql.functions import upper, col
def upperCase_col(df):
    empDF_upper =df.withColumn('name_upper', upper(df.Name))
    return empDF_upper

# COMMAND ----------

Up_Case_DF =upperCase_col(empDF)
display(Up_Case_DF)

# COMMAND ----------


