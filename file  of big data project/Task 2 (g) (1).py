# Databricks notebook source
                                                            Join two DataFrames

# COMMAND ----------

create Employee Dataframe

# COMMAND ----------

employee_data =[(10,"Qumber Raza","1999-09-09","100",2400),
               (20,"Ahmad Ali","1949-06-09","200",2000),
               (30," Zohib Ahmad","1849-11-05","600",6000),
               (40,"Shahid ","1909-05-02","700",2000),
               (50,"Zair Raza","2009-02-08","300",6000),
               (60,"Qumber Raza","1950-06-09","500",6200),
               (70,"Qumber Raza","1993-05-09","900",4200),
               (70,"Qumber Raza","1994-03-09","400",2600),
               (90,"Qumber Raza","1992-04-09","500",2500),
               (100,"Qumber Raza","1939-04-09","700",4200)]
employee_schema =["employee_id","Name","doj","employee_dept_id","salary"]

empDF= spark.createDataFrame(data=employee_data, schema=employee_schema)

display(empDF)

# COMMAND ----------



# COMMAND ----------

Create another Department Dataframe 

# COMMAND ----------

department_data = [("HR", 100),
                   ("Supply", 200),
                  ("Sales", 300),
                  ("Stock", 400)]

department_schema = ["dept_name","dept_id"]
departmentDF = spark.createDataFrame(data=department_data, schema = department_schema)

# COMMAND ----------

display(empDF)

display(departmentDF)

# COMMAND ----------

df_join = empDF.join(departmentDF,empDF.employee_dept_id ==departmentDF.dept_id,"inner")

display(df_join)

# COMMAND ----------


