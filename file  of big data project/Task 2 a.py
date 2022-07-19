# Databricks notebook source
import pandas as pd


# COMMAND ----------

df = pd.DataFrame( 
     [[1, 2, 3], 
     [4, 6, 8],
     [10, 11, 12]],
     index=[1, 2, 3], 
     columns=['a', 'b', 'c'])

# COMMAND ----------

df

# COMMAND ----------

#Data frame with boolean idex     Dictonary of list
dict = {'name':["Ali", "Ahsan", "Ahmad", "Akram"], 
        'degree': ["BSCS", "BSIT", "BSLL", "BSAI"], 
        'score':[1, 2, 3, 4]} 
   
df = pd.DataFrame(dict, index = [True, False, True, False]) 
   print(df) 

# COMMAND ----------

df.drop('name', axis=1)   #drop name through axis

# COMMAND ----------

df['New Column'] = 0                  #creating a new column in dataframe
df

# COMMAND ----------

df.columns = ['Column1', 'Column 2', 'Column 3',' Column 4']      #rename the column
df

# COMMAND ----------

 df.columns = ['Name', 'Degree', 'Score','Zero ']      #rename the column
df

# COMMAND ----------

df.rank()           # rank of particular activity

# COMMAND ----------

df.shape           #shampe of column

# COMMAND ----------

df.index           # retrive index of the column

# COMMAND ----------

df.info()         #Get information on DataFrame


# COMMAND ----------

df.count()            #Retrieve number of non-NA values

# COMMAND ----------

df.sum()     #sum  
             

# COMMAND ----------

df.cumsum()           # cumulative sum


# COMMAND ----------

df.mul(2)    #multiply

# COMMAND ----------

df.min()        #minimum function

# COMMAND ----------

df.max()     #max function

# COMMAND ----------

df.mean()       #mean funnction
  

# COMMAND ----------

df.median()


# COMMAND ----------

df.describe()          #Describe a summary of data statistics

# COMMAND ----------


