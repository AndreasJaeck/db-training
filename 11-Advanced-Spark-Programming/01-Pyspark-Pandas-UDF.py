# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/bikeSharing/data-001/'))

# COMMAND ----------

df = spark.read.load("/databricks-datasets/bikeSharing/data-001/", format="text", wholetext = True) 
df.cache()

# COMMAND ----------

cols = ["value"]
col_exprs = [F.split(F.col(x), ",").alias(x) for x in cols]

df_array_cols = df.select(*col_exprs)



# COMMAND ----------

display(df_array_cols.select(df_array_cols.value.getItem(0)))

# COMMAND ----------

df_line_split.show()

# COMMAND ----------

(df
    .withColumn("value", vector_to_array("vector")))
    .select(["word"] + [col("xs")[i] for i in range(3)]))

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf('array<string>')
def split_string(string):
  msg = string.str.split(",") 
  return msg

# COMMAND ----------

df.show()

# COMMAND ----------

df_day = spark.read.csv("/databricks-datasets/bikeSharing/data-001/day.csv", header=True) 

# COMMAND ----------

display(df_day)

# COMMAND ----------

df_hour = spark.read.csv("/databricks-datasets/bikeSharing/data-001/hour.csv", header=True) 

# COMMAND ----------

display(df_hour)

# COMMAND ----------


