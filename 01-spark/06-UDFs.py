# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Pandas UDFs

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

@pandas_udf(IntegerType())
def slen(s: pd.Series) -> pd.Series:
    return s.str.len()

# COMMAND ----------

@pandas_udf("col1 string, col2 long")
def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
    s3['col2'] = s1 + s2.str.len()
    return s3

# Create a Spark DataFrame that has three columns including a struct column.
df = spark.createDataFrame(
    [[1, "a string", ("a nested string",)]],
    "long_col long, string_col string, struct_col struct<col1:string>")
df.printSchema()



df.select(func("long_col", "string_col", "struct_col")).printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Series to Series

# COMMAND ----------

@pandas_udf("string")
def to_upper(s: pd.Series) -> pd.Series:
    return s.str.upper()

df = spark.createDataFrame([("John Doe",)], ("name",))
df.select(to_upper("name")).show()

# COMMAND ----------

@pandas_udf("first string, last string")
def split_expand(s: pd.Series) -> pd.DataFrame:
    return s.str.split(expand=True)

df = spark.createDataFrame([("John Doe",)], ("name",))
df.select(split_expand("name")).show()

# COMMAND ----------


