# Databricks notebook source
# MAGIC %md ## Quick Start Using Python
# MAGIC * Using a Databricks notebook to showcase DataFrame operations using Python
# MAGIC * Reference http://spark.apache.org/docs/latest/quick-start.html
# MAGIC * Get the new edition of Learning Spark from O’Reilly: https://databricks.com/p/ebook/learning-spark-from-oreilly

# COMMAND ----------

# import necessary dependencies
import pyspark.sql.functions as F
import time


# set a widget to keep everything tidy and clean
dbutils.widgets.text("Name", "Andreas")

# set shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 2)

# config the paths
# name = dbutils.widgets.get("Name")
# notebook = "quick-start"

# dbutils.fs.put(f"/FileStore/01-Spark-Programming/img/Catalyst.png", "Catalyst")

# COMMAND ----------

# MAGIC %md
# MAGIC ### What Are DataFrames?
# MAGIC 
# MAGIC In Spark, a DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs.

# COMMAND ----------

# read csv file into DataFrame
df = spark.read.csv("/databricks-datasets/samples/population-vs-price/data_geo.csv", header="true", inferSchema="true") 

# COMMAND ----------

# Show Number of Paritions the (R)esilent (D)istributed (D)ataset is created on.
df.rdd.getNumPartitions()

# COMMAND ----------

# We will increase the degree of parallelism by splitting the single partition into two partitions
df = df.repartition(2)

# COMMAND ----------

display(df)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md DataFrames allow ***TRANSFORMATIONS***, which return pointers to new DataFrames and ***ACTIONS***, which return values. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Order

# COMMAND ----------

# Order By Column "2014 rank"
df_ordered = df.orderBy("2014 rank")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Filter

# COMMAND ----------

# Filter Data Frame where 2014 Population estimate > 300000
df_filtered = df_ordered.filter(F.col("2014 Population estimate") > 300000)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Select

# COMMAND ----------

# Select Columns
df_select = df_filtered.select(["2014 rank", "City", "State", "State Code", "2014 Population estimate"])

# COMMAND ----------

df_select.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ![Spark Catalyst](files/01-Spark-Programming/img/Catalyst.png)

# COMMAND ----------

df_select.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Action

# COMMAND ----------

display(df_select)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Vectors and Loops with Python and PySpark
# MAGIC 
# MAGIC **Attention! Examples 1 to 3 are Anti-patterns to ETL PySpark Programming!**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 1. Native Python and Naive Program
# MAGIC 
# MAGIC Collect the data from the RDD and send it to the driver node. There it will be materialized in a python envionment. The for loop will apply our function line by line and append it to the pandas data frame. 
# MAGIC 
# MAGIC This leads to: 
# MAGIC 1. Loss of Parallelism
# MAGIC 2. I/O Overhead
# MAGIC 3. Potentially High Memory Consumption on the Drriver
# MAGIC 4. Slow Python Execution

# COMMAND ----------

start = time.time()

# Put DataFrame into Python Runtime and
pd = df_ordered.toPandas()

string_list = []
for i in pd["City"]: 
  
  # strip string from []
  i = re.sub(r"\[.*\]","", i) 
  
  # append items back to list
  string_list.append(i)
  
# overwrite col "City" with list
pd["City"] = string_list

end = time.time()

print(f"Execution Time: {end - start} seconds.")
pd.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 2. Native Python and Vectorized Program
# MAGIC 
# MAGIC Collect the data from the RDD and send it to the driver node. There it will be materialized in a Python envionment. We will apply our function in a vectorized way via the Pandas Data Frame. 
# MAGIC 
# MAGIC This leads to: 
# MAGIC 1. Loss of Parallelism
# MAGIC 2. I/O Overhead
# MAGIC 3. Potentially High Memory Consumption on the Driver
# MAGIC 4. Fast Python Execution

# COMMAND ----------

start = time.time()

# Put DataFrame into Python Runtime and
pd = df_ordered.toPandas()

# Replace content inside brackets with vecorized operation
pd['City'] = pd['City'].str.replace(r"\[.*\]","") 
  
end = time.time()

print(f"Execution Time: {end - start} seconds.")
pd.head()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 3. Native PySpark (Breaking Query Optimization)
# MAGIC 
# MAGIC Apply Python Function to the RDD. 
# MAGIC 
# MAGIC This leads to: 
# MAGIC 2. I/O Overhead (data has to be send to python env on the worker nodes)
# MAGIC 4. Slow PySpark Execution (no query optimization)

# COMMAND ----------

# python function which will be applied to col
def strip_string(str):
  
  # strip string from []
  str = re.sub(r"\[.*\]","", str)   
  return str


start = time.time()

# define spark udf 
convertUDF = udf(lambda z: strip_string(z),F.StringType())

# apply Spark UDF
df_regex = df_ordered.withColumn("City", convertUDF(F.col("City")))

end = time.time()

display(df_regex)
end = time.time()
print(f"Execution Time: {end - start} seconds.")

# COMMAND ----------

df_regex.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### 4. Native PySpark
# MAGIC 
# MAGIC Apply native PySpark Function to the high level Data Frame. 
# MAGIC 
# MAGIC This leads to: 
# MAGIC 1. No I/O Overhead 
# MAGIC 2. Full usage of Query Optimization and Lazy Evaluation

# COMMAND ----------

start = time.time()

df_regex = df_ordered.withColumn("City", F.regexp_replace(
                                   pattern=r"\[.*\]",
                                   replacement="",
                                   str=F.col("City")
                                 )
                                )

display(df_regex)
end = time.time()
print(f"Execution Time: {end - start} seconds.")


# COMMAND ----------

df_regex.explain(mode="formatted")

# COMMAND ----------


