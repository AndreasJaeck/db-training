# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType


pandas_udf_output = [
    (("","Smith","36636","M",3100),("Rose","","40288","M",4300))
  ]


structureSchema = StructType([
        StructField('schema_values_1', StructType([
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True),
             StructField('id', StringType(), True),
             StructField('gender', StringType(), True),
             StructField('salary', IntegerType(), True)
             ])),
        StructField('schema_values_2', StructType([
             StructField('fancyname', StringType(), True),
             StructField('lastname', StringType(), True),
             StructField('main_id', StringType(), True),
             StructField('state', StringType(), True),
             StructField('sum', IntegerType(), True)
             ]))
         ])

df2 = spark.createDataFrame(data=panas_udf_output,schema=structureSchema)
print("The parse schema for the UDF output:")
df2.printSchema()
print("We have one table in every column:")
df2.show(truncate=False)
print("This is the table 1 data frame:")
df_schema_1 = df2.select(col('schema_values_1.*'))
df_schema_1.show()

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType, DoubleType
import pyspark.sql.functions as F 
#from pyspark.sql import Row, Seq

# nested json with column orientation 
pandas_udf_output =  {
    "table_1": [
      {
      "part_id":1,
      "value_x":554.4,
      "value_y":222.4
      },
      {
      "part_id":2,
      "value_x":665.4,
      "value_y":540.4
      },
      {
      "part_id":3,
      "value_x":120.4,
      "value_y":954.4
      },
    ],
    "table_2": [
      {
       "part_id": 1,
       "result": "good",
       "state": "pass"
      },
      {
       "part_id": 4,
       "result": "bad",
       "state": "trash"
      },      
    ]
}


# specifying schema is optional in this case
# pyspark can also parse the schema directly from the json file - but it is more secure to enforce the schema
schema = StructType(
  fields=[
    StructField('table_1', ArrayType(
                StructType([
                StructField('part_id', IntegerType(), False),
                StructField('value_x', DoubleType(), True),
                StructField('value_y', DoubleType(), True)
            ]))),              
    StructField('table_2', ArrayType(
                StructType([
                StructField('part_id', IntegerType(), False),
                StructField('result', StringType(), True),
                StructField('state', StringType(), True)
            ])))
      ])


df = spark.read.json(sc.parallelize([pandas_udf_output]), schema=schema)
print("The parse json schema from the UDF output:")
df.printSchema()
print("We extract table 1 by selecting the according json key and explode the nested structure into columns")
df_table_1 = df.select("table_1", F.explode("table_1").alias("table_1_explode")).select("table_1_explode.*")
print("This is the table 1 data frame:")
display(df_table_1)

# COMMAND ----------


df_table_1 = df.select("table_1", F.explode("table_1").alias("table_1_explode")).select("table_1_explode.*")
