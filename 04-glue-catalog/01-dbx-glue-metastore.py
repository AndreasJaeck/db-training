# Databricks notebook source
# MAGIC %md
# MAGIC # Use DBX with AWS Glue Catalog and Athena, EMR, Glue
# MAGIC 
# MAGIC #### Cluster: alex_sandbox_glue
# MAGIC 
# MAGIC Spark Config: 
# MAGIC 
# MAGIC - spark.databricks.delta.catalog.update.hiveSchema.enabled true
# MAGIC - spark.databricks.hive.metastore.glueCatalog.enabled true

# COMMAND ----------

# DBTITLE 1,Option 1: Read delta table directly from path (no hive metastore required) 
# read from path with delta jar
store_sales = spark \
  .read \
  .format('delta') \
  .load('s3://db-fe-datalake-west2/deltalake/tpcds100gb_s3/store_sales/') \
  .limit(50000)

# COMMAND ----------

# DBTITLE 1,Option 2: Config DBX Cluster to use Glue Catalog as external metastore
#Spark Cluster config (can be enforced with workspace config)

#spark.databricks.delta.catalog.update.hiveSchema.enabled true
#spark.databricks.hive.metastore.glueCatalog.enabled true

spark.sql("show databases").show()

# COMMAND ----------

# create db and check at Athena that db exists 
spark.sql("CREATE DATABASE IF NOT EXISTS pro7db")

# COMMAND ----------

write_format = 'delta'
save_path = 's3://db-fe-datalake-west2/deltalake/pro7db/'
table_name = 'pro7db.store_sales'

store_sales.write \
  .mode('overwrite')\
  .format(write_format) \
  .save(save_path)


spark.sql("DROP TABLE IF EXISTS table_name")

spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'")

# COMMAND ----------

# DBTITLE 1,Read from Glue Catalog with Delta
# MAGIC %md
# MAGIC 
# MAGIC Now you can read directly from Glue Catalog with every Spark context if: 
# MAGIC 
# MAGIC - Spark context is using Glue Catalog as external Hive metastore
# MAGIC - Delta is installed 

# COMMAND ----------

# DBTITLE 1,Use Spark Context to read Delta table through Glue Catalog
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM pro7db.store_sales
# MAGIC LIMIT 10 

# COMMAND ----------

# DBTITLE 1,Limitation: Spark Meta vs Athena Manifest 
# MAGIC %md
# MAGIC The table we just created can be already seen in the Glue Catalog and consumed by EMR and Glue with Delta. But the table doesn't appear in the Athena tab. 
# MAGIC That is because a manifest file is missing in the table directory. We have to create this file first:
# MAGIC 
# MAGIC Here is the recommended workflow for creating Delta tables, writing to them from Databricks, and querying them from Presto or Athena in such a configuration.
# MAGIC 
# MAGIC 1. Create the Delta table in Databricks using one of the following methods:
# MAGIC   - Write to a location and then create the table using that location (for example, dataframe.write.format("delta").save("<path-to-delta-table>") followed by CREATE TABLE ... USING delta ... LOCATION "<path-to-delta-table>").
# MAGIC   - Create the table directly while writing data (for example, dataframe.write.format("delta").saveAsTable(...) or CREATE TABLE ... AS SELECT ...).
# MAGIC Call this table delta_table_for_db.
# MAGIC 
# MAGIC 2. Generate the manifest using deltaTable = DeltaTable.forPath(<path-to-delta-table>) and deltaTable.generate("symlink_format_manifest").
# MAGIC 
# MAGIC 3. Create another table only for Presto or Athena using the manifest location. Call this table delta_table_for_presto. If the table is partitioned, call MSCK REPAIR TABLE delta_table_for_presto.
# MAGIC 
# MAGIC There should be two tables defined on the same data:
# MAGIC 
# MAGIC   - delta_table_for_db: Defined on the data location. All read and write operations in Databricks must use this table. Presto and Athena cannot use this table for any query.
# MAGIC   - delta_table_for_presto: Defined on the manifest location. All read operations from Presto or Athena must use this table. Databricks cannot use this table for any operations.
# MAGIC   
# MAGIC Remember that any schema changes in the Delta table data will be visible to operations in Databricks using delta_table_for_db. However, the schema changes will not be visible to Presto or Athena using delta_table_for_presto until the table is redefined with the new schema.

# COMMAND ----------

# DBTITLE 1,Step2: Generate manifest file from delta table
from delta.tables import *

# get delta table
delta_table = DeltaTable.forPath(sparkSession = spark, path = save_path)

# create manifest file from delta table
delta_table.generate("symlink_format_manifest")


# COMMAND ----------

# DBTITLE 1,Set manifest update to auto
# MAGIC %sql
# MAGIC ALTER TABLE pro7db.store_sales SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)

# COMMAND ----------

# DBTITLE 1,Show files in blob storage
dbutils.fs.ls('s3://db-fe-datalake-west2/deltalake/pro7db/')

# COMMAND ----------

# DBTITLE 1,Step3: Create new table for Athena with manifest file
# MAGIC %sql
# MAGIC CREATE EXTERNAL TABLE pro7db.stores_sales_athena (
# MAGIC   `ss_sold_date_sk` int, 
# MAGIC   `ss_sold_time_sk` int, 
# MAGIC   `ss_item_sk` int, 
# MAGIC   `ss_customer_sk` int, 
# MAGIC   `ss_cdemo_sk` int, 
# MAGIC   `ss_hdemo_sk` int, 
# MAGIC   `ss_addr_sk` int, 
# MAGIC   `ss_store_sk` int, 
# MAGIC   `ss_promo_sk` int, 
# MAGIC   `ss_ticket_number` bigint, 
# MAGIC   `ss_quantity` int, 
# MAGIC   `ss_wholesale_cost` decimal(7,2), 
# MAGIC   `ss_list_price` decimal(7,2), 
# MAGIC   `ss_sales_price` decimal(7,2), 
# MAGIC   `ss_ext_discount_amt` decimal(7,2), 
# MAGIC   `ss_ext_sales_price` decimal(7,2), 
# MAGIC   `ss_ext_wholesale_cost` decimal(7,2), 
# MAGIC   `ss_ext_list_price` decimal(7,2), 
# MAGIC   `ss_ext_tax` decimal(7,2), 
# MAGIC   `ss_coupon_amt` decimal(7,2), 
# MAGIC   `ss_net_paid` decimal(7,2), 
# MAGIC   `ss_net_paid_inc_tax` decimal(7,2), 
# MAGIC   `ss_net_profit` decimal(7,2)
# MAGIC )
# MAGIC ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
# MAGIC STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
# MAGIC OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
# MAGIC LOCATION 's3://db-fe-datalake-west2/deltalake/pro7db//_symlink_format_manifest/' 
