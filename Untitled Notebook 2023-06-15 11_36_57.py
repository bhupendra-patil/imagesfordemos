# Databricks notebook source
# MAGIC %md
# MAGIC ##This is a new file

# COMMAND ----------

import pyspark.sql.functions as F

# Read a CSV file
df = spark.read.csv('/path/to/my/file.csv', inferSchema=True, header=True)

# Show the first 10 rows
df.show(10)

# Aggregate data
agg_df = df.groupBy('group_by_column').agg(F.sum('numeric_column'))

# Show the result
agg_df.show()

# COMMAND ----------

databricks workspace export_dir /path/to/local/folder
