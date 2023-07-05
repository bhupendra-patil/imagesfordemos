# Databricks notebook source
#3test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table hive_metastore.bp_demo_featurestore.symbol_technical_features_shallow
# MAGIC shallow clone hive_metastore.bp_demo_featurestore.symbol_technical_features

# COMMAND ----------

# MAGIC %sql
# MAGIC create table hive_metastore.bp_demo_featurestore.symbol_technical_features_deep
# MAGIC deep clone hive_metastore.bp_demo_featurestore.symbol_technical_features

# COMMAND ----------



# COMMAND ----------

# MAGIC %test

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %wqeqw

# COMMAND ----------

# MAGIC %laslsa
