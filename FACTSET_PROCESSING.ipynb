# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bp_demo_bronze.ngoo_silver2 order by last_date, last_time

# COMMAND ----------

spark.read.table("bp_demo_bronze.ngoo_silver").write.saveAsTable("bp_demo_bronze.ngoo_silver2")

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,StringType, DoubleType
from pyspark.sql.functions import expr, lit

def getBucket(date_time,interval,scale):
  if scale=="m":
      #HH = lpad(hour(date_time),2,"0")
      first_part = date_format(date_time,"yyyyMMddHH")
      second_part = lpad(minute(date_time)-(minute(date_time) % interval),2,"0")
      colx = to_timestamp(concat(first_part,second_part),'yyyyMMddHHmm') 


  elif scale=="H":
      first_part = date_format(date_time,"yyyyMMdd")
      second_part = lpad(hour(date_time)-(hour(date_time) % interval),2,"0")
      colx = to_timestamp(concat(first_part,second_part),'yyyyMMddHH')
  elif scale=="D":
      first_part = date_format(date_time,"yyyyMM")
      second_part = lpad(hour(date_time)-(dayofmonth(date_time) % interval),2,"0")
      colx = to_timestamp(concat(first_part,second_part),'yyyyMMdd')

      
  return colx

spark.udf.register("getBucket", getBucket, IntegerType())  

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
start_date = "197001010000"
df = spark.read.table("bp_demo_bronze.ngoo_silver")
df = df.withColumn("HHmm",substring(df.LAST_TIME,0,4))
df = df.withColumn("date_timex", concat("LAST_DATE","HHmm")) 

df = df.withColumn("date_time",to_timestamp(col("date_timex"),'yyyyMMddHHmm')) \
     .withColumn("base_date",lit(start_date)) \
     .withColumn("base_date",to_timestamp(col("base_date"),'yyyyMMddHHmm')) \
     .withColumn("minute",minute(col("date_time"))) \
     .withColumn("1_MIN_COHORT",getBucket("date_time",1,"m")) \
     .withColumn("3_MIN_COHORT",getBucket("date_time",3,"m")) \
     .withColumn("5_MIN_COHORT",getBucket("date_time",5,"m")) \
     .withColumn("1_HOUR_COHORT",getBucket("date_time",1,"H")) \
     .withColumn("2_HOUR_COHORT",getBucket("date_time",2,"H")) \
     .withColumn("4_HOUR_COHORT",getBucket("date_time",4,"H")) \
     .withColumn("1_DAY_COHORT",getBucket("date_time",1,"D")) 
#     .withColumn("difference",datediff("base_date","date_time"))


#df = df.filter("HHmm is null")
display(df)
df.write.saveAsTable("bp_demo_gold.allcohort")
                   

# COMMAND ----------

df = df.withColumn("HIGH", df["HIGH"].cast(DoubleType()))
df = df.withColumn("LOW", df["LOW"].cast(DoubleType()))
df = df.withColumn("OPEN", df["OPEN"].cast(DoubleType()))
df = df.orderBy("date_time")
#df = df.withColumn("CLOSE", df["CLOSE"].cast(DoubleType()))

# COMMAND ----------

def create_summary_table(df,columnName):
  dfx = df.groupBy(columnName) \
      .agg(max("HIGH").alias("high") \
           ,min("LOW").alias("low") \
           ,first("OPEN").alias("open") \
           ,last("LAST_PRICE").alias("close") \
           ,sum("CVOL").alias("vol"))\
            .withWatermark("timestamp", "10 minutes")\
          .orderBy(columnName)
  final_table_name = '{}.{}'.format("bp_demo_gold", columnName)
  #dfx.write.saveAsTable(final_table_name).mode("append").start()

# COMMAND ----------

create_summary_table(df,"1_MIN_COHORT")
create_summary_table(df,"5_MIN_COHORT")

# COMMAND ----------



# COMMAND ----------


