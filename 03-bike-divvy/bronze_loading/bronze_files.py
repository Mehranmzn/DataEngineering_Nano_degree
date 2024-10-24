# Databricks notebook source
dbutils.fs.ls("/")

# COMMAND ----------

PATH = "/FileStore/Bronze data (Raw)/"


# COMMAND ----------

df_trisp = (spark.read.format("csv")
                .load(f"{PATH}trips.csv", header=False))


df_trisp = df_trisp.toDF('tripid', 'rideable_type', 'started_at', 'ended_at', 'start_station_id', 'end_station_id', 'rider_id')


# COMMAND ----------

display(df_trisp.limit(5))

# COMMAND ----------

(df_trisp.write.format("delta")
                .mode("overwrite")
                .save("/delta/src_trips"))

# COMMAND ----------

# SRC riders
df_riders = spark.read.format("csv").load(f"{PATH}riders.csv", header=False)
df_riders = df_riders.toDF('rider_id','first', 'last', 'address', 'birthday', 'account_start_date', 'account_end_date', 'is_member')

# write to delta file system 
(df_riders.write.format("delta")
                .mode("overwrite")
                .save("/delta/src_riders"))


# COMMAND ----------

# SRC payments
df_pay = spark.read.format("csv").load(f"{PATH}payments.csv", header=False)
df_pay = df_pay.toDF('payment_id','date', 'amount', 'rider_id')

# write to delta file system 
(df_pay.write.format("delta")
                .mode("overwrite")
                .save("/delta/src_payments"))


# COMMAND ----------

# SRC payments
df_station = spark.read.format("csv").load(f"{PATH}stations.csv", header=False)
df_station = df_station.toDF('station_id','name', 'latitude', 'longitude')

# write to delta file system 
(df_station.write.format("delta")
                .mode("overwrite")
                .save("/delta/src_stations"))

