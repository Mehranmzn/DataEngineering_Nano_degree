# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM fact_trip;

# COMMAND ----------

# Replace 'fact_trip' with the name of your Delta table
delta_table = spark.table("fact_trip")
delta_schema = delta_table.schema

# Display the Delta table schema
for field in delta_schema:
    print(f"Column: {field.name}, DataType: {field.dataType}, Nullable: {field.nullable}")


# COMMAND ----------

# MAGIC %md
# MAGIC So we have to insert/ingest data from the src delta files we got, into the already made empty tabels in the databse.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM stations limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE fact_trip ADD COLUMNS (age_of_rider INT);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert into 'fact_trip' while calculating the age of the rider
# MAGIC INSERT INTO fact_trip
# MAGIC SELECT 
# MAGIC   t.tripid AS id,
# MAGIC   t.rider_id,
# MAGIC   t.start_station_id,
# MAGIC   t.end_station_id,
# MAGIC   sha2(encode(t.started_at, 'UTF-8'), 224) AS start_date,
# MAGIC   sha2(encode(t.ended_at, 'UTF-8'), 224) AS end_date,
# MAGIC   try_cast(t.rideable_type AS DECIMAL(10, 0)) AS rideable_type,  -- Use try_cast to handle invalid values
# MAGIC   (unix_timestamp(t.ended_at) - unix_timestamp(t.started_at)) / 60 AS duration,
# MAGIC   FLOOR(months_between(t.started_at, r.birthday) / 12) AS age_of_rider
# MAGIC FROM trips t
# MAGIC JOIN riders r ON t.rider_id = r.rider_id;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO fact_payment
# MAGIC (
# MAGIC   SELECT DISTINCT payments.payment_id AS id,
# MAGIC   sha2(encode(date, "UTF-8"), 224) AS payment_date_id,
# MAGIC   payments.amount,
# MAGIC   riders.rider_id
# MAGIC   FROM payments
# MAGIC   JOIN riders ON (payments.rider_id = riders.rider_id)
# MAGIC   JOIN trips ON (riders.rider_id = trips.rider_id)
# MAGIC )
# MAGIC      

# COMMAND ----------

# MAGIC
# MAGIC %sql 
# MAGIC select * from fact_payment limit 10
