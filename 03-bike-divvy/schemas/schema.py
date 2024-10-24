# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Creating the schema of the whole database

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## First I make the fact tables

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE  fact_trip  (
# MAGIC    id   varchar(255)  ,
# MAGIC    rider_id  int,
# MAGIC    start_station   varchar(200),
# MAGIC    end_station   varchar(200),
# MAGIC    start_date  VARCHAR(255),
# MAGIC    end_date VARCHAR(255),
# MAGIC    duration_trip  decimal,
# MAGIC    age  int
# MAGIC ) USING DELTA LOCATION '/delta/fact_trip';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Second fact table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE  fact_payment  (
# MAGIC    id  integer  ,
# MAGIC    payment_date  VARCHAR(255),
# MAGIC    amount DECIMAL,
# MAGIC    rider_id  integer
# MAGIC ) USING DELTA LOCATION '/delta/fact_payment';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now dimensions

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS dim_datetime(
# MAGIC   id VARCHAR(255),
# MAGIC   date TIMESTAMP,
# MAGIC   hour int,
# MAGIC   day int,
# MAGIC   day_of_week int,
# MAGIC   week int,
# MAGIC   month int,
# MAGIC   quarter int,
# MAGIC   year int
# MAGIC ) USING DELTA LOCATION '/delta/dim_datetime';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  dim_riders  (
# MAGIC    id  integer  ,
# MAGIC    firstname   varchar(255),
# MAGIC    lastname   varchar(255),
# MAGIC    address   varchar(255),
# MAGIC    birthday  date,
# MAGIC    account_start_date  date,
# MAGIC    account_end_date  date,
# MAGIC    is_member  BOOLEAN,
# MAGIC    avg_rides_month  decimal,
# MAGIC    avg_time_spent_month  decimal,
# MAGIC    rider_age_account_start  int
# MAGIC ) USING DELTA LOCATION '/delta/dim_riders';

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE  dim_stations  (
# MAGIC    id   varchar(255)  ,
# MAGIC    name   varchar(255),
# MAGIC    latitude  float,
# MAGIC    longitude  float
# MAGIC ) USING DELTA LOCATION '/delta/dim_stations';
# MAGIC      
