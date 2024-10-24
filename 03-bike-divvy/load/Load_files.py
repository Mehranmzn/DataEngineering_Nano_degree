# Databricks notebook source

# load src_trips into databse tables
spark.sql("CREATE TABLE TRIPS \
     USING DELTA LOCATION '/delta/src_trips'"
)

# COMMAND ----------

#Verify the above:


display(spark.sql("select * from TRIPS limit 5"), limit = 5)

# COMMAND ----------

# load payments into table
spark.sql("CREATE TABLE PAYMENTS \
     USING DELTA LOCATION '/delta/src_payments'")
display(spark.sql("select * from PAYMENTS limit 5"), limit = 5)


# COMMAND ----------

# load riders into table
spark.sql("CREATE TABLE RIDERS \
     USING DELTA LOCATION '/delta/src_riders'"
)
display(spark.sql("select * from RIDERS limit 5"), limit = 5)


# COMMAND ----------


# load stations into table
spark.sql("CREATE TABLE STATIONS \
     USING DELTA LOCATION '/delta/src_stations'"
)
spark.sql("select * from STATIONS limit 5").show()
