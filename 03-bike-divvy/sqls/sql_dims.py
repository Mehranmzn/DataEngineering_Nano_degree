# Databricks notebook source
# MAGIC %md
# MAGIC #Inserting data to Database

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dim_stations
# MAGIC (
# MAGIC   select station_id as id, 
# MAGIC   name,
# MAGIC   latitude,
# MAGIC   longitude
# MAGIC   from stations
# MAGIC )

# COMMAND ----------

# Compute count of rides and total duration per rider, grouped by month and year.
# This query aggregates the number of rides (num_rides) each rider took in a given month
# and calculates the average duration of rides in minutes.
# The calculation uses UNIX timestamps for accurate time difference computation.
# It creates a temporary view 'rider_stats' that can be queried directly in subsequent SQL operations.

riders_augmented = spark.sql(
    """
    WITH time_diff AS (
        SELECT 
            rider_id, 
            MONTH(started_at) as month, 
            YEAR(started_at) as year, 
            unix_timestamp(ended_at) - unix_timestamp(started_at) as duration_seconds
        FROM trips
    )
    SELECT 
        rider_id, 
        month, 
        year, 
        COUNT(*) as num_rides, 
        AVG(duration_seconds) / 60 as minutes_spent
    FROM time_diff
    GROUP BY rider_id, month, year;
    """
)

# Create a temporary view for easy access to the computed rider statistics
riders_augmented.createOrReplaceTempView("riders_augmented")


# COMMAND ----------

# Compute average ride statistics for each rider over all available months.
# This query aggregates the average number of rides (avg_rides_month) and 
# the average time spent riding (avg_time_spent_month) per month for each rider.
# It calculates these metrics using the precomputed 'rider_stats' temporary view, 
# which contains the count of rides and total duration per rider per month.
# The resulting DataFrame is then registered as a temporary view 'rider_stats_month' 
# for easier access in subsequent queries.

riders_aug_datetime = spark.sql(
    """
    SELECT 
        rider_id, 
        AVG(num_rides) as avg_rides_month,
        AVG(minutes_spent) as avg_time_spent_month
    FROM riders_augmented
    GROUP BY rider_id
    """
)

# Display the first few records for quick inspection of the result
display(riders_aug_datetime.limit(5))
# Create a temporary view for the computed monthly average statistics per rider
riders_aug_datetime.createOrReplaceTempView("riders_aug_datetime")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert rider information into the 'dim_rider' dimension table.
# MAGIC -- This query selects rider details such as ID, name, address, birthday, account dates,
# MAGIC -- membership status, and calculated metrics for average rides per month and average time
# MAGIC -- spent per month from the 'riders' table. It joins this data with 'rider_stats_month'
# MAGIC -- to enrich the rider data with average statistics.
# MAGIC -- Additionally, it computes 'rider_age_account_start', which represents the age of each 
# MAGIC -- rider when they started their account, calculated by the difference in years between 
# MAGIC -- the 'account_start_date' and the 'birthday'.
# MAGIC -- The resulting data is then inserted into the 'dim_rider' table, which serves as a 
# MAGIC -- dimension table for analysis and reporting purposes.
# MAGIC
# MAGIC INSERT INTO dim_riders
# MAGIC SELECT 
# MAGIC     riders.rider_id AS id,
# MAGIC     riders.first AS firstname,
# MAGIC     riders.last AS lastname,
# MAGIC     riders.address,
# MAGIC     riders.birthday,
# MAGIC     riders.account_start_date,
# MAGIC     riders.account_end_date,
# MAGIC     riders.is_member,
# MAGIC     riders_aug_datetime.avg_rides_month,
# MAGIC     riders_aug_datetime.avg_time_spent_month,
# MAGIC     FLOOR(months_between(riders.account_start_date, riders.birthday) / 12) AS rider_age_account_start
# MAGIC FROM riders 
# MAGIC JOIN riders_aug_datetime 
# MAGIC     ON riders.rider_id = riders_aug_datetime.rider_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM riders LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## For the datetime dim table

# COMMAND ----------

from pyspark.sql.functions import explode, sequence, to_timestamp, expr, lit

# Define the start and end date as constants
beginDate = '2000-01-01 00:00:00'
endDate = '2030-12-31 00:00:00'

# Generate the sequence of dates and create a DataFrame
date_df = (
    spark.range(1)  # Dummy DataFrame with a single row to enable sequence creation
    .select(
        explode(
            sequence(
                to_timestamp(lit(beginDate)),  # Use lit() to treat beginDate as a literal
                to_timestamp(lit(endDate)),    # Use lit() to treat endDate as a literal
                expr("INTERVAL 1 HOUR")        # Correctly specify the interval as INTERVAL
            )
        ).alias("calendarDate")
    )
)

# Create a temporary view for the generated dates
date_df.createOrReplaceTempView("dates")



# COMMAND ----------

# MAGIC %sql 
# MAGIC INSERT INTO dim_datetime 
# MAGIC  (
# MAGIC   select sha2(encode(calendarDate, "UTF-8"), 224) as id,
# MAGIC   calendarDate as date,
# MAGIC   hour(calendarDate) as hour,
# MAGIC   day(calendarDate) as day,
# MAGIC   dayofweek(calendarDate) as day_of_week,
# MAGIC   weekofyear(calendarDate) as week,
# MAGIC   month(calendarDate) as month,
# MAGIC   quarter(calendarDate) as quarter,
# MAGIC   year(calendarDate) as year
# MAGIC   from dates
# MAGIC  )
