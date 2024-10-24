"""
Author : Olubukunola Akinsola <oo.akinsola@gmail.com>
Date   : 2024-10-24
Purpose: Sora Union DE Assessment - Task 3
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("New York Bike Data Analysis") \
    .getOrCreate()

# Set legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Define schema for the dataset
schema = StructType([
    StructField("trip_id", IntegerType(), False),
    StructField("starttime", StringType(), False),
    StructField("stoptime", StringType(), True),
    StructField("bikeid", IntegerType(), False),
    StructField("trip_duration", IntegerType(), False),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", StringType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("usertype", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("birthyear", IntegerType(), True)
])

# Function to load data
def load_data(file_path):
    loaded_data =  spark.read.csv(file_path, header=True, schema=schema)
    return loaded_data

# Function to parse date columns
def parse_dates(bike_data):

    date_formats = [
        "M/d/yyyy HH:mm:ss", "MM/d/yyyy HH:mm:ss", "M/dd/yyyy HH:mm:ss",
        "MM/dd/yyyy HH:mm:ss", "M/d/yyyy H:mm:ss", "MM/d/yyyy H:mm:ss",
        "M/dd/yyyy H:mm:ss", "MM/dd/yyyy H:mm:ss",
        "M/d/yyyy HH:mm", "MM/d/yyyy HH:mm", "M/dd/yyyy HH:mm",
        "MM/dd/yyyy HH:mm", "M/d/yyyy H:mm", "MM/d/yyyy H:mm",
        "M/dd/yyyy H:mm", "MM/dd/yyyy H:mm"
    ]

    # Create a conditional expression to parse the date formats
    date_expr = F.coalesce(
        *[F.to_timestamp(F.col("starttime"), fmt) for fmt in date_formats]
    )

    # Apply to both starttime and stoptime
    bike_data = bike_data.withColumn("starttime", date_expr) \
                         .withColumn("stoptime", date_expr)



    bike_data = bike_data.withColumn("date", to_date(col("starttime")))
    return bike_data



# Function to prepare data
def prepare_data(bike_data,no_of_partitions):
    bike_data = bike_data.repartition(no_of_partitions, "date", "usertype")
    return bike_data

# Function to aggregate data
def aggregate_data(bike_data):
    aggregated_data=bike_data.groupBy("date", "usertype").agg(
        F.count("trip_id").alias("total_rides"),
        F.round(mean("trip_duration"), 2).alias("avg_trip_duration") 
    )

    return aggregated_data



# Main code execution
file_path = "C:/Users/PC/Downloads/for_sora/new_york_bike_trips.csv"
no_of_partition = 100
bike_data = load_data(file_path)
bike_data = parse_dates(bike_data)
bike_data = prepare_data(bike_data, no_of_partition)
transformed_data = aggregate_data(bike_data)

# Display the transformation results
transformed_data.show()

# Stop Spark session
spark.stop()
