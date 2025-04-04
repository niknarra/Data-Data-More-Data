from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import *
from datetime import date

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RemoveDuplicates") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("user_name", StringType(), True),
    StructField("created_date", DateType(), True),
    StructField("email", StringType(), True)
])

# Sample data
data = [
    (1, "Alice", date(2023, 5, 10), "alice@example.com"),
    (1, "Alice", date(2023, 6, 15), "alice_new@example.com"),
    (2, "Bob", date(2023, 7, 1), "bob@example.com"),
    (3, "Charlie", date(2023, 5, 20), "charlie@example.com"),
    (3, "Charlie", date(2023, 6, 25), "charlie_updated@example.com"),
    (4, "David", date(2023, 8, 5), "david@example.com")
]

# Create DataFrame
user_df = spark.createDataFrame(data, schema)

user_df = user_df.sort(col('created_date').desc()).dropDuplicates(subset=['user_id'])

display(user_df)
