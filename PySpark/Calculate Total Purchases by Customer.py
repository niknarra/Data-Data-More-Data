# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#enter the file path here
file_path = "/datasets/customer_purchases.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

filtered_df = df.groupBy('customer_id')\
                .agg(sum('purchase_amount').cast('int').alias('total_purchase'))\
                .orderBy('customer_id')

# Display the final DataFrame using the display() function.
display(filtered_df)