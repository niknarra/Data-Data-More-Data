# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#enter the file path here
file_path = "/datasets/customers_raw.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

filtered_df = df.filter(
                        (col('customer_id').isNotNull()) &
                        (col('email').isNotNull())
                        )

# Display the final DataFrame using the display() function.
display(filtered_df)