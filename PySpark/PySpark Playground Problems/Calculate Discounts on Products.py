# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#enter the file path here
file_path = "/datasets/products.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

filtered_df = df.withColumn('final_price', col('original_price') * (1 - col('discount_percentage') / 100))\
                .select('product_id', 'product_name', 'final_price')
    

# Display the final DataFrame using the display() function.
display(filtered_df)