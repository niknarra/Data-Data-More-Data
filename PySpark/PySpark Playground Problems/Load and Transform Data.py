# Initialize Spark session
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()

#enter the file path here
file_path = "/datasets/customers.csv"

#read the file
df = spark.read.format('csv').option('header', 'true').load(file_path)

filtered_df = df.select('customer_id', 'name', 'purchase_amount')\
                .filter( 
                        (col('purchase_amount') > 100) & 
                        (col('age') >= 30)
                       )

# Display the final DataFrame using the display() function.
display(filtered_df)