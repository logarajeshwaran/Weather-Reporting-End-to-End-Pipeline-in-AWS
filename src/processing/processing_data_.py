from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


session  = SparkSession.builder.appName("Weather ETL Pipeline").config("spark.jars", "s3://weather-data-input-20250101/input_files/redshift-jdbc42-no-awssdk-1.2.55.1083.jar").getOrCreate()

s3_input_path_name = 's3://weather-data-input-20250101/input_files/dataweather_data.json'
redshift_url = 'jdbc:redshift://dataengineering.180294190881.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
redshift_table = 'weather_readings'
redshift_user = 'admin'
redshift_passwod= 'WIOHDmwvio026+&'

weather_schema = StructType([
    StructField("city", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", IntegerType()),
    StructField("wind_speed", DoubleType()),  
    StructField("weather_condition", StringType()),
    StructField("description", StringType()),
    StructField("timestamp", TimestampType())
])


raw_df = session.read.json(s3_input_path_name,schema=weather_schema)


cleaned_df = raw_df.withColumn(
    "description", 
    regexp_replace(col("description"), "[^a-zA-Z0-9\\s]", "")
).withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
).withColumn(
    "temperature_fahrenheit",
    col("temperature") * 9/5 + 32 
)

cleaned_df.write.format("com.databricks.spark.redshift").option("url",redshift_url).option("dbtable",redshift_table).option("user",redshift_user).option("password",redshift_passwod).option("driver").mode("append").save()

session.stop()