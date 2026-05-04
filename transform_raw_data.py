from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

from dotenv import load_dotenv
load_dotenv();

spark = SparkSession.builder \
    .appName("SyntheticDataPipeline") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['S3_ACCESS_KEY']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['S3_SECRET_KEY']) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.ttl", "300000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000") \
    .getOrCreate()

# read raw data
trades_df = spark.read.parquet("s3a://my-data-lake-raw-026090555251-us-east-1-an/trades/")
print("Raw Data")
trades_df.show(5)

# clean and enrich
cleaned_df = trades_df.withColumn("amount", F.col("quantity") * F.col("price")) \
                      .filter(F.col("price") > 0) \
                      .withColumn("processed_time", F.current_timestamp())
print("Cleaned Data")
cleaned_df.show(5)

# aggregate with windows
windowSpec = Window.partitionBy("account_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
enriched_df = cleaned_df.withColumn("running_total", F.sum("amount").over(windowSpec))
print("Enriched Data")
enriched_df.show(5)

# write enriched results
enriched_df.write.mode("overwrite") \
           .partitionBy("date") \
           .parquet("s3a://my-data-lake-processed-026090555251-us-east-1-an/trades_analytical/")