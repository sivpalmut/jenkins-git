#!/usr/bin/env python3
"""
PySpark Streaming Application: Read from S3 and Write to Iceberg Table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session(app_name="S3ToIcebergStreaming"):
    """
    Create and configure Spark session with Iceberg support
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3://your-warehouse-bucket/iceberg-warehouse") \
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", "https://s3.amazonaws.com") \
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
        .config("spark.sql.catalog.iceberg.s3.access-key", "your-access-key") \
        .config("spark.sql.catalog.iceberg.s3.secret-key", "your-secret-key") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "s3://your-checkpoint-bucket/checkpoints") \
        .getOrCreate()
    
    return spark

def define_schema():
    """
    Define the schema for the streaming data
    Modify this based on your actual data structure
    """
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("category", StringType(), True),
        StructField("source", StringType(), True)
    ])
    return schema

def create_iceberg_table(spark, table_name, schema):
    """
    Create Iceberg table if it doesn't exist
    """
    try:
        # Create table with Iceberg format
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS iceberg.{table_name} (
            id STRING,
            timestamp TIMESTAMP,
            value DOUBLE,
            category STRING,
            source STRING
        ) USING iceberg
        PARTITIONED BY (category, days(timestamp))
        """
        
        spark.sql(create_table_sql)
        logger.info(f"Successfully created/verified Iceberg table: {table_name}")
        
    except Exception as e:
        logger.error(f"Error creating Iceberg table: {str(e)}")
        raise

def process_streaming_data(df, epoch_id):
    """
    Process each batch of streaming data
    """
    if df.count() > 0:
        logger.info(f"Processing batch {epoch_id} with {df.count()} records")
        
        # Add processing timestamp
        processed_df = df.withColumn("processed_at", current_timestamp())
        
        # Write to Iceberg table
        processed_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable("iceberg.streaming_data")
        
        logger.info(f"Successfully wrote {df.count()} records to Iceberg table")
    else:
        logger.info(f"Batch {epoch_id} is empty, skipping")

def main():
    """
    Main function to run the streaming application
    """
    try:
        # Create Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Define schema
        schema = define_schema()
        
        # Create Iceberg table
        create_iceberg_table(spark, "streaming_data", schema)
        
        # Read streaming data from S3
        # Option 1: Read from S3 with file format detection
        streaming_df = spark.readStream \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .option("maxFilesPerTrigger", 10) \
            .option("cleanSource", "delete") \
            .load("s3://your-input-bucket/streaming-data/*")
        
        # Option 2: Read JSON files from S3
        # streaming_df = spark.readStream \
        #     .format("json") \
        #     .schema(schema) \
        #     .option("maxFilesPerTrigger", 10) \
        #     .option("cleanSource", "delete") \
        #     .load("s3://your-input-bucket/streaming-data/*.json")
        
        # Option 3: Read Parquet files from S3
        # streaming_df = spark.readStream \
        #     .format("parquet") \
        #     .option("maxFilesPerTrigger", 10) \
        #     .option("cleanSource", "delete") \
        #     .load("s3://your-input-bucket/streaming-data/*.parquet")
        
        logger.info("Streaming query configured successfully")
        
        # Start the streaming query
        query = streaming_df.writeStream \
            .foreachBatch(process_streaming_data) \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        logger.info("Streaming query started successfully")
        
        # Wait for the query to terminate
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in streaming application: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()