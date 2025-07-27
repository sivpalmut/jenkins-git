#!/usr/bin/env python3
"""
PySpark Streaming Application: S3 to Iceberg
Reads streaming data from S3 and writes to Iceberg table
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_session():
    """
    Create and configure Spark session with necessary dependencies for S3 and Iceberg
    """
    # Iceberg catalog configuration
    iceberg_packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ]
    
    spark = SparkSession.builder \
        .appName("S3-to-Iceberg-Streaming") \
        .config("spark.jars.packages", ",".join(iceberg_packages)) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://your-iceberg-warehouse/") \
        .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", "s3.amazonaws.com")) \
        .config("spark.sql.catalog.iceberg.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("AWS_S3_ENDPOINT", "s3.amazonaws.com")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_schema():
    """
    Define the schema for the streaming data
    Modify this according to your data structure
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True)
    ])

def create_iceberg_table(spark, table_name, schema):
    """
    Create Iceberg table if it doesn't exist
    """
    try:
        # Check if table exists
        spark.sql(f"DESCRIBE TABLE iceberg.{table_name}")
        print(f"Table iceberg.{table_name} already exists")
    except Exception:
        print(f"Creating Iceberg table: iceberg.{table_name}")
        
        # Create table DDL
        create_table_sql = f"""
        CREATE TABLE iceberg.{table_name} (
            id STRING,
            timestamp TIMESTAMP,
            event_type STRING,
            user_id STRING,
            value DOUBLE,
            metadata MAP<STRING, STRING>,
            processing_time TIMESTAMP,
            partition_date DATE
        ) USING iceberg
        PARTITIONED BY (partition_date)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        """
        
        spark.sql(create_table_sql)
        print(f"Successfully created table: iceberg.{table_name}")

def process_batch(batch_df, batch_id, table_name):
    """
    Process each micro-batch and write to Iceberg table
    """
    print(f"Processing batch {batch_id} with {batch_df.count()} records")
    
    if batch_df.count() > 0:
        # Add processing metadata
        processed_df = batch_df \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("partition_date", to_date(col("timestamp")))
        
        # Write to Iceberg table
        processed_df.writeTo(f"iceberg.{table_name}").append()
        
        print(f"Successfully wrote batch {batch_id} to Iceberg table")
    else:
        print(f"Batch {batch_id} is empty, skipping write")

def main():
    """
    Main streaming application
    """
    # Configuration parameters
    S3_INPUT_PATH = os.getenv("S3_INPUT_PATH", "s3a://your-input-bucket/streaming-data/")
    ICEBERG_TABLE_NAME = os.getenv("ICEBERG_TABLE_NAME", "streaming_events")
    CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "s3a://your-checkpoint-bucket/checkpoints/")
    TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
    
    print("Starting Spark S3 to Iceberg Streaming Application")
    print(f"Input path: {S3_INPUT_PATH}")
    print(f"Table name: {ICEBERG_TABLE_NAME}")
    print(f"Checkpoint location: {CHECKPOINT_LOCATION}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Define schema
        schema = define_schema()
        
        # Create Iceberg table
        create_iceberg_table(spark, ICEBERG_TABLE_NAME, schema)
        
        # Read streaming data from S3
        # This example assumes JSON files, modify format as needed
        streaming_df = spark \
            .readStream \
            .format("json") \
            .schema(schema) \
            .option("path", S3_INPUT_PATH) \
            .option("latestFirst", "true") \
            .option("maxFilesPerTrigger", "10") \
            .load()
        
        # Optional: Add data transformations here
        transformed_df = streaming_df \
            .filter(col("id").isNotNull()) \
            .withColumn("timestamp", 
                       when(col("timestamp").isNull(), current_timestamp())
                       .otherwise(col("timestamp")))
        
        # Write stream to Iceberg table
        query = transformed_df \
            .writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_LOCATION) \
            .option("path", f"iceberg.{ICEBERG_TABLE_NAME}") \
            .trigger(processingTime=TRIGGER_INTERVAL) \
            .foreachBatch(lambda batch_df, batch_id: process_batch(batch_df, batch_id, ICEBERG_TABLE_NAME)) \
            .start()
        
        print("Streaming query started. Waiting for data...")
        
        # Wait for the streaming to finish
        query.awaitTermination()
        
    except Exception as e:
        print(f"Error in streaming application: {str(e)}")
        raise e
    finally:
        print("Stopping Spark session")
        spark.stop()

if __name__ == "__main__":
    main()