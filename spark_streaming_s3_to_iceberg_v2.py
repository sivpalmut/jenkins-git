#!/usr/bin/env python3
"""
PySpark Streaming Application: Read from S3 and Write to Iceberg Table
Enhanced version with configuration management and better monitoring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
import os
from datetime import datetime
from config import S3_CONFIG, AWS_CONFIG, ICEBERG_CONFIG, SPARK_CONFIG, DATA_SCHEMA

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3ToIcebergStreaming:
    """
    Main class for S3 to Iceberg streaming application
    """
    
    def __init__(self):
        self.spark = None
        self.query = None
        
    def create_spark_session(self):
        """
        Create and configure Spark session with Iceberg support
        """
        try:
            # Build warehouse and checkpoint paths
            warehouse_path = f"s3://{S3_CONFIG['warehouse_bucket']}/{S3_CONFIG['warehouse_path']}"
            checkpoint_path = f"s3://{S3_CONFIG['checkpoint_bucket']}/{S3_CONFIG['checkpoint_path']}"
            
            spark = SparkSession.builder \
                .appName(SPARK_CONFIG['app_name']) \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}", ICEBERG_CONFIG['catalog_impl']) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.warehouse", warehouse_path) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.catalog-impl", ICEBERG_CONFIG['catalog_impl']) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.io-impl", ICEBERG_CONFIG['io_impl']) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.endpoint", AWS_CONFIG['endpoint']) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.region", AWS_CONFIG['region']) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.access-key", AWS_CONFIG['access_key']) \
                .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.secret-key", AWS_CONFIG['secret_key']) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.streaming.checkpointLocation", checkpoint_path) \
                .config("spark.sql.streaming.schemaInference", "true") \
                .config("spark.sql.streaming.unsupportedOperationCheck", "false") \
                .getOrCreate()
            
            self.spark = spark
            logger.info("Spark session created successfully")
            return spark
            
        except Exception as e:
            logger.error(f"Error creating Spark session: {str(e)}")
            raise
    
    def define_schema(self):
        """
        Define the schema for the streaming data
        Modify this based on your actual data structure
        """
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("source", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        return schema
    
    def create_iceberg_table(self):
        """
        Create Iceberg table if it doesn't exist
        """
        try:
            table_name = ICEBERG_CONFIG['table_name']
            catalog_name = ICEBERG_CONFIG['catalog_name']
            
            # Create table with Iceberg format
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {catalog_name}.{table_name} (
                id STRING,
                timestamp TIMESTAMP,
                value DOUBLE,
                category STRING,
                source STRING,
                user_id STRING,
                event_type STRING,
                processed_at TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (category, days(timestamp))
            """
            
            self.spark.sql(create_table_sql)
            logger.info(f"Successfully created/verified Iceberg table: {catalog_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Error creating Iceberg table: {str(e)}")
            raise
    
    def read_streaming_data(self):
        """
        Read streaming data from S3 based on configuration
        """
        input_path = f"s3://{S3_CONFIG['input_bucket']}/{S3_CONFIG['input_path']}"
        
        # Configure streaming reader based on data format
        reader = self.spark.readStream \
            .format(DATA_SCHEMA['format']) \
            .option("maxFilesPerTrigger", SPARK_CONFIG['max_files_per_trigger']) \
            .option("cleanSource", SPARK_CONFIG['clean_source'])
        
        # Add format-specific options
        if DATA_SCHEMA['format'] == 'csv':
            reader = reader \
                .option("header", str(DATA_SCHEMA['header']).lower()) \
                .option("inferSchema", str(DATA_SCHEMA['infer_schema']).lower()) \
                .schema(self.define_schema())
        elif DATA_SCHEMA['format'] == 'json':
            reader = reader.schema(self.define_schema())
        elif DATA_SCHEMA['format'] == 'parquet':
            # Parquet files usually have schema embedded
            pass
        
        streaming_df = reader.load(input_path)
        logger.info(f"Streaming reader configured for {input_path}")
        return streaming_df
    
    def process_streaming_data(self, df, epoch_id):
        """
        Process each batch of streaming data
        """
        try:
            if df.count() > 0:
                logger.info(f"Processing batch {epoch_id} with {df.count()} records")
                
                # Add processing timestamp and any data transformations
                processed_df = df.withColumn("processed_at", current_timestamp()) \
                    .withColumn("batch_id", lit(epoch_id)) \
                    .withColumn("processing_date", current_date())
                
                # Write to Iceberg table
                table_name = f"{ICEBERG_CONFIG['catalog_name']}.{ICEBERG_CONFIG['table_name']}"
                
                processed_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(table_name)
                
                logger.info(f"Successfully wrote {df.count()} records to Iceberg table")
                
                # Log some statistics
                self.log_batch_statistics(processed_df, epoch_id)
                
            else:
                logger.info(f"Batch {epoch_id} is empty, skipping")
                
        except Exception as e:
            logger.error(f"Error processing batch {epoch_id}: {str(e)}")
            # You might want to implement dead letter queue here
            raise
    
    def log_batch_statistics(self, df, epoch_id):
        """
        Log statistics about the processed batch
        """
        try:
            # Get some basic statistics
            total_records = df.count()
            categories = df.select("category").distinct().count()
            sources = df.select("source").distinct().count()
            
            logger.info(f"Batch {epoch_id} Statistics:")
            logger.info(f"  - Total records: {total_records}")
            logger.info(f"  - Unique categories: {categories}")
            logger.info(f"  - Unique sources: {sources}")
            
        except Exception as e:
            logger.warning(f"Could not log batch statistics: {str(e)}")
    
    def start_streaming(self):
        """
        Start the streaming query
        """
        try:
            # Read streaming data
            streaming_df = self.read_streaming_data()
            
            # Start the streaming query
            self.query = streaming_df.writeStream \
                .foreachBatch(self.process_streaming_data) \
                .outputMode("append") \
                .trigger(processingTime=SPARK_CONFIG['processing_time']) \
                .option("checkpointLocation", f"s3://{S3_CONFIG['checkpoint_bucket']}/{S3_CONFIG['checkpoint_path']}") \
                .start()
            
            logger.info("Streaming query started successfully")
            logger.info(f"Processing time: {SPARK_CONFIG['processing_time']}")
            
            return self.query
            
        except Exception as e:
            logger.error(f"Error starting streaming query: {str(e)}")
            raise
    
    def run(self):
        """
        Main method to run the streaming application
        """
        try:
            # Create Spark session
            self.create_spark_session()
            
            # Create Iceberg table
            self.create_iceberg_table()
            
            # Start streaming
            query = self.start_streaming()
            
            # Wait for the query to terminate
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming application: {str(e)}")
            sys.exit(1)
        finally:
            self.cleanup()
    
    def cleanup(self):
        """
        Cleanup resources
        """
        try:
            if self.query and self.query.isActive:
                self.query.stop()
                logger.info("Streaming query stopped")
            
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    """
    Main function to run the streaming application
    """
    app = S3ToIcebergStreaming()
    app.run()

if __name__ == "__main__":
    main()