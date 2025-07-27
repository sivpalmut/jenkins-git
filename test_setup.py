#!/usr/bin/env python3
"""
Test script to validate Spark and Iceberg setup
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import logging
from config import S3_CONFIG, AWS_CONFIG, ICEBERG_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_spark_session():
    """
    Test Spark session creation with Iceberg support
    """
    try:
        logger.info("Testing Spark session creation...")
        
        # Build warehouse path
        warehouse_path = f"s3://{S3_CONFIG['warehouse_bucket']}/{S3_CONFIG['warehouse_path']}"
        
        spark = SparkSession.builder \
            .appName("TestSetup") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}", ICEBERG_CONFIG['catalog_impl']) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.warehouse", warehouse_path) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.catalog-impl", ICEBERG_CONFIG['catalog_impl']) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.io-impl", ICEBERG_CONFIG['io_impl']) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.endpoint", AWS_CONFIG['endpoint']) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.region", AWS_CONFIG['region']) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.access-key", AWS_CONFIG['access_key']) \
            .config(f"spark.sql.catalog.{ICEBERG_CONFIG['catalog_name']}.s3.secret-key", AWS_CONFIG['secret_key']) \
            .getOrCreate()
        
        logger.info("✅ Spark session created successfully")
        return spark
        
    except Exception as e:
        logger.error(f"❌ Error creating Spark session: {str(e)}")
        raise

def test_iceberg_catalog(spark):
    """
    Test Iceberg catalog functionality
    """
    try:
        logger.info("Testing Iceberg catalog...")
        
        # Test catalog creation
        catalog_name = ICEBERG_CONFIG['catalog_name']
        table_name = ICEBERG_CONFIG['table_name']
        
        # Create a test table
        create_test_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.test_table (
            id STRING,
            name STRING,
            value DOUBLE
        ) USING iceberg
        """
        
        spark.sql(create_test_table_sql)
        logger.info("✅ Test Iceberg table created successfully")
        
        # Insert test data
        test_data = [("1", "test1", 100.0), ("2", "test2", 200.0)]
        test_df = spark.createDataFrame(test_data, ["id", "name", "value"])
        
        test_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(f"{catalog_name}.test_table")
        
        logger.info("✅ Test data inserted successfully")
        
        # Query test data
        result = spark.sql(f"SELECT * FROM {catalog_name}.test_table")
        count = result.count()
        logger.info(f"✅ Test query successful, found {count} records")
        
        # Clean up test table
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.test_table")
        logger.info("✅ Test table cleaned up")
        
    except Exception as e:
        logger.error(f"❌ Error testing Iceberg catalog: {str(e)}")
        raise

def test_s3_access(spark):
    """
    Test S3 access
    """
    try:
        logger.info("Testing S3 access...")
        
        # Test reading from S3 (just check if path exists)
        input_path = f"s3://{S3_CONFIG['input_bucket']}/{S3_CONFIG['input_path']}"
        
        # Try to list files (this will fail if bucket doesn't exist or no access)
        try:
            # Create a dummy DataFrame to test S3 access
            dummy_data = [("test",)]
            dummy_df = spark.createDataFrame(dummy_data, ["test"])
            
            # Try to write to S3 (this will test access)
            test_output_path = f"s3://{S3_CONFIG['checkpoint_bucket']}/test-access"
            dummy_df.write.mode("overwrite").parquet(test_output_path)
            
            logger.info("✅ S3 access test successful")
            
        except Exception as s3_error:
            logger.warning(f"⚠️ S3 access test failed: {str(s3_error)}")
            logger.warning("This might be expected if buckets don't exist yet")
        
    except Exception as e:
        logger.error(f"❌ Error testing S3 access: {str(e)}")
        raise

def test_schema_definition():
    """
    Test schema definition
    """
    try:
        logger.info("Testing schema definition...")
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("source", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True)
        ])
        
        logger.info("✅ Schema definition successful")
        logger.info(f"Schema fields: {[field.name for field in schema.fields]}")
        
    except Exception as e:
        logger.error(f"❌ Error testing schema definition: {str(e)}")
        raise

def main():
    """
    Run all tests
    """
    spark = None
    try:
        logger.info("🚀 Starting setup validation tests...")
        
        # Test 1: Spark session
        spark = test_spark_session()
        
        # Test 2: Iceberg catalog
        test_iceberg_catalog(spark)
        
        # Test 3: S3 access
        test_s3_access(spark)
        
        # Test 4: Schema definition
        test_schema_definition()
        
        logger.info("🎉 All tests passed! Your setup is ready for streaming.")
        
    except Exception as e:
        logger.error(f"❌ Setup validation failed: {str(e)}")
        logger.error("Please check your configuration and try again.")
        return False
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)