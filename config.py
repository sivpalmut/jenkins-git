"""
Configuration file for S3 to Iceberg streaming application
"""

# S3 Configuration
S3_CONFIG = {
    "input_bucket": "your-input-bucket",
    "input_path": "streaming-data/*",
    "warehouse_bucket": "your-warehouse-bucket",
    "warehouse_path": "iceberg-warehouse",
    "checkpoint_bucket": "your-checkpoint-bucket",
    "checkpoint_path": "checkpoints"
}

# AWS Configuration
AWS_CONFIG = {
    "region": "us-east-1",
    "access_key": "your-access-key",
    "secret_key": "your-secret-key",
    "endpoint": "https://s3.amazonaws.com"
}

# Iceberg Configuration
ICEBERG_CONFIG = {
    "catalog_name": "iceberg",
    "table_name": "streaming_data",
    "catalog_impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "io_impl": "org.apache.iceberg.aws.s3.S3FileIO"
}

# Spark Configuration
SPARK_CONFIG = {
    "app_name": "S3ToIcebergStreaming",
    "processing_time": "30 seconds",
    "max_files_per_trigger": 10,
    "clean_source": "delete"
}

# Data Schema Configuration
DATA_SCHEMA = {
    "format": "csv",  # Options: csv, json, parquet
    "header": True,
    "infer_schema": False
}