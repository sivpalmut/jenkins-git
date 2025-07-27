# PySpark Streaming: S3 to Iceberg Table

This project provides a PySpark streaming application that reads data from S3 and writes it to an Apache Iceberg table. The application supports multiple file formats (CSV, JSON, Parquet) and includes comprehensive error handling and monitoring.

## Features

- **Streaming Data Processing**: Real-time data processing from S3
- **Iceberg Integration**: Write to Apache Iceberg tables with ACID transactions
- **Multiple File Formats**: Support for CSV, JSON, and Parquet files
- **Configurable**: Easy configuration management
- **Error Handling**: Comprehensive error handling and logging
- **Monitoring**: Batch statistics and processing metrics
- **Checkpointing**: Fault-tolerant streaming with S3 checkpointing

## Prerequisites

- Apache Spark 3.4.0 or higher
- Python 3.8 or higher
- AWS S3 access
- Apache Iceberg 1.4.0 or higher

## Installation

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Download Iceberg JARs** (if not using managed Spark):
   ```bash
   # Download Iceberg Spark Runtime JAR
   wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.0/iceberg-spark-runtime-3.4_2.12-1.4.0.jar
   ```

## Configuration

### 1. Update `config.py`

Edit the configuration file with your AWS and S3 settings:

```python
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
```

### 2. Environment Variables (Alternative)

You can also set AWS credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

## Usage

### Basic Usage

```bash
python spark_streaming_s3_to_iceberg_v2.py
```

### With Spark Submit

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.0 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  spark_streaming_s3_to_iceberg_v2.py
```

### On EMR

```bash
spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.iceberg.warehouse=s3://your-warehouse-bucket/iceberg-warehouse \
  spark_streaming_s3_to_iceberg_v2.py
```

## Data Schema

The application expects data with the following schema:

```python
StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True),
    StructField("category", StringType(), True),
    StructField("source", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True)
])
```

### Sample Data Formats

**CSV Format**:
```csv
id,timestamp,value,category,source,user_id,event_type
1,2024-01-01 10:00:00,100.5,electronics,web,user123,view
2,2024-01-01 10:01:00,250.0,clothing,mobile,user456,purchase
```

**JSON Format**:
```json
{"id": "1", "timestamp": "2024-01-01T10:00:00", "value": 100.5, "category": "electronics", "source": "web", "user_id": "user123", "event_type": "view"}
{"id": "2", "timestamp": "2024-01-01T10:01:00", "value": 250.0, "category": "clothing", "source": "mobile", "user_id": "user456", "event_type": "purchase"}
```

## Iceberg Table Structure

The application creates an Iceberg table with the following structure:

```sql
CREATE TABLE iceberg.streaming_data (
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
```

## Configuration Options

### Spark Configuration

- `processing_time`: How often to process batches (default: "30 seconds")
- `max_files_per_trigger`: Maximum files to process per batch (default: 10)
- `clean_source`: Whether to delete processed files (default: "delete")

### Data Schema Configuration

- `format`: Input file format ("csv", "json", "parquet")
- `header`: Whether CSV files have headers (default: True)
- `infer_schema`: Whether to infer schema (default: False)

## Monitoring and Logging

The application provides comprehensive logging:

- **Application Start/Stop**: Session creation and cleanup
- **Batch Processing**: Number of records processed per batch
- **Statistics**: Unique categories, sources, and record counts
- **Error Handling**: Detailed error messages and stack traces

### Sample Log Output

```
2024-01-01 10:00:00 - __main__ - INFO - Spark session created successfully
2024-01-01 10:00:01 - __main__ - INFO - Successfully created/verified Iceberg table: iceberg.streaming_data
2024-01-01 10:00:02 - __main__ - INFO - Streaming reader configured for s3://your-input-bucket/streaming-data/*
2024-01-01 10:00:03 - __main__ - INFO - Streaming query started successfully
2024-01-01 10:00:33 - __main__ - INFO - Processing batch 0 with 150 records
2024-01-01 10:00:34 - __main__ - INFO - Successfully wrote 150 records to Iceberg table
2024-01-01 10:00:34 - __main__ - INFO - Batch 0 Statistics:
2024-01-01 10:00:34 - __main__ - INFO -   - Total records: 150
2024-01-01 10:00:34 - __main__ - INFO -   - Unique categories: 5
2024-01-01 10:00:34 - __main__ - INFO -   - Unique sources: 3
```

## Error Handling

The application includes robust error handling:

- **Spark Session Errors**: Graceful handling of session creation failures
- **Table Creation Errors**: Proper error handling for Iceberg table creation
- **Batch Processing Errors**: Individual batch failures don't stop the entire stream
- **Resource Cleanup**: Proper cleanup of Spark sessions and streaming queries

## Performance Tuning

### Spark Configuration

```python
# Add these configurations for better performance
.config("spark.sql.adaptive.enabled", "true")
.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
.config("spark.sql.adaptive.skewJoin.enabled", "true")
.config("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### Iceberg Configuration

```python
# Optimize Iceberg writes
.config("spark.sql.iceberg.optimize.enabled", "true")
.config("spark.sql.iceberg.optimize.auto.enabled", "true")
```

## Troubleshooting

### Common Issues

1. **S3 Access Denied**: Check AWS credentials and bucket permissions
2. **Iceberg Table Creation Failed**: Verify Iceberg JARs are properly loaded
3. **Schema Mismatch**: Ensure input data matches the defined schema
4. **Checkpoint Errors**: Verify checkpoint bucket exists and is writable

### Debug Mode

Enable debug logging:

```python
logging.basicConfig(level=logging.DEBUG)
```

## Security Considerations

- Use IAM roles instead of hardcoded credentials in production
- Enable S3 encryption for sensitive data
- Use VPC endpoints for S3 access in private subnets
- Implement proper access controls for Iceberg tables

## Production Deployment

### EMR Configuration

```bash
spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.iceberg.warehouse=s3://your-warehouse-bucket/iceberg-warehouse \
  --conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.iceberg.s3.region=us-east-1 \
  spark_streaming_s3_to_iceberg_v2.py
```

### Kubernetes Deployment

Create a Spark operator configuration for Kubernetes deployment.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.
