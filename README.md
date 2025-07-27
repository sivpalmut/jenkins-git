# PySpark S3 to Iceberg Streaming Application

A comprehensive PySpark streaming application that reads data from Amazon S3 and writes it to Apache Iceberg tables. This solution provides real-time data processing capabilities with the reliability and scalability of Iceberg's table format.

## Features

- **Real-time Streaming**: Continuous processing of data files from S3
- **Iceberg Integration**: Write data to Iceberg tables with ACID transactions
- **Schema Evolution**: Support for schema changes over time
- **Partitioning**: Automatic date-based partitioning for efficient queries
- **Checkpointing**: Fault-tolerant processing with automatic recovery
- **Configurable**: Flexible configuration through environment variables
- **Monitoring**: Built-in logging and metrics collection

## Architecture

```
S3 Bucket (JSON files) → Spark Streaming → Data Transformation → Iceberg Table (Parquet + Metadata)
```

## Prerequisites

- Apache Spark 3.4+ with Python support
- AWS account with S3 access
- Python 3.7+
- Apache Iceberg 1.4+

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure AWS credentials:**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

3. **Update configuration:**
   Edit `config.env` with your specific S3 paths and settings.

## Configuration

The application uses environment variables for configuration. Key settings include:

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_INPUT_PATH` | S3 path for input data | `s3a://your-input-bucket/streaming-data/` |
| `CHECKPOINT_LOCATION` | S3 path for checkpoints | `s3a://your-checkpoint-bucket/checkpoints/` |
| `ICEBERG_TABLE_NAME` | Name of the Iceberg table | `streaming_events` |
| `TRIGGER_INTERVAL` | Processing trigger interval | `30 seconds` |
| `AWS_ACCESS_KEY_ID` | AWS access key | - |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | - |

## Usage

### Quick Start

1. **Configure environment:**
   ```bash
   source config.env
   ```

2. **Run the streaming application:**
   ```bash
   ./run_streaming.sh
   ```

3. **Generate test data (optional):**
   ```bash
   python generate_sample_data.py --records 1000 --files 10 --s3-bucket your-bucket
   ```

### Advanced Usage

#### Custom Schema
Modify the `define_schema()` function in `spark_s3_iceberg_streaming.py` to match your data structure:

```python
def define_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        # Add your fields here
    ])
```

#### Data Transformations
Add custom transformations in the main processing loop:

```python
transformed_df = streaming_df \
    .filter(col("id").isNotNull()) \
    .withColumn("processed_time", current_timestamp()) \
    .withColumn("hour", hour(col("timestamp")))
```

#### Partitioning Strategy
Modify the Iceberg table creation to use different partitioning:

```sql
CREATE TABLE iceberg.your_table (
    -- columns
) USING iceberg
PARTITIONED BY (bucket(16, user_id), days(timestamp))
```

## File Structure

```
├── spark_s3_iceberg_streaming.py    # Main streaming application
├── generate_sample_data.py          # Sample data generator
├── run_streaming.sh                 # Application runner script
├── config.env                       # Environment configuration
├── requirements.txt                 # Python dependencies
└── README.md                        # This file
```

## Data Format

The application expects JSON files with the following structure:

```json
{
  "id": "evt_1703123456789_1234",
  "timestamp": "2023-12-20T15:30:45.123456",
  "event_type": "click",
  "user_id": "user_0001",
  "value": 42.50,
  "metadata": {
    "source": "web",
    "version": "1.0",
    "session_id": "sess_123456"
  }
}
```

## Monitoring

The application provides several monitoring capabilities:

- **Console Output**: Real-time processing statistics
- **Spark UI**: Available at `http://localhost:4040`
- **Iceberg Metadata**: Table statistics and file information
- **Custom Metrics**: Processing time, record counts, error rates

## Performance Tuning

### Spark Configuration

```bash
spark-submit \
  --conf spark.executor.memory=4g \
  --conf spark.driver.memory=2g \
  --conf spark.executor.cores=4 \
  --conf spark.default.parallelism=16 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  spark_s3_iceberg_streaming.py
```

### Iceberg Optimization

- Use appropriate file sizes (128MB - 1GB)
- Configure compaction strategies
- Optimize partition pruning
- Use column-level statistics

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure proper IAM permissions for S3 access
2. **Schema Mismatch**: Verify input data matches defined schema
3. **Checkpoint Recovery**: Clean checkpoint location if schema changes
4. **Memory Issues**: Adjust Spark memory settings based on data volume

### Error Messages

- `ClassNotFoundException`: Check Iceberg JAR dependencies
- `S3Exception`: Verify AWS credentials and bucket permissions
- `AnalysisException`: Check table schema and data compatibility

## Testing

### Generate Test Data

```bash
# Generate 5 files with 100 records each
python generate_sample_data.py --files 5 --records 100

# Continuous generation for testing
python generate_sample_data.py --continuous --interval 30 --s3-bucket test-bucket
```

### Verify Iceberg Table

```python
# Query the Iceberg table
spark.sql("SELECT COUNT(*) FROM iceberg.streaming_events").show()
spark.sql("SELECT event_type, COUNT(*) FROM iceberg.streaming_events GROUP BY event_type").show()
```

## Production Considerations

- **Security**: Use IAM roles instead of access keys
- **Networking**: Configure VPC endpoints for S3 access
- **Scaling**: Use EMR or Kubernetes for production deployments
- **Monitoring**: Integrate with CloudWatch or other monitoring systems
- **Backup**: Regular snapshots of Iceberg tables
- **Cost Optimization**: Use S3 lifecycle policies for old data

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
