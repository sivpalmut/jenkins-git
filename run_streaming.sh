#!/bin/bash

# PySpark S3 to Iceberg Streaming Application Runner
# This script sets up the environment and runs the streaming application

set -e

echo "Setting up PySpark S3 to Iceberg Streaming Application..."

# Load environment variables
if [ -f "config.env" ]; then
    echo "Loading configuration from config.env..."
    source config.env
else
    echo "Warning: config.env not found. Make sure to set environment variables manually."
fi

# Check required environment variables
required_vars=("AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "S3_INPUT_PATH")
for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        echo "Error: Required environment variable $var is not set"
        exit 1
    fi
done

echo "Configuration:"
echo "  Input Path: ${S3_INPUT_PATH}"
echo "  Table Name: ${ICEBERG_TABLE_NAME:-streaming_events}"
echo "  Checkpoint: ${CHECKPOINT_LOCATION:-s3a://your-checkpoint-bucket/checkpoints/}"
echo "  Trigger Interval: ${TRIGGER_INTERVAL:-30 seconds}"

# Install Python dependencies if requirements.txt exists
if [ -f "requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip install -r requirements.txt
fi

# Set Spark configuration for better performance
export SPARK_CONF_DIR=${SPARK_CONF_DIR:-./conf}

# Additional Spark options
SPARK_OPTS="--conf spark.executor.memory=2g \
           --conf spark.driver.memory=2g \
           --conf spark.executor.cores=2 \
           --conf spark.default.parallelism=8 \
           --conf spark.sql.streaming.metricsEnabled=true \
           --conf spark.sql.streaming.streamingQueryListeners=org.apache.spark.sql.streaming.StreamingQueryListener"

echo "Starting PySpark Streaming Application..."

# Run the application
if command -v spark-submit &> /dev/null; then
    spark-submit \
        --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        $SPARK_OPTS \
        spark_s3_iceberg_streaming.py
else
    echo "spark-submit not found. Running with python directly..."
    python spark_s3_iceberg_streaming.py
fi

echo "Streaming application finished."