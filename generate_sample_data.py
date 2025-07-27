#!/usr/bin/env python3
"""
Sample Data Generator for S3 Streaming
Generates sample JSON files to test the streaming application
"""

import json
import os
import time
import random
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError

def generate_sample_record():
    """Generate a single sample record"""
    now = datetime.now()
    
    event_types = ["click", "purchase", "view", "signup", "logout"]
    user_ids = [f"user_{i:04d}" for i in range(1, 1001)]
    
    record = {
        "id": f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        "timestamp": (now - timedelta(seconds=random.randint(0, 3600))).isoformat(),
        "event_type": random.choice(event_types),
        "user_id": random.choice(user_ids),
        "value": round(random.uniform(1.0, 100.0), 2),
        "metadata": {
            "source": "web",
            "version": "1.0",
            "session_id": f"sess_{random.randint(100000, 999999)}",
            "device": random.choice(["desktop", "mobile", "tablet"])
        }
    }
    
    return record

def generate_batch_file(num_records=100):
    """Generate a batch of records and return as JSON string"""
    records = [generate_sample_record() for _ in range(num_records)]
    return "\n".join([json.dumps(record) for record in records])

def upload_to_s3(content, s3_bucket, s3_key, aws_access_key=None, aws_secret_key=None):
    """Upload content to S3"""
    try:
        if aws_access_key and aws_secret_key:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
        else:
            s3_client = boto3.client('s3')
        
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=content.encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"Successfully uploaded to s3://{s3_bucket}/{s3_key}")
        return True
        
    except ClientError as e:
        print(f"Error uploading to S3: {e}")
        return False

def save_local_file(content, filename):
    """Save content to local file"""
    os.makedirs("sample_data", exist_ok=True)
    filepath = os.path.join("sample_data", filename)
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    print(f"Saved sample data to {filepath}")
    return filepath

def main():
    """Main function to generate and optionally upload sample data"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample streaming data")
    parser.add_argument("--records", type=int, default=100, help="Number of records per file")
    parser.add_argument("--files", type=int, default=5, help="Number of files to generate")
    parser.add_argument("--s3-bucket", help="S3 bucket to upload files (optional)")
    parser.add_argument("--s3-prefix", default="streaming-data", help="S3 prefix for files")
    parser.add_argument("--local-only", action="store_true", help="Only save files locally")
    parser.add_argument("--continuous", action="store_true", help="Generate files continuously")
    parser.add_argument("--interval", type=int, default=60, help="Interval between files in continuous mode (seconds)")
    
    args = parser.parse_args()
    
    # AWS credentials from environment
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if args.continuous:
        print(f"Starting continuous data generation (interval: {args.interval}s)")
        file_counter = 0
        
        try:
            while True:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"events_{timestamp}_{file_counter:04d}.json"
                
                # Generate data
                content = generate_batch_file(args.records)
                
                # Save locally
                save_local_file(content, filename)
                
                # Upload to S3 if configured
                if not args.local_only and args.s3_bucket:
                    s3_key = f"{args.s3_prefix}/{filename}"
                    upload_to_s3(content, args.s3_bucket, s3_key, aws_access_key, aws_secret_key)
                
                file_counter += 1
                print(f"Generated file {file_counter}, waiting {args.interval}s...")
                time.sleep(args.interval)
                
        except KeyboardInterrupt:
            print("\nStopping continuous generation.")
    
    else:
        print(f"Generating {args.files} files with {args.records} records each")
        
        for i in range(args.files):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"events_{timestamp}_{i:04d}.json"
            
            # Generate data
            content = generate_batch_file(args.records)
            
            # Save locally
            save_local_file(content, filename)
            
            # Upload to S3 if configured
            if not args.local_only and args.s3_bucket:
                s3_key = f"{args.s3_prefix}/{filename}"
                upload_to_s3(content, args.s3_bucket, s3_key, aws_access_key, aws_secret_key)
            
            # Small delay between files
            time.sleep(1)
        
        print(f"Generated {args.files} sample data files")

if __name__ == "__main__":
    main()