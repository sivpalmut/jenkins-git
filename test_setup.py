#!/usr/bin/env python3
"""
Test script to verify PySpark and Iceberg setup
"""

import sys
import os

def test_imports():
    """Test that all required packages can be imported"""
    try:
        import pyspark
        print(f"✓ PySpark version: {pyspark.__version__}")
    except ImportError as e:
        print(f"✗ PySpark import failed: {e}")
        return False
    
    try:
        import boto3
        print(f"✓ Boto3 version: {boto3.__version__}")
    except ImportError as e:
        print(f"✗ Boto3 import failed: {e}")
        return False
    
    try:
        from pyspark.sql import SparkSession
        print("✓ PySpark SQL imported successfully")
    except ImportError as e:
        print(f"✗ PySpark SQL import failed: {e}")
        return False
    
    return True

def test_spark_session():
    """Test creating a basic Spark session"""
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("TestSetup") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        print("✓ Spark session created successfully")
        
        # Test basic DataFrame operations
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        columns = ["name", "age"]
        
        df = spark.createDataFrame(data, columns)
        count = df.count()
        
        print(f"✓ DataFrame operations working (created DF with {count} rows)")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"✗ Spark session test failed: {e}")
        return False

def test_environment_variables():
    """Test that required environment variables are set"""
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY"
    ]
    
    optional_vars = [
        "S3_INPUT_PATH",
        "CHECKPOINT_LOCATION",
        "ICEBERG_TABLE_NAME"
    ]
    
    all_good = True
    
    print("\nChecking environment variables:")
    
    for var in required_vars:
        if os.getenv(var):
            print(f"✓ {var} is set")
        else:
            print(f"✗ {var} is not set (required)")
            all_good = False
    
    for var in optional_vars:
        if os.getenv(var):
            print(f"✓ {var} is set")
        else:
            print(f"○ {var} is not set (optional, will use default)")
    
    return all_good

def test_aws_connectivity():
    """Test AWS connectivity"""
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
        
        s3_client = boto3.client('s3')
        
        # Try to list buckets (requires valid credentials)
        response = s3_client.list_buckets()
        bucket_count = len(response.get('Buckets', []))
        
        print(f"✓ AWS S3 connectivity successful ({bucket_count} buckets accessible)")
        return True
        
    except NoCredentialsError:
        print("✗ AWS credentials not found")
        return False
    except ClientError as e:
        print(f"✗ AWS connectivity failed: {e}")
        return False
    except Exception as e:
        print(f"✗ AWS test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("PySpark S3 to Iceberg Streaming Setup Test")
    print("=" * 50)
    
    tests = [
        ("Python Imports", test_imports),
        ("Spark Session", test_spark_session),
        ("Environment Variables", test_environment_variables),
        ("AWS Connectivity", test_aws_connectivity)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 20)
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY:")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        symbol = "✓" if result else "✗"
        print(f"{symbol} {test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your setup is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the configuration.")
        return 1

if __name__ == "__main__":
    sys.exit(main())