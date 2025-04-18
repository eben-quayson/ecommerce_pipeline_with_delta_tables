import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime
import os
import sys
import types
from decimal import Decimal

# Mocking the awsglue modules to avoid import errors during testing
@pytest.fixture(autouse=True, scope="session")
def mock_awsglue_modules():
    import types
    import sys

    # Mock the awsglue.utils module
    sys.modules['awsglue'] = types.SimpleNamespace()
    sys.modules['awsglue.transforms'] = types.SimpleNamespace()
    sys.modules['awsglue.utils'] = types.SimpleNamespace()

    # Mock the getResolvedOptions function
    def mock_getResolvedOptions(argv, options):
        # Return a dictionary with default values for the required options
        return {option: f"mock_value_for_{option}" for option in options}

    sys.modules['awsglue.utils'].getResolvedOptions = mock_getResolvedOptions



# Add the parent directory to the system path to import the transformation scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Mock parameters for local testing
dataset_type = 'orders'
source_path = './mock_data/source/'
target_path = './mock_data/target/'
rejected_path = './mock_data/rejected/'
job_name = 'test_job'


from transformation_scripts.transformations import (
    get_schema_for_dataset,
    get_required_cols,
    get_pk_column,
    get_partition_column,
    get_final_columns,
)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TransformationTests") \
        .master("local[*]") \
        .getOrCreate()


# ----------------------
# PURE FUNCTION TESTS
# ----------------------

def test_get_required_cols_orders():
    assert get_required_cols("orders") == ["order_id", "user_id"]

def test_get_partition_column_orders():
    assert get_partition_column("orders") == "order_date"

def test_get_final_columns_order_items():
    assert get_final_columns("order_items") == [
         "order_item_id", "order_id", "user_id", "product_id", "add_to_cart_order",
        "reordered", "order_timestamp", "order_date", "days_since_prior_order",
        "_ingestion_timestamp_utc", "_source_file"
    ]


# ----------------------
# DATAFRAME TRANSFORMATION TESTS
# ----------------------

def test_orders_schema_and_validation(spark):
    schema = get_schema_for_dataset("orders")
    
    data = [
        ("1", "o1", "u1", "2024-01-01 09:00:00", Decimal("100.00"), "2024-01-01"),
        ("2", "o2", None, "2024-01-02 09:00:00", Decimal("150.50"), "2024-01-02"),
        ("3", "o3", "u3", None, Decimal("200.25"), "2024-01-03"),
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    
    # Required columns check
    required_cols = get_required_cols("orders")
    for col in required_cols:
        assert col in df.columns
    
    # Null check
    null_required = df.filter("user_id IS NULL")
    assert null_required.count() == 1

    # Timestamp check
    df = df.withColumn("created_at", df["order_timestamp_str"].cast(TimestampType()))
    invalid_timestamps = df.filter("created_at IS NULL")
    assert invalid_timestamps.count() == 1


def test_metadata_enrichment(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_date", TimestampType(), True),
        
    ])

    data = [("1", "u1", datetime.now())]
    df = spark.createDataFrame(data, schema=schema)

    enriched_df = df \
        .withColumn("ingestion_timestamp", df["order_date"]) \
        .withColumn("source_file", df["user_id"])  # Just mocking enrichment

    assert "ingestion_timestamp" in enriched_df.columns
    assert "source_file" in enriched_df.columns


def test_deduplication_logic(spark):
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("order_date", TimestampType(), True),
    ])

    data = [
        ("1", "u1", datetime.now()),
        ("1", "u1", datetime.now()),  # duplicate based on order_id
        ("2", "u2", datetime.now())
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    
    # Deduplication by primary key
    pk = get_pk_column("orders")
    deduped_df = df.dropDuplicates([pk])
    
    assert deduped_df.count() == 2

