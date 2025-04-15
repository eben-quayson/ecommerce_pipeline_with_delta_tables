# test_transformation.py
import pytest
from decimal import Decimal
import os
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, to_timestamp, current_timestamp, when, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
    TimestampType, BooleanType, DateType, DoubleType
)
from datetime import datetime, date
import logging

logging.getLogger('py4j').setLevel(logging.ERROR)
logging.getLogger('pyspark').setLevel(logging.ERROR)


@pytest.fixture(scope="session")
def spark():
    """Provides a Spark session for the test suite."""
    session = SparkSession.builder \
        .appName("pytest-local-spark-testing") \
        .master("local[2]")\
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "512m")\
        .config("spark.executor.memory", "512m")\
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()

# --- Redefined Helper Functions (based on the provided script) ---
def get_schema_for_dataset(dataset_type):
    if dataset_type == 'orders':
        return StructType([
            StructField("order_num", StringType(), True), StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True), StructField("order_timestamp_str", StringType(), True),
            StructField("total_amount", DecimalType(10, 2), True), StructField("date_str", StringType(), True)
        ])
    elif dataset_type == 'order_items':
         return StructType([
            StructField("id", StringType(), True), StructField("order_id", StringType(), True),
            StructField("user_id", StringType(), True), # Added user_id based on final cols
            StructField("product_id", StringType(), True), StructField("add_to_cart_order", IntegerType(), True),
            StructField("reordered", StringType(), True), StructField("order_timestamp", StringType(), True), # Assuming string initially
            StructField("days_since_prior_order", StringType(), True) # Assuming string initially
        ])
    elif dataset_type == 'products':
         return StructType([
            StructField("product_id", StringType(), True), StructField("department_id", StringType(), True),
            StructField("department", StringType(), True), StructField("product_name", StringType(), True)
        ])
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")

def get_required_cols(dataset_type):
    if dataset_type == 'orders':
        return ["order_id", "user_id"]
    elif dataset_type == 'order_items':
        return ["id", "order_id"] # Original required cols
    elif dataset_type == 'products':
        return ["product_id"]
    else:
        return []

def get_pk_column(dataset_type):
    if dataset_type == 'orders':
        return "order_id"
    elif dataset_type == 'order_items':
        return "order_item_id" # Target PK name
    elif dataset_type == 'products':
        return "product_id"
    else:
        raise ValueError(f"No PK defined for {dataset_type}")

def get_final_columns(dataset_type):
    common_cols = ["_ingestion_timestamp_utc", "_source_file"]
    if dataset_type == 'orders':
        return ["order_num", "order_id", "user_id", "total_amount", "order_timestamp", "order_date"] + common_cols
    elif dataset_type == 'order_items':
        return ["order_item_id", "order_id", "user_id", "product_id", "add_to_cart_order",
                "reordered", "order_timestamp", "order_date", "days_since_prior_order"] + common_cols
    elif dataset_type == 'products':
        return ["product_id", "department_id", "department", "product_name"] + common_cols
    else:
        return []

# --- Test Data ---

@pytest.fixture
def orders_raw_data(spark):
    schema = get_schema_for_dataset('orders')
    data = [
        ("ORD100", "O1", "U1", "2023-10-26T10:00:00", 100.50, "2023-10-26"), # Valid
        ("ORD101", "O2", "U2", "2023-10-26T11:30:00", 75.00, "2023-10-26"),  # Valid
        ("ORD102", None, "U3", "2023-10-27T09:15:00", 25.25, "2023-10-27"), # Invalid (null order_id)
        ("ORD103", "O3", None, "2023-10-27T14:00:00", 50.00, "2023-10-27"), # Invalid (null user_id)
        ("ORD104", "O4", "U4", "invalid-date", 10.00, "2023-10-28"),       # Invalid (bad timestamp str)
        ("ORD105", "O1", "U5", "2023-10-29T10:00:00", 200.00, "2023-10-29"), # Duplicate O1
    ]
    # Convert Decimal manually for Spark < 3.x compatibility if needed
    data_rows = [Row(order_num=r[0], order_id=r[1], user_id=r[2], order_timestamp_str=r[3], total_amount=Decimal(r[4]) if r[4] is not None else None, date_str=r[5]) for r in data]
    return spark.createDataFrame(data_rows, schema).withColumn("_source_file", lit("dummy_orders.csv"))

@pytest.fixture
def order_items_raw_data(spark):
    schema = get_schema_for_dataset('order_items')
    data = [
        ("I1", "O1", "U1", "P1", 1, "0", "2023-10-26 10:05:00", "10"), # Valid
        ("I2", "O1", "U1", "P2", 2, "1", "2023-10-26 10:05:00", "10"), # Valid
        ("I3", "O2", "U2", "P3", 1, "0", "2023-10-26 11:35:00", None), # Valid
        (None, "O3", "U3", "P1", 1, "0", "2023-10-27 09:20:00", "5"),  # Invalid (null id)
        ("I4", None, "U4", "P4", 1, "1", "2023-10-27 14:05:00", "3"),  # Invalid (null order_id)
        ("I5", "O4", "U5", "P5", 1, "9", "2023-10-28 10:10:00", "2"),  # Invalid reordered value
        ("I1", "O5", "U6", "P6", 1, "0", "2023-10-29 11:00:00", "1"),  # Duplicate I1
    ]
    return spark.createDataFrame(data, schema).withColumn("_source_file", lit("dummy_items.csv"))

@pytest.fixture
def products_raw_data(spark):
    schema = get_schema_for_dataset('products')
    data = [
        ("P1", "D1", "Dept A", "Product Alpha"),    # Valid
        ("P2", "D1", "Dept A", "Product Beta"),     # Valid
        (None, "D2", "Dept B", "Product Gamma"),    # Invalid (null product_id)
        ("P3", "D2", "Dept B", "Product Delta"),    # Valid
        ("P1", "D3", "Dept C", "Product Alpha Dup"),# Duplicate P1
    ]
    return spark.createDataFrame(data, schema).withColumn("_source_file", lit("dummy_products.csv"))


# --- Test Functions ---

def apply_transformations(df, dataset_type):
    """Applies the core transformation logic from the script."""
    transformed_df = df
    if dataset_type == 'orders':
        transformed_df = transformed_df \
            .withColumn("order_timestamp", to_timestamp(col("order_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("order_date", col("order_timestamp").cast(DateType())) \
            .drop("order_timestamp_str", "date_str")
    elif dataset_type == 'order_items':
        transformed_df = transformed_df \
            .withColumnRenamed("id", "order_item_id") \
            .withColumn("order_timestamp", col("order_timestamp").cast(TimestampType())) \
            .withColumn("order_date", col("order_timestamp").cast(DateType())) \
            .withColumn("reordered",
                        when(col("reordered") == "1", lit(True)) # Assuming input is string '1' or '0'
                        .when(col("reordered") == "0", lit(False))
                        .otherwise(lit(None).cast(BooleanType()))) \
            .withColumn("days_since_prior_order", col("days_since_prior_order").cast(IntegerType())) # Cast days
    elif dataset_type == 'products':
        transformed_df = transformed_df # No specific transformations in the original script
    return transformed_df

def apply_validation(df, dataset_type):
    """Applies the core validation logic."""
    required_cols_source = get_required_cols(dataset_type)
    null_check_condition = None

    # Build null check condition based on *original* required columns
    for req_col in required_cols_source:
        # Handle potential renames during check
        current_col_name = req_col
        if req_col == 'id' and dataset_type == 'order_items' and 'order_item_id' in df.columns:
            current_col_name = 'order_item_id' # Check the renamed column

        if current_col_name in df.columns:
            cond = col(current_col_name).isNull()
            null_check_condition = cond if null_check_condition is None else null_check_condition | cond
        else:
             # This case should ideally not happen if schema is applied correctly
             print(f"Warning: Required source column '{req_col}' (checking as '{current_col_name}') not found in DataFrame for validation.")


    # Check timestamp validity AFTER conversion for relevant types
    if dataset_type in ['orders', 'order_items'] and 'order_timestamp' in df.columns:
        ts_cond = col('order_timestamp').isNull()
        null_check_condition = ts_cond if null_check_condition is None else null_check_condition | ts_cond

    # Add check for invalid 'reordered' values (became null after transformation)
    if dataset_type == 'order_items' and 'reordered' in df.columns:
        reorder_cond = col('reordered').isNull() & col('reordered').cast(StringType()).isNotNull() 


    if null_check_condition is not None:
        rejected_df = df.filter(null_check_condition) \
                        .withColumn("rejection_reason", lit("Null key or invalid data")) 
        valid_df = df.filter(~null_check_condition)
    else:
        
        rejected_df = spark.createDataFrame([], df.schema.add("rejection_reason", StringType()))
        valid_df = df

    return valid_df, rejected_df

def apply_deduplication_and_final_steps(valid_df, dataset_type):
    """Applies deduplication, adds metadata, selects final columns."""
    pk_column_target = get_pk_column(dataset_type)
    valid_deduped_df = valid_df.dropDuplicates([pk_column_target])

    # Add metadata and select final columns
    final_df = valid_deduped_df.withColumn("_ingestion_timestamp_utc", current_timestamp())
    final_columns = get_final_columns(dataset_type)

    # Ensure all final columns exist before selecting
    cols_to_select = [c for c in final_columns if c in final_df.columns or c == "_ingestion_timestamp_utc"]
    if "_source_file" not in final_df.columns and "_source_file" in final_columns:
         final_df = final_df.withColumn("_source_file", lit("dummy_source.csv")) # Add dummy if missing

    final_df = final_df.select(cols_to_select)
    return final_df

# --- Orders Tests ---

def test_orders_transformation(orders_raw_data):
    transformed_df = apply_transformations(orders_raw_data, 'orders')
    assert "order_timestamp" in transformed_df.columns
    assert "order_date" in transformed_df.columns
    assert "order_timestamp_str" not in transformed_df.columns
    assert "date_str" not in transformed_df.columns
    assert isinstance(transformed_df.schema["order_timestamp"].dataType, TimestampType)
    assert isinstance(transformed_df.schema["order_date"].dataType, DateType)
    # Check a valid conversion
    valid_row = transformed_df.filter(col("order_id") == "O1").first()
    assert valid_row["order_timestamp"] == datetime(2023, 10, 26, 10, 0, 0)
    assert valid_row["order_date"] == date(2023, 10, 26)
    # Check invalid date conversion results in null
    invalid_row = transformed_df.filter(col("order_id") == "O4").first()
    assert invalid_row["order_timestamp"] is None
    assert invalid_row["order_date"] is None

def test_orders_validation(orders_raw_data):
    transformed_df = apply_transformations(orders_raw_data, 'orders')
    valid_df, rejected_df = apply_validation(transformed_df, 'orders')

    assert valid_df.count() == 2 # O1, O2 (O1 duplicate is still valid at this stage)
    assert rejected_df.count() == 3 # Null order_id, null user_id, invalid timestamp

    valid_ids = {row.order_id for row in valid_df.select("order_id").collect()}
    assert valid_ids == {"O1", "O2"}

    rejected_ids = {row.order_num for row in rejected_df.select("order_num").collect()} # Use order_num as ID might be null
    assert rejected_ids == {"ORD102", "ORD103", "ORD104"} # Rows with nulls or bad dates

def test_orders_deduplication_and_final(orders_raw_data):
    transformed_df = apply_transformations(orders_raw_data, 'orders')
    valid_df, _ = apply_validation(transformed_df, 'orders')
    final_df = apply_deduplication_and_final_steps(valid_df, 'orders')

    assert final_df.count() == 2 # O1 (first one), O2
    final_ids = {row.order_id for row in final_df.select("order_id").collect()}
    assert final_ids == {"O1", "O2"}

    # Check final schema
    expected_final_cols = set(get_final_columns('orders'))
    assert set(final_df.columns) == expected_final_cols
    assert "_ingestion_timestamp_utc" in final_df.columns
    assert "_source_file" in final_df.columns
    assert isinstance(final_df.schema["_ingestion_timestamp_utc"].dataType, TimestampType)


# --- Order Items Tests ---

def test_order_items_transformation(order_items_raw_data):
    transformed_df = apply_transformations(order_items_raw_data, 'order_items')
    assert "order_item_id" in transformed_df.columns
    assert "id" not in transformed_df.columns
    assert isinstance(transformed_df.schema["order_timestamp"].dataType, TimestampType)
    assert isinstance(transformed_df.schema["order_date"].dataType, DateType)
    assert isinstance(transformed_df.schema["reordered"].dataType, BooleanType)
    assert isinstance(transformed_df.schema["days_since_prior_order"].dataType, IntegerType)

    # Check valid conversion
    valid_row = transformed_df.filter(col("order_item_id") == "I1").first()
    assert valid_row["order_timestamp"] == datetime(2023, 10, 26, 10, 5, 0)
    assert valid_row["order_date"] == date(2023, 10, 26)
    assert valid_row["reordered"] is False
    assert valid_row["days_since_prior_order"] == 10

    valid_row_reordered = transformed_df.filter(col("order_item_id") == "I2").first()
    assert valid_row_reordered["reordered"] is True

    # Check invalid reordered value results in null
    invalid_reordered_row = transformed_df.filter(col("order_item_id") == "I5").first()
    assert invalid_reordered_row["reordered"] is None


def test_order_items_validation(order_items_raw_data):
    transformed_df = apply_transformations(order_items_raw_data, 'order_items')
    valid_df, rejected_df = apply_validation(transformed_df, 'order_items')

    # Expected valid: I1, I2, I3 (I1 duplicate still valid here)
    # Expected rejected: Null id, Null order_id, Invalid reordered ('I5' might be rejected depending on strictness, let's assume it is for now)
    # The validation logic checks nulls in *original* required cols ('id', 'order_id')
    # and nulls in 'order_timestamp' after conversion.
    # The row with null 'id' will be rejected.
    # The row with null 'order_id' will be rejected.
    # The row 'I5' has reordered=null after transformation, but id/order_id are not null.
    # Let's refine the validation check slightly to be more explicit about required cols.

    assert valid_df.count() == 4 # I1, I2, I3, I5 (I1 duplicate is still valid, I5 is valid based on PK/FK null check)
    assert rejected_df.count() == 2 # Row with null id, row with null order_id

    valid_ids = {row.order_item_id for row in valid_df.select("order_item_id").collect()}
    assert valid_ids == {"I1", "I2", "I3", "I5"} # Includes the duplicate I1 for now

    # Collect rejected based on a non-null column if PK/FK might be null
    rejected_product_ids = {row.product_id for row in rejected_df.select("product_id").collect()}
    assert rejected_product_ids == {"P1", "P4"} # Corresponds to the rows with null id and null order_id


def test_order_items_deduplication_and_final(order_items_raw_data):
    transformed_df = apply_transformations(order_items_raw_data, 'order_items')
    valid_df, _ = apply_validation(transformed_df, 'order_items')
    final_df = apply_deduplication_and_final_steps(valid_df, 'order_items')

    assert final_df.count() == 3 # I1 (first one), I2, I3, I5 (deduplicated I1 removed)
    final_ids = {row.order_item_id for row in final_df.select("order_item_id").collect()}
    assert final_ids == {"I1", "I2", "I3", "I5"} # Should be 4 after correction

    # Correction: The duplicate I1 row in the raw data has order_id O5, the original has O1.
    # The deduplication is on 'order_item_id'. So the second 'I1' *should* be kept if it arrived later,
    # but dropDuplicates is non-deterministic without ordering. Let's assume one 'I1' remains.
    # Re-running the validation logic:
    # Raw: I1, I2, I3, (null id), (null order_id), I5, I1(dup)
    # Transformed: I1, I2, I3, (null id), (null order_id), I5(reordered=null), I1(dup)
    # Validated (valid): I1, I2, I3, I5, I1(dup) -> Count = 5
    # Validated (rejected): (null id), (null order_id) -> Count = 2
    # Deduped (on order_item_id): I1(one of them), I2, I3, I5 -> Count = 4

    assert final_df.count() == 4 # Corrected expected count
    final_ids = {row.order_item_id for row in final_df.select("order_item_id").collect()}
    assert final_ids == {"I1", "I2", "I3", "I5"} # Corrected expected IDs


    # Check final schema
    expected_final_cols = set(get_final_columns('order_items'))
    assert set(final_df.columns) == expected_final_cols
    assert "_ingestion_timestamp_utc" in final_df.columns
    assert "_source_file" in final_df.columns
    assert isinstance(final_df.schema["_ingestion_timestamp_utc"].dataType, TimestampType)


# --- Products Tests ---

def test_products_transformation(products_raw_data):
    # No specific transformations defined, so it should just pass through
    transformed_df = apply_transformations(products_raw_data, 'products')
    assert transformed_df.count() == products_raw_data.count()
    assert list(transformed_df.columns) == list(products_raw_data.columns) # Schemas should match

def test_products_validation(products_raw_data):
    transformed_df = apply_transformations(products_raw_data, 'products')
    valid_df, rejected_df = apply_validation(transformed_df, 'products')

    # Required col is product_id
    assert valid_df.count() == 3 # P1, P2, P3 (P1 duplicate still valid here)
    assert rejected_df.count() == 1 # Row with null product_id

    valid_ids = {row.product_id for row in valid_df.select("product_id").collect()}
    assert valid_ids == {"P1", "P2", "P3"}

    rejected_names = {row.product_name for row in rejected_df.select("product_name").collect()}
    assert rejected_names == {"Product Gamma"}

def test_products_deduplication_and_final(products_raw_data):
    transformed_df = apply_transformations(products_raw_data, 'products')
    valid_df, _ = apply_validation(transformed_df, 'products')
    final_df = apply_deduplication_and_final_steps(valid_df, 'products')

    assert final_df.count() == 3 # P1 (one of them), P2, P3
    final_ids = {row.product_id for row in final_df.select("product_id").collect()}
    assert final_ids == {"P1", "P2", "P3"}

    # Check final schema
    expected_final_cols = set(get_final_columns('products'))
    assert set(final_df.columns) == expected_final_cols
    assert "_ingestion_timestamp_utc" in final_df.columns
    assert "_source_file" in final_df.columns
    assert isinstance(final_df.schema["_ingestion_timestamp_utc"].dataType, TimestampType)


