import pytest
from datetime import datetime
from decimal import Decimal
from transformation_scripts.transformations import transform_dataset, apply_validation
from pyspark.sql import SparkSession
from pyspark.sql import Row


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .master("local[*]") \
        .appName("TestETL") \
        .getOrCreate()


@pytest.fixture
def orders_raw_data(spark):
    data = [
        Row(id="1", user_id="101", product_id="1001", quantity=2, price=Decimal("10.5"), order_date="2023-10-26T10:00:00"),
        Row(id="2", user_id=None, product_id="1002", quantity=1, price=Decimal("15.0"), order_date="2023-10-27T14:30:00"),
        Row(id="3", user_id="103", product_id="1003", quantity=None, price=Decimal("20.0"), order_date="2023-10-28T09:45:00"),
        Row(id="1", user_id="101", product_id="1001", quantity=2, price=Decimal("10.5"), order_date="2023-10-26T10:00:00")  # Duplicate
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def order_items_raw_data(spark):
    data = [
        Row(id="1", order_id="201", product_id="1001", quantity=1, price=Decimal("10.5"), reordered="1"),
        Row(id="2", order_id="202", product_id="1002", quantity=2, price=Decimal("15.0"), reordered="0"),
        Row(id="3", order_id=None, product_id="1003", quantity=1, price=Decimal("20.0"), reordered="1"),  # Missing order_id
        Row(id="4", order_id="204", product_id="1004", quantity=None, price=Decimal("25.0"), reordered="9"),  # Invalid reordered
        Row(id="1", order_id="201", product_id="1001", quantity=1, price=Decimal("10.5"), reordered="1")  # Duplicate
    ]
    return spark.createDataFrame(data)


def test_orders_transformation(orders_raw_data):
    transformed_df = transform_dataset("orders", orders_raw_data)
    transformed_df.show(truncate=False)

    # Ensure 'order_id' exists, 'id' does not
    assert "order_id" in transformed_df.columns
    assert "id" not in transformed_df.columns

    # Check timestamp transformation
    sample = transformed_df.filter("order_id = '1'").collect()[0]
    assert sample["order_timestamp"].strftime("%Y-%m-%d %H:%M:%S") == "2023-10-26 10:00:00"


def test_orders_validation_and_deduplication(orders_raw_data):
    transformed_df = transform_dataset("orders", orders_raw_data)
    valid_df, rejected_df = apply_validation("orders", transformed_df)

    valid_df.show(truncate=False)
    rejected_df.show(truncate=False)

    # Valid: records 1 and 2 (record 3 has null quantity, record 1 duplicate removed)
    assert valid_df.count() == 2
    assert rejected_df.count() == 2  # One with null, one duplicate

    # Check enrichment metadata
    for row in valid_df.collect():
        assert row["ingestion_timestamp"] is not None
        assert row["source_file"] == "source_file_name.csv"


def test_order_items_transformation(order_items_raw_data):
    transformed_df = transform_dataset("order_items", order_items_raw_data)
    transformed_df.show(truncate=False)

    assert "order_item_id" in transformed_df.columns
    assert "id" not in transformed_df.columns

    # Check cast of 'reordered'
    sample = transformed_df.filter("order_item_id = '1'").collect()[0]
    assert isinstance(sample["reordered"], int)
    assert sample["reordered"] == 1


def test_order_items_validation_and_deduplication(order_items_raw_data):
    transformed_df = transform_dataset("order_items", order_items_raw_data)
    valid_df, rejected_df = apply_validation("order_items", transformed_df)

    valid_df.show(truncate=False)
    rejected_df.show(truncate=False)

    # Valid: records 1 and 2
    assert valid_df.count() == 2
    assert rejected_df.count() == 3  # Missing order_id, invalid 'reordered', duplicate

    for row in valid_df.collect():
        assert row["ingestion_timestamp"] is not None
        assert row["source_file"] == "source_file_name.csv"


