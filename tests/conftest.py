# conftest.py
import pytest
from pyspark.sql import SparkSession
import logging

# Suppress excessive Spark logging during tests
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

