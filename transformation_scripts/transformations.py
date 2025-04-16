import sys
import logging 
import boto3
import os
from urllib.parse import urlparse

# --- Importing necessary libraries ---
# Note: The following import statements are wrapped in a try-except block to handle environments where AWS Glue libraries are not available (e.g., local testing).
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_timestamp, date_format, lit, input_file_name, current_timestamp, when, to_date
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, BooleanType, DateType
    from delta.tables import DeltaTable
    modules_available = True
except ImportError as e:
    modules_available = False

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Starting Glue job...")

# --- Job Parameters ---
## @params: [JOB_NAME]
if modules_available == True:
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'dataset_type',
        'source_path',
        'target_path',
        'rejected_path' 
    ])

    dataset_type = args['dataset_type']
    source_path = args['source_path']
    target_path = args['target_path']
    rejected_path = args['rejected_path']
    job_name = args['JOB_NAME']

    logger.info(f"Job Name: {job_name}")
    logger.info(f"Dataset Type: {dataset_type}")
    logger.info(f"Source Path: {source_path}")
    logger.info(f"Target Path: {target_path}")
    logger.info(f"Rejected Path: {rejected_path}")


    # --- Glue and Spark Context Initialization ---
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(job_name, args)
    logger.info("Glue and Spark contexts initialized.")


    # --- Helper functions ---
    def get_schema_for_dataset(dataset_type):
        logger.debug(f"Getting schema for dataset type: {dataset_type}")
        if dataset_type == 'orders':
            return StructType([
                StructField("order_num", StringType(), True), StructField("order_id", StringType(), True),
                StructField("user_id", StringType(), True), StructField("order_timestamp_str", StringType(), True),
                StructField("total_amount", DecimalType(10, 2), True), StructField("date_str", StringType(), True)
            ])
        elif dataset_type == 'order_items':
            return StructType([
                StructField("id", StringType(), True), StructField("order_id", StringType(), True),
                
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True), StructField("add_to_cart_order", IntegerType(), True),
                StructField("reordered", StringType(), True), StructField("order_timestamp", StringType(), True),
                StructField("days_since_prior_order", StringType(), True)
                
            ])
        elif dataset_type == 'products':
            return StructType([ 
                StructField("product_id", StringType(), True), StructField("department_id", StringType(), True),
                StructField("department", StringType(), True), StructField("product_name", StringType(), True)
            ])
        else:
            logger.error(f"Unknown dataset type: {dataset_type}")
            raise ValueError(f"Unknown dataset type: {dataset_type}")

    def get_required_cols(dataset_type):
        logger.debug(f"Getting required columns for dataset type: {dataset_type}")
        if dataset_type == 'orders':
            return ["order_id", "user_id"]
        elif dataset_type == 'order_items':
            
            return ["id", "order_id"]
        elif dataset_type == 'products': 
            return ["product_id"]
        else:
            logger.warning(f"No required columns defined for dataset type: {dataset_type}")
            return []

    def get_pk_column(dataset_type):
        logger.debug(f"Getting primary key column for dataset type: {dataset_type}")
        if dataset_type == 'orders':
            return "order_id"
        elif dataset_type == 'order_items':
            return "order_item_id" 
        elif dataset_type == 'products': # Added products PK
            return "product_id"
        else:
            logger.error(f"No PK defined for {dataset_type}")
            raise ValueError(f"No PK defined for {dataset_type}")

    def get_final_columns(dataset_type):
        logger.debug(f"Getting final columns for dataset type: {dataset_type}")
        common_cols = ["_ingestion_timestamp_utc", "_source_file"] # Include source file
        if dataset_type == 'orders':
            return ["order_num", "order_id", "user_id", "total_amount", "order_timestamp", "order_date"] + common_cols
        elif dataset_type == 'order_items':
            # Ensure user_id is included if it's part of the schema
            return ["order_item_id", "order_id", "user_id", "product_id", "add_to_cart_order",
                    "reordered", "order_timestamp", "order_date", "days_since_prior_order"] + common_cols
        elif dataset_type == 'products': # Added products final cols
            return ["product_id", "department_id", "department", "product_name"] + common_cols
        else:
            logger.warning(f"No final columns defined for dataset type: {dataset_type}")
            return []

    def get_partition_column(dataset_type):
        logger.debug(f"Getting partition column for dataset type: {dataset_type}")
        if dataset_type in ['orders', 'order_items']:
            return "order_date"
        else: # No partitioning for products (or others)
            logger.info(f"No partition column defined for dataset type: {dataset_type}")
            return None

    # --- 1. Start Spark session (Delta Lake configured) ---
    # Note: GlueContext already provides a Spark session, but we reconfigure it for Delta
    logger.info("Configuring Spark session for Delta Lake...")
    spark = SparkSession.builder \
        .appName(f"PySpark Ingestion Pipeline - {dataset_type}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    logger.info("Spark session configured.")

    # --- 2. Read raw data ---
    logger.info(f"Reading raw data from: {source_path}")
    schema = get_schema_for_dataset(dataset_type)
    try:
        raw_df = spark.read.format("csv") \
            .option("header", "true") \
            .schema(schema) \
            .load(source_path) \
            .withColumn("_source_file", input_file_name())
        logger.info(f"Successfully read {raw_df.count()} records from source.")
        # Optional: Log schema if needed for debugging
        # logger.debug(f"Raw DataFrame schema: {raw_df.schema.simpleString()}")
    except Exception as e:
        logger.error(f"Error reading data from {source_path}: {str(e)}")
        raise e

    # --- 3. Basic transformation ---
    logger.info("Starting basic transformations...")
    transformed_df = raw_df
    if dataset_type == 'orders':
        transformed_df = transformed_df \
            .withColumn("order_timestamp", to_timestamp(col("order_timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("order_date", col("order_timestamp").cast(DateType())) \
            .drop("order_timestamp_str", "date_str")
        logger.info("Applied 'orders' transformations.")
    elif dataset_type == 'order_items':
        transformed_df = transformed_df \
            .withColumnRenamed("id", "order_item_id")\
            .withColumn("order_date", col("order_timestamp").cast(DateType()))\
            .withColumn("reordered",
            when(col("reordered") == 1, lit(True))
            .when(col("reordered") == 0, lit(False))
            .otherwise(lit(None)))
        transformed_df = transformed_df.withColumn(
        "order_timestamp",
        col("order_timestamp").cast("timestamp")  # try_cast in Spark
    )
        
        logger.info("Applied 'order_items' transformations.")
    elif dataset_type == 'products':
        transformed_df = transformed_df # Add any specific product transformations if needed (e.g., trimming whitespace)
        logger.info("Applied 'products' transformations (if any).")
    # Optional: Log schema after transformation
    logger.debug(f"Transformed DataFrame schema: {transformed_df.schema.simpleString()}")
    logger.info("Basic transformations completed.")


    # --- 4. Validation ---
    logger.info("Starting data validation...")
    required_cols_source = get_required_cols(dataset_type)
    pk_column_target = get_pk_column(dataset_type)
    logger.info(f"Required source columns: {required_cols_source}")
    logger.info(f"Target primary key column: {pk_column_target}")

    null_check_condition = None

    # Check original required columns before potential renames for simplicity
    for req_col in required_cols_source:
        if req_col in transformed_df.columns:
            cond = col(req_col).isNull()
            null_check_condition = cond if null_check_condition is None else null_check_condition | cond
            logger.debug(f"Added null check for column: {req_col}")
        # Handle case where required col might have been renamed (e.g. 'id' -> 'order_item_id')
        elif req_col == 'id' and dataset_type == 'order_items' and 'order_item_id' in transformed_df.columns:
            cond = col('order_item_id').isNull()
            null_check_condition = cond if null_check_condition is None else null_check_condition | cond
            logger.debug(f"Added null check for renamed column: order_item_id (original: {req_col})")
        else:
            logger.warning(f"Required column '{req_col}' not found in transformed DataFrame columns: {transformed_df.columns}. Skipping null check for it.")


    # Check timestamp validity AFTER conversion
    if 'order_timestamp' in transformed_df.columns:
        ts_cond = col('order_timestamp').isNull()
        null_check_condition = ts_cond if null_check_condition is None else null_check_condition | ts_cond
        logger.debug("Added null check for 'order_timestamp' column.")

    # Add more specific validation checks here if needed

    # Filter valid vs rejected
    if null_check_condition is not None:
        rejected_df = transformed_df.filter(null_check_condition) \
                                    .withColumn("rejection_reason", lit("Null key or invalid timestamp")) # TODO: Make reason more specific
        valid_df = transformed_df.filter(~null_check_condition)
        rejected_count = rejected_df.count()
        valid_count = valid_df.count()
        logger.info(f"Validation complete. Valid records: {valid_count}, Rejected records: {rejected_count}")
    else:
        logger.warning("No validation conditions were defined. Assuming all records are valid.")
        rejected_df = spark.createDataFrame([], transformed_df.schema.add("rejection_reason", StringType())) # Create empty DF with schema
        valid_df = transformed_df
        valid_count = valid_df.count()
        logger.info(f"Validation skipped. Valid records: {valid_count}, Rejected records: 0")


    # --- 5. Deduplication (within the batch) ---
    logger.info(f"Starting deduplication based on target PK: {pk_column_target}")
    valid_deduped_df = valid_df.dropDuplicates([pk_column_target])
    deduped_count = valid_deduped_df.count()
    duplicates_removed = valid_count - deduped_count
    logger.info(f"Deduplication complete. Records after deduplication: {deduped_count}. Duplicates removed in this batch: {duplicates_removed}")

    # --- 6. Add ingestion metadata & Select Final Columns ---
    logger.info("Adding ingestion metadata and selecting final columns.")
    final_df = valid_deduped_df.withColumn("_ingestion_timestamp_utc", current_timestamp())
    final_columns = get_final_columns(dataset_type)
    logger.info(f"Final columns selected: {final_columns}")
    final_df = final_df.select(final_columns)

    logger.debug(f"Final DataFrame schema: {final_df.schema.simpleString()}")

    # --- 7. Write rejected records ---
    # Check if rejected_df is empty before writing
    # Use isEmpty check which is generally more efficient than count() == 0 for just checking emptiness
    if not rejected_df.rdd.isEmpty():
        logger.info(f"Writing {rejected_count} rejected records to: {rejected_path}")
        try:
            rejected_df.write \
                .format("csv")\
                .option("header", "true") \
                .mode("append") \
                .save(rejected_path)
            logger.info("Successfully wrote rejected records.")
        except Exception as e:
            logger.error(f"Error writing rejected records to {rejected_path}: {str(e)}")
            # Decide if this should be a fatal error for the job
            # raise e # Uncomment if job should fail if rejected write fails
    else:
        logger.info("No rejected records to write.")

    # --- 8. Write or merge final data ---
    partition_col = get_partition_column(dataset_type)
    merge_options = {
        "mergeSchema": "true" # Allow schema evolution during merge
    }

    logger.info(f"Preparing to write/merge data to target Delta table: {target_path}")
    if partition_col:
        logger.info(f"Using partition column: {partition_col}")

    try:
        if not DeltaTable.isDeltaTable(spark, target_path):
            logger.info(f"Target path {target_path} is not a Delta table. Creating initial table.")
            writer = final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            if partition_col:
                writer = writer.partitionBy(partition_col)
            writer.save(target_path)
            logger.info(f"Successfully created and wrote initial data to Delta table at {target_path}")


            try:
                db_name = f"{dataset_type}db" # Replace with your actual database name if applicable
                logger.info(f"Registering Delta table {dataset_type} in database {db_name}...")
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                spark.sql(f"CREATE TABLE IF NOT EXISTS {db_name}.{dataset_type} USING DELTA LOCATION '{target_path}'")
                logger.info(f"Successfully registered table {db_name}.{dataset_type}")
            except Exception as e_sql:
                logger.warning(f"Could not register table in metastore: {str(e_sql)}")

        else:
            logger.info(f"Target path {target_path} is a Delta table. Merging data.")
            delta_table = DeltaTable.forPath(spark, target_path)
            merge_condition = f"target.{pk_column_target} = source.{pk_column_target}" # Use TARGET pk name

            logger.info(f"Executing merge on {target_path} with condition: {merge_condition}")
            logger.info(f"Merge options: {merge_options}")

            # Build the merge statement with schema evolution
            merge_builder = delta_table.alias("target").merge(
                final_df.alias("source"),
                condition=merge_condition
            )

            # Apply schema evolution option and define merge actions
            # Default behavior: Update all columns on match, insert all columns when not matched.
            # Customize .whenMatchedUpdate(...) or .whenNotMatchedInsert(...) if needed.
            merge_builder = merge_builder \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll()

            # Execute the merge with schema evolution enabled via options
            merge_builder.execute() # Pass option directly here

            logger.info(f"Successfully merged data into Delta table at {target_path}")

    except Exception as e:
        logger.error(f"Error processing data for {target_path}: {str(e)}", exc_info=True) # Log traceback
        # Re-raise the exception so Glue/Step Functions mark the job as failed
        raise e

    # --- 9. Archive processed files ---
    logger.info("Archiving processed files...")

    def move_files_to_archive(source_path, archive_path):
        s3 = boto3.resource('s3')

        parsed_source = urlparse(source_path)
        parsed_archive = urlparse(archive_path)

        source_bucket = parsed_source.netloc
        source_prefix = parsed_source.path.lstrip('/')

        archive_bucket = parsed_archive.netloc
        archive_prefix = parsed_archive.path.lstrip('/')

        source_bucket_obj = s3.Bucket(source_bucket)
        for obj_summary in source_bucket_obj.objects.filter(Prefix=source_prefix):
            source_key = obj_summary.key
            if not source_key.endswith("/"):  # Ignore folder placeholders
                archive_key = os.path.join(archive_prefix, os.path.basename(source_key))
                logger.info(f"Moving {source_key} to {archive_key}")
                # Copy to archive
                s3.Object(archive_bucket, archive_key).copy_from(CopySource={'Bucket': source_bucket, 'Key': source_key})
                # Delete original
                s3.Object(source_bucket, source_key).delete()
                logger.info(f"Archived and deleted: {source_key}")

    try:
        archive_path = source_path.replace("/raw/", "/archive/")  # Assumes 'raw' in path, change as needed
        move_files_to_archive(source_path, archive_path)
        logger.info(f"Successfully archived files from {source_path} to {archive_path}")
    except Exception as e:
        logger.error(f"Error archiving files from {source_path}: {str(e)}")


    # --- Job Commit ---
    logger.info("Committing Glue job.")
    job.commit()
    logger.info("Glue job finished successfully.")