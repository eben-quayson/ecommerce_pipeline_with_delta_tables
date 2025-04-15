## 🏗️ README: Architecture

```markdown
# 🏗️ Architecture Documentation

This document provides a detailed overview of the data ingestion pipeline architecture using AWS Glue and Delta Lake.

---

## 📚 Table of Contents

1. [Overview](#overview)
2. [High-Level Architecture Diagram](#high-level-architecture-diagram)
3. [Components](#components)
4. [Data Flow](#data-flow)
5. [Schema Inference and Validation](#schema-inference-and-validation)
6. [Metadata Enrichment](#metadata-enrichment)
7. [Delta Lake Write Modes](#delta-lake-write-modes)
8. [Rejection Handling](#rejection-handling)
9. [Orchestration](#orchestration)
10. [Monitoring](#monitoring)

---

## 1. Overview

This ingestion pipeline processes raw CSV data from Amazon S3, validates it, deduplicates it, and ingests it into Delta Lake (stored on S3) using Apache Spark on AWS Glue.

---

## 2. High-Level Architecture Diagram

```
            +---------------+
            |  Source S3    |
            |  (CSV Files)  |
            +------+--------+
                   |
                   v
        +----------+-----------+
        |  AWS Glue Job (PySpark) |
        +----------+-----------+
                   |
                   v
         +---------+---------+
         |   Delta Lake on S3 |
         +--------------------+
```

---

## 3. Components

| Component        | Description |
|------------------|-------------|
| AWS S3           | Source and target storage |
| AWS Glue         | Serverless ETL engine     |
| Delta Lake       | ACID-compliant storage layer |
| PySpark          | Transformation engine     |
| IAM Roles        | Access and permissions control |
| CloudWatch       | Monitoring and logging    |

---

## 4. Data Flow

1. Glue job triggered manually or via schedule.
2. Reads CSV files from the source path in S3.
3. Infers schema and performs validation.
4. Deduplicates and adds metadata columns.
5. Writes clean data to Delta Lake.
6. Invalid records (if any) written to `rejected/` path.

---

## 5. Schema Inference and Validation

- Schema inferred from source files using Spark’s `read.option("inferSchema", "true")`.
- Validation rules applied based on dataset type:
  - Mandatory fields
  - Type checks
  - Value range enforcement
- Invalid rows captured and written to a rejection path.

---

## 6. Metadata Enrichment

Each record is enriched with the following:

| Column          | Description                              |
|------------------|------------------------------------------|
| `ingestion_timestamp` | Time of ingestion into pipeline        |
| `source_path`         | Original S3 path of input file         |
| `source_bucket`       | Extracted S3 bucket name               |
| `source_file_name`    | Extracted file name from path          |

---

## 7. Delta Lake Write Modes

| Dataset        | Write Mode  | Description                          |
|----------------|-------------|--------------------------------------|
| `orders`       | Merge       | Upsert based on `order_id`           |
| `products`     | Overwrite   | Full refresh                         |
| `order_items`  | Append      | New records appended without merge   |

---

## 8. Rejection Handling

Invalid records are:
- Collected into a DataFrame during validation.
- Written to S3 under a `rejected/<dataset_type>/` prefix.
- Include a `rejection_reason` column for debugging.

---

## 9. Orchestration

Orchestration handled via:
- AWS Glue Triggers (optional)
- AWS Step Functions (in advanced setups)
- EventsBridge to trigger Step Function

---

## 10. Monitoring

| Tool             | Description |
|------------------|-------------|
| CloudWatch Logs  | Real-time logs from Glue jobs |
| S3 Events        | Optional triggers on object creation |
| Glue Console     | Job history, metrics, failures |

---


