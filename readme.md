# 🚀 AWS Glue Delta Lake Ingestion Pipeline

A scalable, schema-evolving data ingestion pipeline that processes raw CSV data from Amazon S3, performs validation, transformation, deduplication, and metadata enrichment, and ingests it into Delta Lake tables using AWS Glue and PySpark.

---

## 📚 Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Supported Datasets](#supported-datasets)
- [How It Works](#how-it-works)
- [Setup and Deployment](#setup-and-deployment)
- [Security](#security)
- [Monitoring and Logging](#monitoring-and-logging)
- [Contributing](#contributing)
- [License](#license)

---

## ✨ Features

- 📥 **Source-Agnostic**: Reads CSV files from S3 dynamically.
- ✅ **Schema Inference & Validation**: Ensures data integrity.
- 🧠 **Metadata Enrichment**: Adds ingestion context like timestamp and source path.
- 🔄 **Merge / Append / Overwrite**: Conditional write logic based on dataset.
- 🧹 **Rejection Handling**: Invalid rows are logged and stored separately.
- 💾 **Delta Lake Storage**: ACID-compliant tables stored on S3.
- 🔐 **Secure by Design**: IAM, encryption, and access policies enforced.

---

## 🏗️ Architecture Overview

```text
         +---------------------+
         |     Source S3       |
         |  (CSV Files in S3)  |
         +----------+----------+
                    |
                    v
        +-----------+-----------+
        |     AWS Glue (Job)    |
        | PySpark Transformation|
        +-----------+-----------+
                    |
          +---------+----------+
          | Delta Lake on S3   |
          +--------------------+
```

---

## 🧰 Tech Stack

- **AWS Glue** (PySpark ETL jobs)
- **Amazon S3** (input, output, rejected storage)
- **Delta Lake** (ACID table format on S3)
- **Apache Spark**
- **AWS IAM, KMS, CloudWatch**

---

## 📦 Supported Datasets

| Dataset       | Key Column   | Write Mode  |
|---------------|--------------|-------------|
| `orders`      | `order_id`   | Merge       |
| `order_items` | None         | Append      |
| `products`    | None         | Overwrite   |

---

## ⚙️ How It Works

1. Glue job is triggered.
2. It loads all `.csv` files from the S3 source bucket/folder.
3. Infers schema and validates rows.
4. Deduplicates and enriches data.
5. Writes output to Delta Lake using appropriate write mode:
   - Merge (if `key_column` provided)
   - Overwrite or Append otherwise
6. Rejected rows are saved to `rejected/` folder with error reasons.

---

## 🚀 Setup and Deployment

### Prerequisites

- AWS Account with Glue and S3 access
- Python 3.x for local development
- Delta Lake JARs available to Glue job (or configure AWS Glue 4.0)

### Deploying Glue Job

1. Upload `ingest_pipeline.py` to an S3 location.
2. Create a new Glue job and configure the script path.
3. Set IAM role with access to:
   - S3 source and target buckets
   - Glue service
   - CloudWatch Logs
4. (Optional) Configure job parameters in `config/job_parameters.json`.

### Running the Job

Via console or scheduled Glue trigger.

---

## 🔐 Security

- IAM roles follow least privilege principle.
- S3 buckets are encrypted with SSE-KMS.
- Glue job uses encrypted temporary storage.
- Secrets managed using AWS Secrets Manager.
- Full security breakdown available in [`docs/SECURITY.md`](docs/SECURITY.md).

---

## 📊 Monitoring and Logging

| Tool            | Use Case                     |
|------------------|------------------------------|
| AWS CloudWatch   | Logs and error tracing       |
| Glue Console     | Job metrics and execution    |
| S3 Event Logging | Optional object-level audit  |

---

## 🤝 Contributing

Contributions are welcome! Please fork the repo and submit a pull request. Make sure your changes are covered with tests and linted before submission.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---