# 🔐 Security Documentation

This document outlines the security architecture, practices, and configurations applied to the AWS Glue Delta Lake ingestion pipeline.

---

## 📚 Table of Contents

1. [Overview](#overview)
2. [IAM Policies](#iam-policies)
3. [S3 Bucket Policies](#s3-bucket-policies)
4. [Glue Job Security](#glue-job-security)
5. [Delta Lake Storage Security](#delta-lake-storage-security)
6. [Encryption](#encryption)
7. [Logging and Monitoring](#logging-and-monitoring)
8. [Secrets Management](#secrets-management)
9. [Access Controls](#access-controls)
10. [Compliance Considerations](#compliance-considerations)

---

## 1. Overview

This document describes the security measures implemented in the ingestion pipeline, including access controls, encryption, IAM roles, and secure data handling practices across AWS Glue, S3, and Delta Lake.

---

## 2. IAM Policies

### 🧾 IAM Role for Glue Job

| Resource         | Access Level       | Reason                                  |
|------------------|--------------------|-----------------------------------------|
| `s3://source/*`  | Read-only          | To read source CSV data                 |
| `s3://target/*`  | Write-only         | To write validated Delta tables         |
| `logs:*`         | Write              | For Glue job logging                    |
| `glue:*`         | Limited (Job only) | To allow job execution and monitoring   |

### Principle of Least Privilege

All IAM policies follow the principle of least privilege — only necessary permissions are granted to reduce the attack surface.

---

## 3. S3 Bucket Policies

### 🔐 Source Bucket (`source-bucket`)

- Read access restricted to Glue job IAM role
- Public access blocked

### 🔐 Target Bucket (`delta-lake-bucket`)

- Write access restricted to Glue job IAM role
- Enforced bucket policies for encryption at rest

---

## 4. Glue Job Security

- Glue job runs with an IAM role with scoped permissions.
- The job script is stored in S3 with restricted access.
- Temporary credentials are rotated by AWS automatically.

---

## 5. Delta Lake Storage Security

Delta tables are stored in S3:
- Encrypted at rest using AWS KMS (default SSE-KMS)
- Fine-grained access via S3 bucket policies

---

## 6. Encryption

| Type               | Method      | Description                                     |
|--------------------|-------------|-------------------------------------------------|
| At rest (S3)       | SSE-KMS     | Server-Side Encryption with AWS Key Management |
| In transit         | SSL/TLS     | Enforced by AWS S3 and Glue                    |

---

## 7. Logging and Monitoring

| Tool             | Purpose                       |
|------------------|-------------------------------|
| CloudWatch Logs  | Glue job logs and error output |
| AWS CloudTrail   | Auditing S3, Glue, IAM activity |
| Amazon S3 Events | Object-level monitoring        |

---

## 8. Secrets Management

- All secrets (e.g., database credentials) are stored in AWS Secrets Manager.
- Jobs fetch secrets using IAM-based secure retrieval.
- Secrets are rotated and version-controlled.

---

## 9. Access Controls

| Resource | Control Mechanism | Access |
|----------|-------------------|--------|
| Glue Job | IAM Role          | Developers, Data Engineers |
| Secrets  | Secrets Manager   | Read-only via IAM policies |
| S3       | Bucket Policies   | Role-specific access        |

---

## 10. Compliance Considerations

This pipeline can be aligned with:
- **GDPR**: Supports data lineage and access control
- **HIPAA**: Uses encryption and access logging
- **SOC 2**: Audit trails and principle of least privilege

---
