# 💳 FraudShield: Smart Credit Card Fraud Detection Pipeline

## 📌 Objective
The goal of this project is to **detect potential credit card frauds** by building an automated data pipeline using **Apache Spark**, **Google Cloud Platform (GCP)** services like **BigQuery**, **Cloud Storage**, **Cloud Composer (Airflow)**, and **GitHub Actions**. It ensures timely and accurate processing of daily transaction data and identifies risky activities based on defined business rules.

---

## 🚀 Project Overview

This pipeline automates the **ingestion, transformation, risk analysis**, and **storage** of daily credit card transaction data to flag potentially fraudulent activity.

---

## 🗃️ Data Sources

### BigQuery Static Table (`cardholders`)
Contains credit card holder details:
- `card_no`
- `card_type`
- `cust_name`
- `card_limit`
- `points`

### Google Cloud Storage (GCS)
Stores **daily transaction JSON files** (e.g., `transactions_20250413.json`)

---

## 🔧 Spark Job Processing Logic

1. **Read Input Data**
   - Static cardholder table from **BigQuery**
   - Daily transaction JSON files from **GCS**

2. **Transformations on Transactions**
   - Filter invalid data: `amount > 0`, `card_no is not null`
   - Compute `transaction_category`:
     - `amount < 100` → **High**
     - `100 ≤ amount < 500` → **Medium**
     - `amount ≥ 500` → **Low**
   - Compute `high_risk` (True if any condition below is met):
     - `transaction_category = High`
     - `fraud_flag = True`
     - `amount > 100000`

3. **Join with Cardholders Table**
   - On `card_no`

4. **Compute `fraud_risk_level`**
   - `Critical`: if `high_risk = True`
   - `High`: if `risk_score > 0.3` or `fraud_flag = True`
   - `Low`: otherwise

5. **Write Final Output to BigQuery**
   - Table name: `transactions`

---

## ✅ Testing with Pytest

- Used `pytest` for validating data transformation and fraud detection logic.
- Sample test cases are created to verify each business rule.

---

## ⏰ Workflow Orchestration with Airflow (Cloud Composer)

### DAG Tasks:

1. **File Check**
   - `GCSObjectExistenceOperator`: Checks if `transactions_` file is present in GCS.

2. **Execute Spark Job**
   - `DataprocCreateBatchOperator`: Runs the Spark job on **Dataproc Serverless** using predefined config and environment.

3. **Archive Processed File**
   - `GCSToGCSOperator`: Moves the processed JSON file from the transaction bucket to an archive bucket.

4. **Task Flow**
   - Tasks are executed in sequence and configured using appropriate trigger rules.

---

## 🔄 CI/CD with GitHub Actions

- Two environments:
  - `dev`: for testing the Spark and Airflow jobs
  - `prod`: for production deployment after successful tests
- Workflow:
  - Merge triggers deployment of Spark and Airflow code to specific GCS paths.

---

## ✅ Final Output

After successful execution:
- BigQuery table `transactions` is updated with cleaned and enriched data.
- Each record includes a **fraud risk level** to highlight suspicious activities.

---

## 📌 Summary

**FraudShield** provides a **robust, automated, and scalable fraud detection system** using GCP and Spark. The project ensures secure, fast, and reliable identification of risky transactions, empowering businesses to react swiftly to fraudulent activities.

