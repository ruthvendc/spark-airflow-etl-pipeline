# Data Engineering Portfolio

Hi 👋, I'm **David Ruthven** — a senior IT professional pivoting into **Data Engineering**.

I bring **14+ years of experience** in Oracle database administration, data modeling, performance tuning, and systems architecture, and I’m now focused on building modern data pipelines using **Python, Spark, Airflow, and cloud-native tools**.

This repository (and the linked projects below) serve as a **hands-on portfolio** demonstrating real-world data engineering skills: ingestion, transformation, orchestration, data quality, and analytics-ready outputs.

---

## 🔧 Core Skills

- **Languages:** Python, SQL
- **Data Processing:** Apache Spark (PySpark), Spark SQL
- **Orchestration:** Apache Airflow
- **Storage & Formats:** Parquet, CSV, relational databases
- **Data Modeling:** Star schemas, normalized & analytical models
- **Cloud & Platforms:** Azure (Fundamentals), Oracle
- **Dev Practices:** Git, modular code, reproducible pipelines

---

## 📂 Featured Projects

### 1️⃣ End-to-End Data Engineering Pipeline (Synthetic Data)
**Tech:** Python, PySpark, Airflow, Parquet

**Overview:**
An end-to-end batch data pipeline that ingests raw CSV data, applies transformations using Spark, and produces analytics-ready datasets.

**What it Demonstrates:**
- Synthetic data generation for repeatable testing
- Spark-based transformations and aggregations
- Partitioned Parquet outputs
- Airflow DAG design and scheduling
- Separation of raw, staged, and curated data layers

📁 Repo: `data-engineering-pipeline`

---

### 2️⃣ Data Modeling & Analytics Project
**Tech:** SQL, Spark SQL

**Overview:**
Design and implementation of analytical datasets optimized for reporting and BI use cases.

**What it Demonstrates:**
- Fact and dimension table design
- Data quality checks
- Performance-aware transformations

📁 Repo: `analytics-data-model`

---

### 3️⃣ Python Data Utilities
**Tech:** Python

**Overview:**
A collection of reusable Python utilities for data ingestion, validation, and transformation.

**What it Demonstrates:**
- Clean, modular Python code
- Reusable data engineering helpers
- Logging and error handling

📁 Repo: `python-data-utils`

---

## 🧱 Architecture Patterns Used

- **Medallion Architecture** (Bronze / Silver / Gold)
- Batch-oriented ETL pipelines
- Idempotent data processing
- Schema evolution awareness

---

## 📊 Sample Workflow

1. Generate or ingest raw data (CSV)
2. Validate schema and data quality
3. Transform using Spark (PySpark / Spark SQL)
4. Write optimized Parquet outputs
5. Orchestrate pipeline execution with Airflow

---

## 🚀 How to Run Projects Locally

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run Spark job
spark-submit jobs/transform_data.py
```

> Each project repository contains its own detailed setup instructions.

---

## 🎯 Purpose of This Portfolio

This portfolio is designed to:
- Demonstrate **practical data engineering skills**
- Show a transition from traditional DBA work to **modern data platforms**
- Highlight clean design, scalability, and production-minded thinking

All datasets used are **synthetic or public** — no proprietary or employer-related data is included.

---

## 📫 Contact

- **GitHub:** https://github.com/your-username
- **LinkedIn:** https://www.linkedin.com/in/your-profile

---

⭐ If you’re reviewing this as part of an interview process, feel free to explore individual repositories for deeper technical detail, diagrams, and design notes.
