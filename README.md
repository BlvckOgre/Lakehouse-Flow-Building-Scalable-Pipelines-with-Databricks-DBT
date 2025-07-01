# 🚀 Databricks Modular Data Lakehouse

This project is an **end-to-end modular data engineering pipeline** built with **Apache Spark**, **Databricks**, **Delta Lake**, and **dbt**, designed to support structured, real-time and historical analysis on aviation-related datasets: **bookings**, **flights**, **airports**, and **passengers**.

Currently, the pipeline is built up to the **Silver Layer**. The **Gold Layer** with advanced star-schema modeling and dbt integrations is in-progress.

---

## 🌐 Architecture Overview

### Lakehouse Layers:

1. **Raw (External/Managed Volumes)**
   - Upload and organize raw datasets in Databricks-managed volumes (S3 backend).

2. **Bronze Layer**
   - Ingests raw data incrementally using **Spark Structured Streaming** via **Auto Loader**.
   - Paths, schemas, and control flow are parameterized for reuse across datasets.

3. **Silver Layer**
   - Built using **Lakehouse Flow (DLT)** declarative pipelines.
   - Transforms bronze data using dictionary-driven logic and DLT decorators (`@dlt.table`).
   - Enables schema evolution and CDC (Change Data Capture).

4. **Gold Layer**
   - Star schema with **dimension** and **fact** tables.
   - Incorporates Slowly Changing Dimensions (Type 1/2).
   - Automates surrogate key generation.
   - Dynamic logic for reusable dimension and fact generation.

---

## 📝 Setup Instructions

### ✏️ Volume & Schema Creation
```sql
-- Create raw managed volume and subfolders
CREATE VOLUME workspace.rawschema.rawvolume;
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/bookings");
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/flights");
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/airports");
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/customers");

-- Create schemas and volumes for lakehouse layers
CREATE SCHEMA workspace.bronze;
CREATE VOLUME workspace.bronze.bronzevolume;

CREATE SCHEMA workspace.silver;
CREATE VOLUME workspace.silver.silvervolume;

CREATE SCHEMA workspace.gold;
CREATE VOLUME workspace.gold.goldvolume;
```

---

## 🚀 Bronze Layer (Auto Loader Ingestion)

- Built using Spark Structured Streaming + Auto Loader.
- Paths, file formats, and schema hints are handled dynamically using an array-based control flow.
- Supports ingestion from managed volumes into Delta Lake format.

---

## 🚀 Silver Layer (Lakehouse Flow Pipelines)

Implemented using **DLT Python scripts**, not notebooks, for better modularity and CI/CD readiness.

Key Features:
- **@dlt.table** decorators define table logic.
- A central **transformation dictionary** is passed to dynamically control table creation logic.
- Enables quick extension and maintenance.
- Supports **dry runs** to pre-check DAG build and error validation.

---

## 📊 Gold Layer 

### ✨ Dimension Tables

- Each dimension (e.g., `DimFlights`, `DimAirports`, etc.) uses a consistent pattern:
  - Dynamic handling of keys and columns.
  - CDC filtering based on `modifiedDate`.
  - Surrogate key generation using `monotonically_increasing_id()`.
  - Merge into existing Delta Tables via `DeltaTable.merge()`.

### 📈 Fact Table

- A dynamic function builds the SQL query to join dimensions onto the base fact.
- Columns are dynamically selected.
- Dimensions are joined via configurable `join_key` metadata.
- Result is upserted into `FactBookings` using merge logic.

---

## 🔗 dbt Integration 

- dbt will connect to the **Databricks SQL Warehouse** endpoint.
- dbt models will be built on top of Gold Layer tables.
- Enables version control, lineage tracking, and testing via dbt YAML.

---

## 📚 Notebooks Summary

### Bronze Layer: `bronze_loader.py`
- Uses control flow over a list of tables
- Auto Loader reads from `rawvolume` and writes to `bronzevolume`

### Silver Layer: `dlt_silver_layer.py`
- Transforms bronze to silver
- Uses dictionary-driven logic for maintainability

### Gold Layer: `dim_dynamic_builder.py`
- Dynamically builds all dimension tables
- Uses CDC, surrogate keys, and Spark SQL merges

### Gold Layer: `fact_dynamic_builder.py`
- Dynamically builds fact table by joining to dimensions
- Supports incremental refresh via `modifiedDate`

---

## 💎 Technologies Used

| Component | Purpose |
|----------|---------|
| **Databricks** | Unified platform for Spark + DLT + SQL |
| **Auto Loader** | Scalable file ingestion with schema evolution |
| **DLT (LakeFlow)** | Declarative transformation pipelines |
| **Delta Lake** | Incremental processing with ACID guarantees |
| **dbt** | Model governance, lineage, testing  |

---

## 🚫 Still To Do

- Finalize and validate Gold Layer notebooks.
- Configure and run dbt against Databricks SQL endpoint.
- Enable CI/CD via GitHub Actions or Azure DevOps.
- Add quality checks (e.g., Great Expectations, dbt tests).

---

## ✨ Outcome

By the end of this project, we'll have a fully operational, modular, and maintainable lakehouse pipeline capable of processing aviation records with full support for schema management, historical change tracking, and automated model generation.

---

