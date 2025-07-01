
# 🌊 Lakehouse Flow: Building Scalable Pipelines with Databricks & DBT

An end-to-end data engineering project that demonstrates how to architect a modern data lakehouse using **Databricks**, **Spark Structured Streaming**, **Auto Loader**, and **dbt**. This project showcases best practices from ingestion to the Silver layer using **LakeFlow Declarative Pipelines**, with a dynamic and modular implementation of the Gold layer using surrogate keys, CDC, and automated fact-dimension pipelines.

---

## 🧱 Architecture Overview

### ✅ Implemented
- **Incremental ingestion** with Spark Streaming & Auto Loader
- **Parameterized pipeline control** using Databricks Jobs
- **LakeFlow (DLT) declarative pipelines** for the Silver layer
- **Managed volumes and schema separation** for Bronze, Silver, and Gold layers
- **Path-driven, modularized ingestion logic**
- **Dynamic Fact & Dimension pipelines** in Gold layer

### 🏗️ In Progress
- dbt integration and version-controlled data modeling
- Documentation, testing, and lineage with dbt
- Multi-threading optimization for large scale processing

---

## 📂 Project Structure

```
📁 notebooks/
│   ├── 00_setup_environment.py
│   ├── 01_bronze_ingestion_autoloader.py
│   ├── 02_silver_layer_dlt.py
│   ├── gold_layer_dimensions.py
│   └── gold_fact.py
│
📁 dlt/
│   └── dlt_silver_layer.py
│
📁 dbt/
│   ├── models/
│   └── dbt_project.yml
│
📁 images/
│   └── architecture-diagram.png
```

---

## 🏗️ Setup & Environment

### ⚙️ Create Managed Volumes & Schemas

Use a setup notebook to execute the following:
```sql
CREATE SCHEMA workspace.rawschema;
CREATE VOLUME workspace.rawschema.rawvolume;

-- Prepare folder structure in volume
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/bookings")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema.rawvolume/rawdata/flights")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema.rawvolume/rawdata/airports")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema.rawvolume/rawdata/customers")

-- Bronze, Silver, Gold layers
CREATE SCHEMA workspace.bronze;
CREATE VOLUME workspace.bronze.bronzevolume;

CREATE SCHEMA workspace.silver;
CREATE VOLUME workspace.silver.silvervolume;

CREATE SCHEMA workspace.gold;
CREATE VOLUME workspace.gold.goldvolume;
```

---

## ⚡ Bronze Layer: Auto Loader Streaming

- Ingest data using **Databricks Auto Loader** for scalable file ingestion.
- Control flow uses array-driven logic to dynamically load various tables.
- Ingestion is incremental using **Structured Streaming** and `cloudFiles`.

---

## 🧪 Silver Layer: LakeFlow DLT Pipelines

- Implemented using `@dlt.table` decorators in Python modules.
- Transforms and enriches Bronze data for analytical usability.
- CDC logic and schema validation are performed through LakeFlow DAGs.
- Uses dictionaries to define transformation logic per table.

---

## ✨ Gold Layer: Dimensional Modeling with Dynamic Pipelines

### Overview
At the Gold layer, we construct a **Star Schema** using both dimension and fact tables with incremental and dynamic PySpark logic.

### 🧩 Dimension Table Creation (`gold_layer_dimensions.py`)

- Surrogate keys are generated using `monotonically_increasing_id()`.
- Incremental changes are fetched based on a `modifiedDate` column.
- Pipeline is fully parameterized using:
  - List comprehensions for multiple dimension creation.
  - Reusable code blocks and dynamic joins.
- Records are merged using `DeltaTable.merge()`.

### 📈 Fact Table Construction (`gold_fact.py`)

- Dynamically builds `FactBookings` by joining all dimension tables.
- Uses a function to build SQL JOIN logic for scalability.
- CDC is used to determine new vs existing records.
- Joins are executed based on configured `join_key` definitions per dimension.
- Automatically inserts surrogate keys into the final fact table.

---

## 🧩 dbt Integration (Planned)

- Connect to Databricks SQL endpoint via `profiles.yml`
- Use version control and modular dbt models to query and document Gold layer tables.
- Run tests, generate docs, and build model dependencies with `dbt run`, `dbt test`, and `dbt docs`.

---

## 🧪 Data Domain Summary

This project analyzes and connects the following datasets:
- **Passengers**: Personal data and demographics.
- **Airports**: Geolocation and operational metadata.
- **Flights**: Time, location, and aircraft routes.
- **Bookings**: Financials and passenger activity.

All entities flow from Raw → Bronze → Silver → Gold layers with incremental, scalable transformation logic.

---

## 🧠 Tech Stack

- **Databricks (Unity Catalog, SQL Warehouse)**
- **Apache Spark** (Streaming, Structured)
- **Delta Lake + DLT (LakeFlow)**
- **dbt (planned for modeling)**

---

## 🚀 How to Run

1. **Upload raw files** into the defined volume directories.
2. **Run Bronze notebook** to stream raw data into Delta tables.
3. **Run Silver DLT pipeline** to curate enriched data.
4. **Run Gold layer dimension notebook** (`gold_layer_dimensions.py`).
5. **Run Fact notebook** (`gold_fact.py`) to generate `FactBookings`.
6. (**Optional**) Connect dbt to the SQL endpoint for model development.

---

## 📸 Architecture Diagram

![Architecture](./images/architecture-diagram.png)

---

## 📬 Contact

For questions or issues, please open a GitHub issue or contact the maintainer.
