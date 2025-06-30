
# 🌊 Lakehouse Flow: Building Scalable Pipelines with Databricks & DBT

An end-to-end data engineering project that demonstrates how to architect a modern data lakehouse using **Databricks**, **Spark Structured Streaming**, **Auto Loader**, and **dbt**. This project showcases best practices from ingestion to the Silver layer using **LakeFlow Declarative Pipelines**, with plans to implement the Gold layer using dimensional modeling, SCDs, and automated fact builders.

---

## 🧱 Architecture Overview

### ✅ Implemented
- **Incremental ingestion** with Spark Streaming & Auto Loader
- **Parameterized pipeline control** using Databricks Jobs
- **LakeFlow (DLT) declarative pipelines** for the Silver layer
- **Managed volumes and schema separation** for Bronze, Silver, and Gold layers
- **Path-driven, modularized ingestion logic**

### 🏗️ In Progress
- Dimensional modeling (Star Schema)
- Slowly Changing Dimensions (SCD Type 2)
- Automated Fact Builders (fact table creation from dimensions)
- Integration with **dbt** for Gold layer modeling and documentation

---

## 📂 Project Structure

```
📁 notebooks/
│   ├── 00_setup_environment.py
│   ├── 01_bronze_ingestion_autoloader.py
│   ├── 02_silver_layer_dlt.py
│
📁 dlt/
│   └── dlt_silver_layer.py         # Production-grade DLT logic
│
📁 dbt/
│   ├── models/
│   └── dbt_project.yml
│
📁 data/                            # Uploaded via Databricks Volume UI
│
📁 images/
│   └── architecture-diagram.png
```

---

## 🏗️ Setup & Environment

### ⚙️ 1. Create Managed Volumes & Schemas

Use a setup notebook or SQL editor to create the volume structure:

```sql
-- Create managed volumes (stored in AWS S3 under the hood)
CREATE SCHEMA workspace.rawschema;
CREATE VOLUME workspace.rawschema.rawvolume;

-- Prepare folders for each source
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/bookings")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/flights")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/airports")
dbutils.fs.mkdirs("/Volumes/workspace/rawschema/rawvolume/rawdata/customers")

-- Create additional schema-volume layers
CREATE SCHEMA workspace.bronze;
CREATE VOLUME workspace.bronze.bronzevolume;

CREATE SCHEMA workspace.silver;
CREATE VOLUME workspace.silver.silvervolume;

CREATE SCHEMA workspace.gold;
CREATE VOLUME workspace.gold.goldvolume;
```

> 📥 Upload raw CSV/Parquet data to the corresponding folders in the `rawvolume`.

---

## ⚡ Bronze Layer: Incremental Streaming Ingestion

- Implemented with **Databricks Auto Loader** and **Spark Structured Streaming**
- Ingestion notebooks are **parameterized** via arrays and control flow
- Supports incremental file loading (ideal for real-time or micro-batch data)

**Notebook**: `01_bronze_ingestion_autoloader.py`

---

## 🧪 Silver Layer: DLT (LakeFlow Declarative Pipelines)

We use **DLT pipelines** to handle transformations at scale using declarative logic:

- Source-controlled with `.py` script (`dlt_silver_layer.py`)
- Uses the `@dlt.table` decorator for modular logic
- Incorporates validation rules and transformations via **dictionary-driven logic**
- Supports CDC and dry-runs for DAG creation and error tracing

```python
@dlt.table(
    comment="Cleaned and enriched flight bookings data",
)
def silver_bookings():
    return (
        spark.read.table("workspace.bronze.bookings")
        .filter("status IS NOT NULL")
        .withColumn("booking_date", to_date("booking_time"))
    )
```

---

## 📊 Gold Layer: [Coming Soon]

Planned features for the Gold layer:

- **Star schema modeling** (Fact & Dimension separation)
- **Slowly Changing Dimensions (SCD Type 2)** using DLT Change Data Capture
- **Automatic Fact Table Builders** driven by metadata mapping
- **Integration with `dbt`** to:
  - Document models
  - Generate lineage graphs
  - Manage testing and deployment

---

## 🔗 dbt + Databricks Integration (Planned)

This project will integrate `dbt` for the Gold layer by:

- Connecting dbt to Unity Catalog
- Defining models and documentation in YAML
- Building the Gold layer in SQL using dbt's transformation framework

---

## 🚀 How to Run

1. **Clone the project** and upload the setup notebook to Databricks.
2. **Create managed volumes and schemas** (see Setup section).
3. **Upload raw datasets** to the appropriate folders in `rawvolume`.
4. **Run Bronze ingestion notebook** to test Auto Loader streams.
5. **Configure and run LakeFlow DLT pipeline** using `dlt_silver_layer.py`.
6. (**Optional**) Upload additional incremental files and re-run the stream.
7. (**Future**) Proceed to build Gold layer models with dbt and DLT.

---

## 📌 Notes

- Databricks automatically uses **AWS S3 buckets** under the hood for managed volumes.
- External volumes can be mounted for BYOL (Bring Your Own Lake) scenarios, but managed volumes simplify governance.
- Always run **DLT in dry-run mode first** to validate your DAG and catch schema issues early.

---

## 📸 Architecture Diagram

![Architecture](./images/architecture-diagram.png)

---

## 📅 Roadmap

| Layer        | Status         | Notes                                       |
|--------------|----------------|---------------------------------------------|
| Raw/Bronze   | ✅ Completed    | Ingested with Auto Loader (incremental)     |
| Silver       | ✅ Completed    | Declarative pipelines with DLT              |
| Gold         | 🚧 In Progress  | Dimensional modeling, dbt integration       |
| dbt Models   | 🔜 Not Started  | Star schema, SCDs, Fact builders            |

---

## 🧠 Tech Stack

- **Databricks**
- **Apache Spark** (Structured Streaming)
- **Delta Live Tables (DLT) / LakeFlow**
- **Databricks Auto Loader**
- **dbt** (Data Build Tool)

---

## 📬 Contact

For any questions, feel free to reach out via GitHub issues or open a discussion.
