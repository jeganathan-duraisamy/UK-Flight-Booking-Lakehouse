#  UK-Flight-Booking-Lakehouse: Data Engineering Project

##  Overview

This project implements a complete **end-to-end data engineering pipeline** for UK property and flight booking data using **Databricks**, **Delta Lake**, and **DLT (Delta Live Tables)**. The solution demonstrates best practices in data warehousing, transformation, and analytics using the modern **medallion architecture** (Bronze → Silver → Gold).

The pipeline processes bookings, flights, passengers, and airport data through incremental loading, **Slowly Changing Dimensions (SCD Type 2)**, and creates analytics-ready **star schema** datasets.

##  Architecture

### Data Flow

```
Raw CSV Data (S3/Blob) → Databricks → Bronze Layer → Silver Layer → Gold Layer
                         (Autoloader)    (Raw)      (Streaming)   (Analytics)
                                          ↓            ↓              ↓
                                      Delta Tables  DLT Tables    Star Schema
```

### Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Cloud Warehouse** | Azure Databricks (Free Edition) | Unified analytics platform |
| **Storage** | Azure Data Lake Storage Gen2 | Raw data landing zone |
| **Ingestion** | Autoloader (Cloud Files API) | Streaming file detection |
| **Processing** | PySpark + SQL | Data transformations |
| **Streaming** | Delta Live Tables (DLT) | Declarative transformations |
| **Format** | Delta Lake | ACID transactions & time travel |
| **Governance** | Unity Catalog | Schema-level access control |
| **Version Control** | GitHub | Code management |

## Data Model

### Medallion Architecture

#### **Bronze Layer** (Raw Data)

Immutable raw data ingestion with zero transformations:

- `bronze.flights` - Raw flight information (110 records)
- `bronze.passengers` - Raw passenger data (235 records)
- `bronze.airports` - Raw airport information (55 records)
- `bronze.bookings` - Raw booking transactions (1.3K records)

**Key Features:**
- ✅ Autoloader with checkpoint-based streaming
- ✅ Schema evolution (RESCUE mode captures unknown columns)
- ✅ Idempotent processing (exactly-once semantics verified)
- ✅ Parameterized job (single job, 4 entities)

####  **Silver Layer** (Cleaned Data)

Unified business model with quality enforcement:

- `silver.silver_flights` - Validated flight records (110 rows)
- `silver.silver_passengers` - Enhanced passenger profiles (220 rows)
- `silver.silver_airports` - Standardized airport data (55 rows)
- `silver.silver_bookings` - Quality-checked bookings (1.3K rows)
- `silver.silver_business` - Denormalized fact+dimension join (1.3K rows)

**Key Features:**
- ✅ Delta Live Tables (DLT) streaming pipelines
- ✅ Data quality expectations (2 enforced rules)
- ✅ Schema evolution auto-detection
- ✅ Type casting & standardization

####  **Gold Layer** (Analytics-Ready)

Business-ready dimensional model (star schema):

**Dimensions:**
- `gold.DimPassengers` - SCD Type 2 tracked (235 rows, sequential surrogate keys)
- `gold.DimFlights` - Historical changes tracked (110 rows)
- `gold.DimAirports` - Dimension table (55 rows)

**Fact Table:**
- `gold.FactBookings` - Central fact table (1.3K rows) with foreign keys to all dimensions

**Key Features:**
- ✅ Surrogate keys (sequential IDs for stability)
- ✅ SCD Type 2 tracking (create_date, update_date columns)
- ✅ MERGE logic (incremental updates with deduplication)
- ✅ Multi-threaded dimension building (parallel processing)

## Project Structure

```
uk-property-lakehouse/
│
├── README.md                           # This file (project overview)
├── LICENSE                             # MIT License
├── .gitignore                          # Git ignore rules
│
├── notebooks/                          # Databricks notebook source code
│   ├── bronze/
│   │   ├── BronzeLayer.py             # Autoloader ingestion (parameterized)
│   │   └── Src_Parameters.py          # Job orchestration parameters
│   │
│   ├── silver/
│   │   └── SILVER_dLt_PIPELINE.py     # DLT streaming transformations
│   │
│   └── gold/
│       ├── GOLD_DIMS.py               # Dimension building (SCD Type 2)
│       └── GOLD_FACT.py               # Fact table with dynamic joins
│
├── configs/                            # Configuration files
│   ├── unity_catalog_setup.sql        # Schema creation script
│   ├── dbt_models_future.sql          # Sample dbt models (Phase 2)
│   └── dashboard_queries.sql          # Analytics queries
│
├── docs/                               # Documentation
│   ├── ARCHITECTURE.md                 # Technical deep-dive
│   ├── OPERATIONS.md                   # How to run & monitor
│   ├── DATA_DICTIONARY.md              # Column metadata
│   └── TROUBLESHOOTING.md              # Common issues & fixes
│
├── assets/                             # Screenshots & diagrams
│   ├── bronze-job-runs.png            # Databricks job execution
│   ├── silver-dlt-lineage.png         # DLT pipeline diagram
│   ├── gold-star-schema.png           # Dimensional model
│   └── medallion-architecture.png     # System overview
│
└── tests/                              # Data quality validation
    ├── test_bronze_idempotency.py     # Verify 0 duplicates
    ├── test_silver_quality.py         # Quality expectations
    └── test_gold_schema.py            # Surrogate key validation
```

##  Quick Start

### Prerequisites

1. **Azure Databricks** (Free Edition)
2. **Azure Data Lake Storage Gen2** (for CSV storage)
3. **GitHub Account** (version control)
4. **Python 3.8+** (optional, for tests)

### Step 1: Import Notebooks

```
In Databricks UI:
1. Workspace → Import → Select notebooks/ folder
2. Upload all files to /Users/{your-email}/
3. Structure:
   - /Bronze/BronzeLayer.py
   - /Silver/SILVER_dLt_PIPELINE.py
   - /Gold/GOLD_DIMS.py, GOLD_FACT.py
```

### Step 2: Create Unity Catalog

```sql
-- Run in Databricks SQL Editor
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA workspace.raw;
CREATE SCHEMA workspace.bronze;
CREATE SCHEMA workspace.silver;
CREATE SCHEMA workspace.gold;

-- Create volumes
CREATE VOLUME workspace.raw.rawvolume;
CREATE VOLUME workspace.bronze.bronzevolume;
CREATE VOLUME workspace.silver.silvervolume;
CREATE VOLUME workspace.gold.goldvolume;
```

### Step 3: Upload Raw Data

```
Upload CSVs to blob storage:
/raw/rawvolume/rawdata/
  ├─ flights/flights.csv
  ├─ passengers/passengers.csv
  ├─ airports/airports.csv
  └─ bookings/bookings.csv
```

### Step 4: Run Bronze Job

```
Workflows → Jobs → Create
Name: BronzeIngestion run
Notebook: BronzeLayer.py
Parameters: src_array = ["flights", "passengers", "airports", "bookings"]
Trigger: Manual or scheduled daily
```

### Step 5: Run Silver DLT Pipeline

```
Workflows → Pipelines → Create
Name: SILVER_dLt_PIPELINE
Notebook: SILVER_dLt_PIPELINE.py
Config: Advanced edition
Trigger: Auto (after Bronze) or manual
```

### Step 6: Run Gold Notebooks

```
Run sequentially:
1. GOLD_DIMS.py → Creates 4 dimension tables
2. GOLD_FACT.py → Creates fact table with joins
```


## Results & Validation

| Metric | Value | Status |
|--------|-------|--------|
| **Daily Transactions** | 1.3K bookings | ✅ |
| **Duplicate Rate** | 0% (2 runs tested) | ✅ |
| **Quality Gates** | 2 enforced rules | ✅ |
| **Dimension Coverage** | 4 entities | ✅ |
| **Surrogate Keys** | Sequential (1-235) | ✅ |
| **Processing Time** | ~2 min (full), ~30s (incr) | ✅ |
| **Schema Evolution** | Auto-detected | ✅ |

## Security & Best Practices

1. **Unity Catalog Governance**
   - Schema-level isolation (raw → bronze → silver → gold)
   - Version control for all code
   - Audit trail via Delta Lake time travel

2. **Credentials Management**
   - No hardcoded passwords
   - Databricks secrets for auth
   - Service principal support

3. **Code Quality**
   - Parameterized notebooks (reusable)
   - Type hints & docstrings
   - Consistent naming conventions


## Technologies Demonstrated

✅ **Medallion Architecture**  
✅ **Idempotent Streaming Ingestion**  
✅ **Delta Live Tables (DLT)**  
✅ **SCD Type 2 Dimensions**  
✅ **Surrogate Key Generation**  
✅ **Star Schema Modeling**  
✅ **Incremental Load Patterns**  
✅ **Schema Evolution**  
✅ **Data Quality Enforcement**  
✅ **Multi-threading & Parallelization**  
