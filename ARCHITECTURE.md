# Architecture: UK Property Lakehouse

## Overview

This project implements a medallion architecture (Bronze → Silver → Gold) on Azure Databricks, ingesting UK flight booking and property data into a fully governed Delta Lake. The pipeline is designed for incremental ingestion, schema enforcement, and analytical querying via a star schema.

---

## Architecture Diagram

```
Raw CSV Files (Cloud Storage)
        │
        ▼
┌───────────────────┐
│   BRONZE LAYER    │  Autoloader (idempotent ingestion)
│  Delta Tables     │  Schema inference, deduplication
│  (Raw + Append)   │  Checkpoint-based replayability
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│   SILVER LAYER    │  Delta Live Tables (DLT streaming)
│  Cleansed Tables  │  Data quality expectations
│  (SCD Type 2)     │  Incremental processing (1.3K records)
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│    GOLD LAYER     │  Star schema (4 dims + 1 fact)
│  Dim + Fact Tables│  Business-ready aggregations
│  (Analytics-Ready)│  Unity Catalog governance
└───────────────────┘
```

---

## Layer Details

### Bronze Layer

| Component | Detail |
|-----------|--------|
| Ingestion method | Databricks Autoloader (`cloudFiles`) |
| File format | CSV (raw source) → Delta |
| Deduplication | Checkpoint-based - 0 duplicate records verified |
| Tables | `dim_airports`, `dim_flights`, `dim_passengers`, `fact_bookings` |
| Notebook | `notebooks/bronze/BronzeLayer.py` |
| Parameters | `notebooks/bronze/Src_Parameters.py` |

Key design decisions:
- Autoloader used over COPY INTO for scalability with large file volumes
- Schema inference enabled with `cloudFiles.inferColumnTypes = true`
- Checkpoints stored in DBFS to guarantee exactly-once ingestion

### Silver Layer

| Component | Detail |
|-----------|--------|
| Pipeline type | Delta Live Tables (DLT) streaming |
| Records processed | 1,300+ records validated |
| SCD handling | Type 2 (historical tracking with `is_current` flag) |
| Data quality | DLT expectations enforced (null checks, type constraints) |
| Notebook | `notebooks/silver/SilverNotebook-bookings.py` |

Key design decisions:
- DLT chosen over manual Spark for built-in lineage and observability
- SCD Type 2 applied to `dim_passengers` to track customer changes over time
- Incremental data files (`_increment` CSVs) used to simulate real-world CDC

### Gold Layer

| Component | Detail |
|-----------|--------|
| Schema | Star schema |
| Dimensions | `dim_airports`, `dim_flights`, `dim_passengers`, `dim_dates` |
| Fact table | `fact_bookings` (grain: one row per booking) |
| Notebooks | `notebooks/gold/GOLD_DIMS.py`, `notebooks/gold/GOLD_FACT.py` |

Key design decisions:
- Star schema chosen over normalised model for Power BI / analytical query performance
- Surrogate keys generated using `monotonically_increasing_id()` for dimension tables
- Gold tables registered in Unity Catalog for governance and discoverability

---

## Data Model

```
             ┌──────────────────┐
             │   dim_airports   │
             │  airport_id (PK) │
             └────────┬─────────┘
                      │
┌──────────────┐      │      ┌──────────────────┐
│  dim_flights │──────┼──────│   fact_bookings  │
│ flight_id(PK)│      │      │  booking_id (PK) │
└──────────────┘      │      │  flight_id (FK)  │
                      │      │  passenger_id(FK)│
┌────────────────┐    │      │  airport_id (FK) │
│dim_passengers  │────┘      │  date_id (FK)    │
│passenger_id(PK)│           │  revenue         │
│ is_current     │           └──────────────────┘
└────────────────┘
```

---

## Technology Stack

| Layer | Technology |
|-------|------------|
| Compute | Azure Databricks (Serverless) |
| Storage format | Delta Lake |
| Ingestion | Databricks Autoloader |
| Transformation | PySpark, Delta Live Tables |
| Orchestration | DLT Pipeline (Jobs & Pipelines) |
| Governance | Unity Catalog |
| Version control | GitHub |
| Source data | UK HM Land Registry / flight booking CSVs |

---

## Design Principles

1. **Idempotency** - Bronze ingestion can be re-run without producing duplicates
2. **Incremental processing** - Silver DLT pipelines process only new records
3. **Data quality at ingestion** - DLT expectations fail records before they reach Gold
4. **Governance by default** - All tables registered in Unity Catalog with owner metadata
5. **Separation of concerns** - Each layer has a single responsibility; no cross-layer writes
