# Operations Guide: UK Property Lakehouse

## Prerequisites

| Requirement | Detail |
|-------------|--------|
| Databricks workspace | Free Edition or above |
| Runtime | Databricks Runtime 13.3 LTS or higher |
| Compute | Serverless (recommended) or single-node cluster |
| Storage | DBFS or external volume (`/Volumes/workspace/raw/rawVolume/`) |
| GitHub | Personal Access Token with `repo` scope |

---

## Repository Structure

```
Uk-property-lakehouse/
├── data/                          # Raw source CSV files
│   ├── dim_airports.csv
│   ├── dim_flights.csv
│   ├── dim_passengers.csv
│   ├── fact_bookings.csv
│   └── *_increment*.csv           # Incremental load files
├── notebooks/
│   ├── bronze/
│   │   ├── BronzeLayer.py         # Autoloader ingestion
│   │   └── Src_Parameters.py      # Source path parameters
│   ├── silver/
│   │   └── SilverNotebook-bookings.py  # DLT streaming pipeline
│   └── gold/
│       ├── GOLD_DIMS.py           # Dimension table builds
│       └── GOLD_FACT.py           # Fact table build
├── Screenshots/
│   ├── Data_Incremental_Pipeline.png
│   └── Silver_Pipeline.png
├── docs/
│   ├── ARCHITECTURE.md
│   └── OPERATIONS.md
└── README.md
```

---

## Setup Instructions

### Step 1 — Upload raw data to Databricks volume

1. In Databricks, go to **Catalog → Volumes**
2. Navigate to `/Volumes/workspace/raw/rawVolume/rawdata/`
3. Upload all CSV files from the `data/` folder

### Step 2 — Import notebooks

1. In Databricks Workspace, create a folder: `End to End Databricks Flight Project`
2. Import each notebook from `notebooks/` using **File → Import**
3. Verify folder structure matches:
   ```
   /Users/your-email/End to End Databricks Flight Project/
   ├── BronzeLayer
   ├── Src_Parameters
   ├── SilverNotebook- bookings
   ├── GOLD_DIMS
   └── GOLD_FACT
   ```

### Step 3 — Configure parameters

Open `Src_Parameters` and verify the source path widget matches your volume path:

```python
src_value = dbutils.widgets.get("src")
# Default: /Volumes/workspace/raw/rawVolume/rawdata/
```

### Step 4 — Run Bronze layer

1. Open `BronzeLayer`
2. Attach to a Serverless cluster
3. Run all cells
4. Verify output: 0 duplicate records, all 4 base tables loaded

### Step 5 — Run Silver DLT pipeline

1. Go to **Jobs & Pipelines → Create Pipeline**
2. Set pipeline mode: **Triggered**
3. Add notebook path: `SilverNotebook- bookings`
4. Click **Start** and monitor the pipeline graph
5. Verify: 1,300+ records processed, all expectations passed

### Step 6 — Run Gold layer

1. Open `GOLD_DIMS` → Run all cells (builds 4 dimension tables)
2. Open `GOLD_FACT` → Run all cells (builds fact_bookings)
3. Verify star schema is queryable via SQL Editor

---

## Running Incremental Loads

The project includes increment CSV files to simulate real-world data arrival:

```
data/
├── dim_passengers_increment1.csv
├── fact_bookings_SCD_increment*.csv
```

To process an incremental load:

1. Upload the increment CSV to the raw volume
2. Re-run `BronzeLayer` — Autoloader detects new files via checkpoint
3. Re-trigger the Silver DLT pipeline — only new records are processed
4. Re-run Gold notebooks — dimensions and fact table update automatically

---

## Verification Queries

Run these in the Databricks SQL Editor to validate each layer:

```sql
-- Bronze: row counts
SELECT COUNT(*) FROM bronze.dim_flights;
SELECT COUNT(*) FROM bronze.fact_bookings;

-- Silver: SCD Type 2 check
SELECT passenger_id, COUNT(*) as versions
FROM silver.dim_passengers
GROUP BY passenger_id
HAVING COUNT(*) > 1;

-- Gold: star schema join
SELECT
  f.flight_number,
  p.passenger_name,
  a.airport_name,
  b.revenue
FROM gold.fact_bookings b
JOIN gold.dim_flights f ON b.flight_id = f.flight_id
JOIN gold.dim_passengers p ON b.passenger_id = p.passenger_id
JOIN gold.dim_airports a ON b.airport_id = a.airport_id
LIMIT 10;
```

---

## Troubleshooting

| Issue | Likely cause | Fix |
|-------|-------------|-----|
| Autoloader finds 0 files | Wrong volume path | Check `src_value` widget in `Src_Parameters` |
| DLT pipeline fails immediately | Runtime mismatch | Ensure DBR 13.3 LTS or higher |
| Duplicate records in Bronze | Checkpoint deleted | Restore or reset checkpoint path |
| Gold table not found | Silver didn't complete | Re-run Silver pipeline first |
| Unity Catalog permission error | Catalog not configured | Enable Unity Catalog in workspace admin settings |

---

## Key Metrics

| Metric | Value |
|--------|-------|
| Bronze duplicates | 0 (idempotent Autoloader) |
| Silver records processed | 1,300+ |
| Gold dimensions | 4 |
| Gold fact table grain | One row per booking |
| SCD Type 2 tables | 1 (`dim_passengers`) |
