# FHIR API Data Ingestion & Analytics Pipeline
## Medallion Lakehouse Architecture Implementation

A production-grade data engineering solution that ingests healthcare data from a public FHIR API and transforms it through a medallion architecture (Raw → Bronze → Silver → Gold) using Microsoft Fabric, PySpark, and Delta Lake.

---

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Data Model](#data-model)
- [Pipeline Components](#pipeline-components)
- [Setup & Installation](#setup--installation)
- [How to Run](#how-to-run)
- [Table Descriptions](#table-descriptions)
- [Data Quality & Versioning](#data-quality--versioning)
- [Project Structure](#project-structure)
- [Future Enhancements](#future-enhancements)

---

## 🎯 Project Overview

This project implements a complete end-to-end data pipeline that:
- Fetches healthcare data from the **HAPI FHIR public API** (HL7 FHIR R4 standard)
- Implements **incremental ingestion** with pagination
- Stores raw JSON responses in structured lakehouse folders
- Transforms data through 4 layers: **Raw → Bronze → Silver → Gold**
- Tracks **data versioning** using Slowly Changing Dimension (SCD) Type 2
- Logs **pipeline metadata** for monitoring and auditing
- Orchestrates all workflows via **Fabric Data Pipeline**

### Key Features
✅ Medallion architecture with full data lineage
✅ SCD Type 2 implementation for historical tracking
✅ Parameterized pipeline (configurable dates, page limits)
✅ Comprehensive metadata logging (pipeline_run_log table)
✅ Automated orchestration with error handling
✅ Production-ready code with proper exception handling

---

## 🏗️ Architecture

### Medallion Architecture Layers

RAW LAYER
- JSON files bucketed by resource type and load date
- Path: Files/Raw/{ResourceType}/{LoadDate}/bundle_*.json

BRONZE LAYER
- Delta tables with raw ingested data + metadata
- Tables: bronze_patient, bronze_encounter, bronze_observation, bronze_condition
- Extra Columns: extraction_timestamp, load_date, api_base_url, api_params

SILVER LAYER
- Cleaned, deduplicated, typed Delta tables
- SCD Type 2: valid_from, valid_to, is_current, row_hash
- Tables: silver_patient, silver_encounter, silver_observation, silver_condition

GOLD LAYER
- Analytics-ready dimensional model
- Dimensions: gold_dim_patient, gold_dim_encounter
- Facts: gold_fact_observation, gold_fact_condition
- Views: gold_vw_patient_encounter_observation, gold_vw_patient_conditions, gold_vw_full_clinical_summary

### Pipeline Flow

NB_01_FHIR_Ingestion
  - Fetch FHIR API
  - Save Raw JSON
  - Write Bronze Delta
  - Log to metadata
        ↓ On Success
    Wait 60 seconds
        ↓
NB_02_FHIR_Silver
  - Deduplicate
  - Type casting
  - SCD Type 2 merge
        ↓ On Success
    Wait 60 seconds
        ↓
NB_03_FHIR_Gold
  - Create Dim/Fact tables
  - Build analytical views
  - Analytics layer ready

---

## 🛠️ Technologies Used

| Component | Technology |
|-----------|-----------|
| Cloud Platform | Microsoft Fabric |
| Data Processing | PySpark (Python 3.10+) |
| Storage Format | Delta Lake (Parquet + transaction log) |
| Orchestration | Fabric Data Pipeline |
| API | HAPI FHIR R4 (https://hapi.fhir.org/baseR4/) |
| Data Model | Star Schema (Dimensional Modeling) |
| Version Control | Git / GitHub |
| Language | Python 3.10+ |

### Python Libraries Used
- pyspark.sql.functions
- pyspark.sql.types
- pyspark.sql.window
- delta.tables.DeltaTable
- requests
- json
- uuid
- datetime

---

## 📊 Data Model

### FHIR Resources Ingested
1. Patient - Demographics (name, gender, birthDate, address)
2. Encounter - Healthcare visits (status, period, type)
3. Observation - Clinical measurements (vital signs, lab results)
4. Condition - Diagnoses and health conditions

### Entity Relationship Diagram

gold_dim_patient (patient_id PK)
    │
    ├──────────────────────────────────┐
    │                                  │
    ↓ 1:N                              ↓ 1:N
gold_dim_encounter                gold_fact_condition
  (encounter_id PK)                 (condition_id PK)
  (patient_id FK)                   (patient_id FK)
    │                                 (encounter_id FK)
    │ 1:N
    ↓
gold_fact_observation
  (observation_id PK)
  (patient_id FK)
  (encounter_id FK)

---

## 🔧 Pipeline Components

### NB_01_FHIR_Ingestion (Raw + Bronze)

Purpose: Fetch data from FHIR API and create Bronze Delta tables

Key Functions:
- ingest_fhir_resource() — Main ingestion logic with pagination
- ensure_log_table_exists() — Creates metadata logging table
- write_log_entry() — Logs each resource run (SUCCESS/FAILED)

Parameters accepted from Pipeline:
- start_date (default: 2026-04-01) — First date to ingest
- days_to_ingest (default: 3) — Number of days to fetch
- max_pages_per_day (default: 10) — Pagination safety limit

Outputs:
- Raw JSON files: Files/Raw/{ResourceType}/{LoadDate}/bundle_*.json
- Bronze tables: bronze_patient, bronze_encounter, bronze_observation, bronze_condition
- Metadata log: pipeline_run_log

Metadata Columns Added:
- extraction_timestamp — When API was called
- load_date — Which batch this belongs to
- api_base_url — API endpoint used
- api_params — Query parameters (_count, _lastUpdated)
- ingestion_start_date — Start of date range
- ingestion_days — Days requested
- bronze_load_id — Unique row identifier

---

### NB_02_FHIR_Silver (SCD Type 2)

Purpose: Clean, deduplicate, and track historical changes

Key Functions:
- transform_to_silver() — Main transformation per resource
- scd2_merge() — Implements Slowly Changing Dimension Type 2
- add_row_hash() — MD5 hash for change detection
- bootstrap_silver_table() — Creates table on first run

SCD Type 2 Logic:
- IF record is NEW → INSERT with is_current=True, valid_from=now
- IF record CHANGED (hash different) → expire old row + INSERT new row
- IF record UNCHANGED → Do nothing (idempotent)

SCD Columns Added:
- valid_from — When this version became active
- valid_to — When this version expired (NULL if current)
- is_current — Boolean flag (True = latest version)
- row_hash — MD5 hash for change detection
- silver_load_timestamp — When record was written to silver

Outputs:
- silver_patient, silver_encounter, silver_observation, silver_condition

---

### NB_03_FHIR_Gold (Analytics Layer)

Purpose: Create dimensional model for reporting and analytics

Dimension Tables:
- gold_dim_patient — Patient master data
- gold_dim_encounter — Visit/encounter master data

Fact Tables:
- gold_fact_observation — Clinical measurements and vitals
- gold_fact_condition — Diagnoses and health conditions

Analytical Views:
- gold_vw_patient_encounter_observation — Wide clinical view
- gold_vw_patient_conditions — Patient condition history
- gold_vw_full_clinical_summary — Complete 360 degree patient view

Key Transformations:
- Filters to WHERE is_current = True (only latest versions)
- Strips FHIR reference prefixes: "Patient/123" → "123"
- Flattens nested JSON structures (code.text, valueQuantity.value)
- Creates proper foreign key relationships

---

### Pipeline Orchestration

Pipeline Name: PL_FHIR_Medallion

Activities (in order):
1. Notebook: NB_01_FHIR_Ingestion
2. Wait: 60 seconds
3. Notebook: NB_02_FHIR_Silver
4. Wait: 60 seconds
5. Notebook: NB_03_FHIR_Gold

Pipeline Parameters:
- start_date → Injected into NB_01
- days_to_ingest → Injected into NB_01
- max_pages_per_day → Injected into NB_01

Error Handling:
- Each notebook wrapped in try/except
- Failures logged to pipeline_run_log with error message
- Pipeline stops on failure (green arrow = on success only)

---

## 🚀 Setup & Installation

### Prerequisites
- Microsoft Fabric workspace (60-day free trial available)
- Lakehouse created in workspace
- Internet access to HAPI FHIR API

### Step 1: Create Lakehouse
1. Open Microsoft Fabric workspace
2. Click New → Lakehouse
3. Name it: FHIR_Lakehouse

### Step 2: Create Notebooks
1. Create 3 Spark notebooks: NB_01_FHIR_Ingestion, NB_02_FHIR_Silver, NB_03_FHIR_Gold
2. Attach each notebook to FHIR_Lakehouse (Left panel → Add Lakehouse)
3. Copy-paste code from repository into each notebook

### Step 3: Create Pipeline
1. Click New → Data Pipeline
2. Name it: PL_FHIR_Medallion
3. Add 3 Notebook activities + 2 Wait activities
4. Connect with On Success arrows: NB_01 → Wait(60s) → NB_02 → Wait(60s) → NB_03
5. Add pipeline parameters:
   - start_date (String, default: 2026-04-01)
   - days_to_ingest (Int, default: 3)
   - max_pages_per_day (Int, default: 10)
6. Link parameters to NB_01 via Settings → Base parameters

---

## ▶️ How to Run

### Option 1: Run Full Pipeline (Recommended)
1. Open PL_FHIR_Medallion pipeline
2. Click Run
3. Adjust parameters if needed:
   - start_date: 2026-04-01
   - days_to_ingest: 3
   - max_pages_per_day: 10
4. Click OK
5. Monitor execution in Output tab
6. Expected duration: 8-12 minutes

### Option 2: Run Notebooks Manually (Testing)
1. Open NB_01 → Click Run All → Wait for completion (~4 min)
2. Open NB_02 → Click Run All → Wait for completion (~1 min)
3. Open NB_03 → Click Run All → Wait for completion (~30 sec)

### Verify Success (run in SQL endpoint)
SELECT 'bronze_patient' AS table_name, COUNT(*) AS rows FROM bronze_patient UNION ALL
SELECT 'bronze_encounter', COUNT(*) FROM bronze_encounter UNION ALL
SELECT 'bronze_observation', COUNT(*) FROM bronze_observation UNION ALL
SELECT 'bronze_condition', COUNT(*) FROM bronze_condition UNION ALL
SELECT 'silver_patient', COUNT(*) FROM silver_patient UNION ALL
SELECT 'silver_encounter', COUNT(*) FROM silver_encounter UNION ALL
SELECT 'silver_observation', COUNT(*) FROM silver_observation UNION ALL
SELECT 'silver_condition', COUNT(*) FROM silver_condition UNION ALL
SELECT 'gold_dim_patient', COUNT(*) FROM gold_dim_patient UNION ALL
SELECT 'gold_dim_encounter', COUNT(*) FROM gold_dim_encounter UNION ALL
SELECT 'gold_fact_observation', COUNT(*) FROM gold_fact_observation UNION ALL
SELECT 'gold_fact_condition', COUNT(*) FROM gold_fact_condition UNION ALL
SELECT 'pipeline_run_log', COUNT(*) FROM pipeline_run_log
ORDER BY table_name;

Expected Results (3 days of data):
- Bronze tables: ~6,000 rows each (includes duplicates across days)
- Silver tables: ~1,300-1,500 rows each (deduplicated)
- Gold tables: same as silver (filtered to is_current = True)
- pipeline_run_log: 4+ rows (one per resource per run)

---

## 📚 Table Descriptions

### Bronze Layer

| Table | Description | Approx Rows (3 days) |
|-------|-------------|----------------------|
| bronze_patient | Raw patient demographics from API | ~6,000 |
| bronze_encounter | Raw encounter/visit records | ~6,000 |
| bronze_observation | Raw clinical observations | ~6,000 |
| bronze_condition | Raw diagnoses/conditions | ~6,000 |

Key Columns:
- All original FHIR fields (JSON structure preserved as Delta columns)
- extraction_timestamp — API call timestamp
- load_date — Batch date identifier
- api_base_url, api_params — Full API tracking

### Silver Layer

| Table | Description | Approx Rows |
|-------|-------------|-------------|
| silver_patient | Cleaned patient master | ~1,326 |
| silver_encounter | Cleaned encounters | ~1,500 |
| silver_observation | Cleaned observations | ~1,500 |
| silver_condition | Cleaned conditions | ~1,500 |

Key Columns:
- All business columns (correctly typed)
- is_current — TRUE for latest version, FALSE for historical
- valid_from, valid_to — SCD Type 2 validity window
- row_hash — MD5 hash for change detection

Sample Queries:
-- Current patients only
SELECT * FROM silver_patient WHERE is_current = True;

-- Patient version history
SELECT id, gender, valid_from, valid_to, is_current
FROM silver_patient WHERE id = 'patient-123'
ORDER BY valid_from DESC;

### Gold Layer

gold_dim_patient columns:
- patient_id, patient_name, gender, birthDate, address, valid_from, is_current

gold_dim_encounter columns:
- encounter_id, patient_id (FK), status, class, period_start, period_end, type

gold_fact_observation columns:
- observation_id, patient_id (FK), encounter_id (FK), observation_code, value, unit, effectiveDateTime, status

gold_fact_condition columns:
- condition_id, patient_id (FK), encounter_id (FK), condition_code, clinical_status, onsetDateTime, recordedDate

---

## 🔍 Data Quality & Versioning

### SCD Type 2 — How It Works

Day 1 — Patient first ingested:
{id: "p123", gender: "male", is_current: True, valid_from: "2026-04-20", valid_to: NULL}

Day 5 — Gender changes in source system:
Old row expires:
{id: "p123", gender: "male", is_current: False, valid_from: "2026-04-20", valid_to: "2026-04-25"}
New row inserted:
{id: "p123", gender: "other", is_current: True, valid_from: "2026-04-25", valid_to: NULL}

Change Detection Method:
- MD5 hash computed from business columns (gender, birthDate, status, etc.)
- Incoming hash compared to stored hash
- If different → change detected → SCD Type 2 triggered
- If same → no action (fully idempotent)

### Metadata Logging — pipeline_run_log

Columns tracked:
- run_id — Unique UUID per resource run
- pipeline_name — NB_01_FHIR_Ingestion
- resource_type — Patient / Encounter / Observation / Condition
- run_timestamp, end_timestamp — Execution window
- duration_seconds — How long the run took
- rows_ingested — Raw records fetched from API
- pages_fetched — Number of paginated API calls made
- bronze_rows — Rows written to Bronze table
- status — SUCCESS or FAILED
- error_message — NULL on success, exception string on failure
- api_base_url, api_params — Exact API call details
- start_date, days_ingested, load_date, raw_path — Run configuration

Sample Queries:
-- All pipeline runs
SELECT * FROM pipeline_run_log ORDER BY run_timestamp DESC;

-- Success rate by resource
SELECT resource_type, COUNT(*) AS total_runs,
SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
AVG(duration_seconds) AS avg_duration_seconds
FROM pipeline_run_log GROUP BY resource_type;

-- Failed runs only
SELECT * FROM pipeline_run_log WHERE status = 'FAILED';

---

## 📁 Project Structure

fhir-medallion-pipeline/
│
├── notebooks/
│   ├── NB_01_FHIR_Ingestion.py       # Raw + Bronze layer + Metadata logging
│   ├── NB_02_FHIR_Silver.py          # Silver layer with SCD Type 2
│   └── NB_03_FHIR_Gold.py            # Gold layer with Star schema
│
├── pipeline/
│   └── PL_FHIR_Medallion.json        # Pipeline definition export
│
├── sql/
│   ├── verify_tables.sql             # Table validation queries
│   ├── sample_analytics.sql          # Example analytical queries
│   └── scd2_verification.sql         # Check historical tracking
│
├── README.md                          # This file
├── LICENSE                            # MIT License
└── .gitignore                         # Git ignore file

---

## 🔮 Future Enhancements

Immediate Next Steps:
- Add XML ingestion as alternative to JSON format
- Create Power BI report connected to Gold layer
- Implement null handling and referential integrity checks
- Add auto-detection of last load date for true incremental loads

Advanced Features:
- Support more FHIR resources (Medication, Procedure, AllergyIntolerance)
- Implement real-time streaming ingestion via Event Hubs
- Add data masking and anonymization for PII fields
- Build machine learning feature store on Gold layer
- Create dbt models for transformation layer
- Add automated data quality testing (Great Expectations)
- Implement CI/CD pipeline for notebook deployment

Observability:
- Integrate with Azure Monitor and Application Insights
- Create alerting for pipeline failures via email/Teams
- Build pipeline execution dashboard in Power BI
- Add data lineage visualization

---

## ✅ Assignment Requirements Checklist

| Requirement | Status | Notes |
|------------|--------|-------|
| Incremental ingestion 2-3 days | ✅ Complete | Parameterized start_date + days_to_ingest |
| Pagination | ✅ Complete | max_pages_per_day safety limit |
| Raw layer JSON bucketed by date | ✅ Complete | Files/Raw/{Resource}/{Date}/ |
| Bronze layer Delta tables | ✅ Complete | 4 bronze tables created |
| Silver layer cleaned + deduplicated | ✅ Complete | SCD Type 2 implemented |
| Gold layer dimensional model | ✅ Complete | Star schema with 3 analytical views |
| Metadata columns | ✅ Complete | extraction_timestamp, api_url, api_params |
| Data versioning SCD Type 2 | ✅ Complete | valid_from, valid_to, is_current, row_hash |
| Metadata logging | ✅ Complete | pipeline_run_log Delta table |
| Orchestration pipeline | ✅ Complete | 3 notebooks chained with parameters |
| Modular reusable code no hardcoding | ✅ Complete | All values parameterized |
| Documentation | ✅ Complete | This README + inline code comments |
| Git repository submission | ✅ Complete | All notebooks versioned |
| XML ingestion (optional) | ⬜ Not implemented | Stretch goal |
| Power BI report (optional) | ⬜ Not implemented | Stretch goal |

---

## 👨‍💻 Author

Your Name
Data Engineer — Microsoft Fabric | PySpark | Delta Lake

---

## 📄 License

This project is licensed under the MIT License. See LICENSE file for details.

---

## 🙏 Acknowledgments

- HAPI FHIR for providing free public API access at https://hapi.fhir.org
- Microsoft Fabric for the unified lakehouse platform
- HL7 FHIR for the international healthcare data standard

---

Last Updated: April 2026
Version: 1.0.0
