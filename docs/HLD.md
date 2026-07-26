# BTS Aviation Delay Intelligence System

## High Level Design (HLD)

### Version: 0.1 | Date: July 26, 2026 | Status: Draft

---

## 1. Problem Statement

The US Bureau of Transportation Statistics publishes domestic flight
delay data publicly — but in raw CSV format with no structure,
no validation, and no analytical layer.

This project builds a data engineering pipeline that transforms
raw BTS flight records into a trusted analytical pipeline for
delay analysis and business reporting.

**Core question:** Where are delays happening, why, and what does it cost?

---

## 2. Design Goals

- Reliability over speed
- Reproducible and idempotent pipelines
- Statistically honest analytics
- Scalable columnar storage
- Modular architecture
- Business-oriented data model

---

## 3. Data Source

- **Source:** US Bureau of Transportation Statistics (BTS)
- **URL:** transtats.bts.gov
- **Coverage:** US domestic flights, 2023-2025
- **Volume:** ~18 million flight records (estimated)
- **Format:** Monthly CSV files (36 files)
- **Cost:** Free — US Government public data

---

## 4. Architecture — Medallion Pattern

BTS Monthly CSV Files
│
▼
Bronze (Immutable Raw Storage)
│
▼
Silver (Validated Analytics Dataset)
│
▼
Gold (Star Schema + Business Metrics)
│
▼
SQL / Dashboards / Analytics

### Bronze Layer

- Reads raw BTS CSV files as-is
- Adds ingestion timestamp
- Validates schema and row counts
- Writes to Parquet — append only
- Never modified after write

### Silver Layer

- Selects key columns for analysis
- Corrects data types
- Applies validation rules
- Handles NULLs honestly
- Partitions by MONTH
- Separates cancelled vs delayed flights

### Gold Layer

- Builds star schema (Kimball methodology)
- fact_flights + dimension tables
- Trusted metrics: AVG + STDDEV + MEDIAN + PERCENTILES
- Root cause breakdown from BTS delay cause columns
- Cost indicators using Ferguson et al. methodology

---

## 5. Tech Stack

| Component  | Technology                      | Reason                                 |
| ---------- | ------------------------------- | -------------------------------------- |
| Processing | PySpark 3.5.1                   | Handles 18M rows efficiently           |
| Storage    | Parquet                         | 3-10x faster than CSV, schema embedded |
| Language   | Python 3.11                     | Stable with PySpark 3.5.x              |
| Query      | SQL / Spark SQL                 | Data modeling and validation           |
| Cloud      | Local (August), Azure (October) | Migrate after pipeline stable          |

---

## 6. Defensive Data Engineering Principles

- Bronze is immutable — raw data never modified
- Idempotent processing — safe to rerun without side effects
- Fail fast on invalid schema
- Explicit data quality checks at each layer
- Document every architectural decision in ADR folder
- Preserve business meaning over convenience
- Filter early, aggregate early, join late
- Never report AVG alone — always with STDDEV + MEDIAN + PERCENTILES
- Never fill ARR_DELAY nulls with 0 — NULL means cancelled, not on time

---

## 7. Data Quality

Each layer applies explicit validation:

- **Schema validation** — expected columns present, correct types
- **Null validation** — NULLs documented and handled per column
- **Duplicate detection** — no duplicate flight records
- **Domain validation** — year in 2023-2025, distance > 0
- **Referential integrity** — carrier codes match known carriers
- **Business rule validation** — ARR_DELAY NULL only when CANCELLED = 1

---

## 8. Non-Functional Requirements

**Scalability**

- Designed to process 18M+ rows using PySpark

**Reliability**

- Idempotent ingestion — reruns produce same result

**Maintainability**

- Modular Bronze/Silver/Gold layers
- Every decision documented in ADR folder

**Performance**

- Columnar Parquet storage
- Partition pruning by MONTH

**Observability**

- Row count validation at each layer
- Schema validation logging

---

## 9. Cost Estimation Approach

Based on Ferguson et al. (George Mason University, NASA-funded)
delay cost methodology.

Key finding: airborne delay costs approximately 20x gate delay.

**Important limitations:**

- Costs vary by aircraft type, airline, and delay phase
- BTS data does not contain aircraft type detail
- All cost figures are directional indicators only
- Cancelled flights excluded from delay cost calculation

---

## 10. Assumptions

- BTS data is historically accurate as published
- Monthly CSV files remain unchanged after publication
- Delay codes follow standard BTS definitions
- No airline internal operational data available
- Cost estimates are directional — not precise financial calculations

---

## 11. What This System Does

✅ Identifies where delays are concentrated
✅ Explains root cause by delay type
✅ Provides directional cost indicators
✅ Shows delay patterns by route, carrier, airport, season
✅ Statistically honest metrics with full distribution

---

## 12. What This System Does NOT Do

❌ Real-time flight data (historical only)
❌ Predict future delays
❌ Provide precise cost calculations
❌ Access internal airline operational data

---

## 13. Future Scope

_These are intended directions — not committed deliverables._

- Predictive delay models (ML layer)
- Weather data integration
- Aircraft rotation analysis
- Real-time streaming pipeline
- AI-assisted operational insights
- Maintenance data integration

---

## 14. Repository Structure

-BTS-Aviation-Delay-Intelligence/
├── pipeline/
│ ├── bronze/bronze_ingestion.py
│ ├── silver/silver_transform.py
│ └── gold/gold_aggregation.py
├── tests/
├── config/
├── docs/
│ ├── HLD.md
│ ├── data_model.md
│ └── ADR/
├── data/ (gitignored)
│ ├── raw/
│ ├── bronze/
│ ├── silver/
│ └── gold/
├── requirements.txt
├── README.md
└── analysis/

---

> Note: This document describes the intended architecture.
> Sections updated as each layer is implemented.
> Every architectural decision documented in ADR folder.
