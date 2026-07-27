# High-Level Design (HLD)

# BTS Aviation Delay Intelligence Platform

**Version:** 0.2
**Status:** Draft — Pre-Implementation
**Date:** July 2026
**Author:** Narsing Shiva Kumar

> Note: This document describes the planned system
> architecture. Sections marked [PLANNED] will be
> updated with verified facts after implementation
> in August–October 2026.

---

## 1. Problem Statement

Airlines generate millions of flight records every year.
The US Bureau of Transportation Statistics (BTS)
publishes this data monthly — raw CSV files with no
structure, no validation, no analytical layer.

**The core question this system answers:**

> "Which flights delayed, why did they delay,
> how much did it cost, and what patterns repeat
> across carriers, airports, and time?"

Without a trusted pipeline, this question cannot be
answered reliably. Raw BTS data has:

- No enforced schema
- No validated business rules
- No analytical model
- No cost context
- No domain knowledge embedded

This system solves all five.

---

## 2. Design Goals

| Priority | Goal                                                          |
| -------- | ------------------------------------------------------------- |
| 1        | Correctness — every number must be defensible                 |
| 2        | Reliability — silent failures are not acceptable              |
| 3        | Auditability — full lineage from raw CSV to dashboard         |
| 4        | Honesty — no fabricated metrics, no inflated claims           |
| 5        | Clarity — errors must explain what broke, where, and why      |
| 6        | Reliability over speed — correctness first, performance after |

---

## 3. Data Source

| Property | Detail                                                 |
| -------- | ------------------------------------------------------ |
| Source   | US Bureau of Transportation Statistics (BTS TranStats) |
| Dataset  | On-Time Performance — Reporting Carrier                |
| Coverage | January 2023 — December 2025 (3 years)                 |
| Volume   | ~18–20 million rows across 36 monthly CSV files        |
| Format   | CSV, one file per month                                |
| Columns  | 37 selected from 43+ available                         |
| Download | transtats.bts.gov                                      |

**Why 2023–2025:**
Post-COVID normal operations baseline. All five delay
cause columns consistently populated. Mature OCC
reporting standards. Valid year-over-year comparison.
Pre-2000s data excluded — inconsistent reporting
standards and fewer delay cause fields.

---

## 4. System Architecture

### Data Flow

BTS CSV Files (36 files, ~4.5 GB raw)
│
▼
┌─────────────────────────────────────┐
│ BRONZE LAYER │
│ PySpark — Append-only ingestion │
│ Partition: year + month │
│ Format: Parquet │
│ Idempotency: partition check │
└─────────────────────────────────────┘
│
▼
┌─────────────────────────────────────┐
│ SILVER LAYER │
│ PySpark — Validation + Transform │
│ 6-dimension data quality checks │
│ NULL preservation enforced │
│ Deduplication applied │
│ Pattern: DELETE + INSERT │
│ Partition: year + month │
│ Format: Parquet │
└─────────────────────────────────────┘
│
▼
┌─────────────────────────────────────┐
│ GOLD LAYER │
│ PySpark → MySQL Star Schema │
│ Surrogate key assignment │
│ Foreign key validation │
│ Post-join row count checks │
│ Pattern: INSERT OVERWRITE │
│ Indexes: all FK columns │
└─────────────────────────────────────┘
│
▼
┌─────────────────────────────────────┐
│ CACHE LAYER │
│ Redis — Cache-Aside pattern │
│ Heavy Gold aggregations cached │
│ Invalidated on pipeline refresh │
│ Expires: daily after pipeline run │
└─────────────────────────────────────┘
│
▼
┌─────────────────────────────────────┐
│ ANALYTICS LAYER │
│ Power BI Dashboard │
│ (static assets served via CDN) │
│ SQL Analytical Queries (6+) │
│ Cost Sensitivity Calculator │
└─────────────────────────────────────┘

### Reliability Layer (across all layers)

- Errors as a UI: every failure says WHAT, WHERE,
  WHY, and HOW TO FIX
- Pipeline watermark table tracking freshness
  per layer per partition
- Health checks on pipeline jobs
- Late arriving fact detection via watermark
  comparison (Silver vs Gold per partition)

---

## 5. Technology Stack

| Component       | Technology      | Reason                                       |
| --------------- | --------------- | -------------------------------------------- |
| Processing      | PySpark 3.5.1   | 18M rows exceeds pandas threshold (~4.5M)    |
| Storage (raw)   | Parquet         | Columnar, compressed, partition-aware        |
| Storage (gold)  | MySQL           | Star schema, B-Tree indexes, local available |
| Cache           | Redis           | Cache-Aside pattern for Gold aggregations    |
| Dashboard       | Power BI        | Direct connect to MySQL Gold layer           |
| Language        | Python 3.11     | PySpark, validation logic, pipeline code     |
| Version control | Git / GitHub    | All code, docs, ADRs versioned               |
| Environment     | Local (Phase 1) | Validate decisions before cloud migration    |
| Cloud (Phase 2) | Azure           | ADF → ADLS Gen2 → Synapse → Power BI         |
|                 |                 | Migration planned: November 2026+            |

---

## 6. Medallion Architecture

### Bronze Layer — Raw Ingestion

- Append-only. Raw BTS CSV data. Never modified.
- Partition: year + month
- Format: Parquet
- Idempotency: partition existence check before write
- Schema enforced on read via defined PySpark schema
- No transformations. No business logic. Data as-is.

### Silver Layer — Validation + Transformation

- 6-dimension data quality validation gate
- Type casting (FL_DATE string → DateType,
  ARR_DEL15 double → IntegerType)
- NULL preservation per documented policy
- Deduplication on unique flight key
- Pattern: DELETE partition → INSERT clean records
- Errors fail loudly with WHAT / WHERE / WHY /
  HOW TO FIX — never silent

### Gold Layer — Analytical Model

- Star schema: fact_delays + 5 dimension tables
- Surrogate key assignment for all dimensions
- Foreign key validation after joins
- Post-join row count assertion
- Pattern: INSERT OVERWRITE per partition
- B-Tree indexes on all FK columns

---

## 7. Star Schema (Summary)

Full design in data_model.md

| Table            | Type      | SCD    | Rows (est.) |
| ---------------- | --------- | ------ | ----------- |
| fact_delays      | Fact      | N/A    | ~18M        |
| dim_carrier      | Dimension | Type 2 | ~30         |
| dim_airport      | Dimension | Type 2 | ~400        |
| dim_date         | Dimension | Static | 3,653       |
| dim_delay_reason | Dimension | Type 1 | 6           |
| dim_aircraft     | Dimension | Type 4 | ~10,000     |

**Grain:** One row = one scheduled flight
per calendar day

---

## 8. Non-Functional Requirements

### Correctness

- Zero silent failures at any layer
- Every pipeline error includes:
  WHAT broke, WHERE it broke,
  WHY it broke, HOW TO FIX it
- Business rules validated in Silver layer
  before Gold layer receives any data

### Auditability

- Bronze layer is immutable — raw data
  preserved exactly as received from BTS
- Full data lineage: CSV → Bronze → Silver
  → Gold → Dashboard
- pipeline_watermark table tracks freshness
  per layer per partition
- Every schema decision recorded in ADRs

### Idempotency

- Bronze: partition existence check
  (skip if already ingested)
- Silver: DELETE + INSERT
  (safe to rerun — no duplicates)
- Gold: INSERT OVERWRITE
  (safe to rerun — partition replaced)
- Any layer can be rerun without corrupting
  downstream data

### CAP Theorem Decisions

- Analytics layer (Gold + Dashboard): AP
  Availability prioritized over strict consistency.
  24-hour batch data — eventual consistency
  is acceptable. Dashboard may show data from
  previous pipeline run during refresh.
  This is known and acceptable behaviour.

- Silver validation layer: CP
  Consistency required. Validation failures
  reject data rather than allowing stale or
  incorrect records through to Gold.
  System fails loudly. Never serves wrong data.

### Scalability

**Current scale: 18M rows — local single instance**

Decisions made explicitly at this scale:

Sharding:
Not implemented. Single MySQL instance
sufficient at 18M rows.
Re-evaluate when: dataset exceeds 100M rows
OR indexed query latency exceeds 2 seconds.
Future strategy: consistent hashing with
virtual nodes, shard key FL_DATE + ORIGIN.

Read Replica:
Not implemented locally. Single user,
no concurrent read pressure.
Re-evaluate when: multiple concurrent
dashboard users OR pipeline writes visibly
impact dashboard read performance.
Future: one replica for dashboard reads,
primary for pipeline writes only.

Partitioning:
Partition by year + month = 36 partitions,
~500K rows each.
Enables partition pruning — queries scan
only relevant months, not full 18M rows.
Estimated 30x speedup on time-filtered queries.
Idempotency: reprocess one month = delete
one partition, rerun, rewrite.

### Availability

- Local pipeline: no HA requirements in Phase 1
- Future cloud architecture:
  Active-Passive failover for pipeline orchestrator
  ensures no missed daily runs during failover

---

## 9. Data Quality Rules

### Core Business Rules

**Rule 1 — Delay cause NULL preservation (CRITICAL):**
When ARR_DEL15 = 0, all five delay cause columns
MUST be NULL.
Validated on Q1 2024: 1,658,259 rows, 0 violations.
This is correct behaviour — 80.1% of flights are
on time and have no delay cause to report.
These NULLs must NEVER be replaced with 0.

**Rule 2 — ARR_DELAY NULL conditions:**
ARR_DELAY is NULL when CANCELLED = 1 OR DIVERTED = 1.
A cancelled flight never arrived.
A diverted flight arrived at a different airport.
Neither can have a meaningful arrival delay.
Preserve NULL. Never substitute 0.

**Rule 3 — CANCELLATION_CODE conditions:**
CANCELLATION_CODE is NULL when CANCELLED = 0.
It is populated (A/B/C/D) only when CANCELLED = 1.
A = Carrier, B = Weather, C = NAS, D = Security.

**Rule 4 — Uniqueness:**
Each flight on each date must appear exactly once.
Unique key: FL_DATE + OP_UNIQUE_CARRIER +
OP_CARRIER_FL_NUM + ORIGIN + DEST.
Duplicates deduplicated in Silver layer.

**Rule 5 — Completeness:**
Core identity columns (YEAR, MONTH, ORIGIN, DEST,
OP_UNIQUE_CARRIER) must never be NULL.
Row count per month must exceed 400,000.
Any violation fails the Silver gate loudly.

### Silver Validation Gate

All five rules checked in Silver layer.
Any violation raises a structured error:
[ERROR_TYPE] | Layer | Rule | Violations found |
Expected | Likely cause | Recommended action.
Pipeline halts. Gold layer never receives bad data.

---

## 10. Cost Sensitivity Framework

### Approach

The system provides operational cost context
using real delay patterns + user-supplied
cost assumptions.

**Formula:**
Estimated Cost Exposure =
Total Delayed Minutes (from Gold layer) ×
Cost Per Delay Minute (user input)

### Citation

Industry cost benchmarks sourced from:
Ferguson, J. et al. — "Total Delay Impact Study"
(FAA/NEXTOR, 2010). Figures used as directional
indicators only. Users supply their own
cost-per-minute assumptions.

### Honesty Principle

> This system never fabricates cost numbers.
> All estimates are clearly labelled as estimates.
> Users input their own cost assumptions.
> The system provides the delay data.
> The interpretation is theirs.

---

## 11. What This System Does

- Ingests 3 years of US domestic flight delay data
- Validates data quality across 6 dimensions
- Preserves all NULL values with documented reasoning
- Builds a star schema analytical model
- Surfaces delay patterns by carrier, airport,
  route, time, and delay cause
- Provides cost exposure estimates using
  real delay patterns and user-supplied cost rates
- Tracks Late Aircraft cascade effect
  using TAIL_NUM across a single day
- Maps delay causes to IOC operational pillars
  (Safety, Legality, Efficiency)

---

## 12. What This System Does NOT Do

- Does not predict future delays (descriptive only)
- Does not process non-US or international flights
- Does not connect to live airline systems
- Does not ingest real-time data (batch only, v1)
- Does not store personally identifiable information
- Does not fabricate or impute missing delay values
- Does not provide legal or regulatory compliance advice
- Does not guarantee cost estimates are exact

---

## 13. Future Architecture

### Phase 2 — Cloud Migration (November 2026+)

ADF (ingestion)
↓
ADLS Gen2
├── bronze/
├── silver/
└── gold/
↓
Synapse Spark (PySpark transforms)
↓
Synapse SQL (analytical queries)
↓
Power BI (DirectQuery dashboard)

## 14. Repository Structure

BTS-Aviation-Delay-Intelligence/
│
├── README.md ← Product brief
│
├── docs/
│ ├── HLD.md ← High Level Design
│ ├── data_model.md ← Star schema design
│ └── decisions/
│ ├── ADR-001-surrogate-keys.md
│ ├── ADR-002-star-schema.md
│ ├── ADR-003-scd-type2-carriers.md
│ ├── ADR-004-airport-domain-columns.md
│ ├── ADR-005-null-preservation.md
│ ├── ADR-006-ioc-pillar-mapping.md
│ └── ADR-007-scd-type4-aircraft.md
│
├── governance/
│ └── data_principles.md ← Engineering principles
│
├── pipeline/
│ ├── bronze/
│ │ └── bronze_ingestion.py
│ ├── silver/
│ │ └── silver_transform.py
│ └── gold/
│ └── gold_star_schema.py
│
├── analysis/
│ └── gold_analytics.sql ← 6+ analytical queries
│
├── interface/
│ └── bts_assistant.py ← Cost Sensitivity Calculator
│
├── tests/
│ ├── test_schema.py
│ ├── test_row_counts.py
│ └── test_null_rules.py
│
├── config/
│ └── pipeline_config.py ← Paths, thresholds, settings
│
├── data/
│ └── raw/ ← gitignored
│
├── .gitignore
├── README.md
└── requirements.txt

---

## 15. Engineering Principles

1. **Errors as a UI** — every failure explains
   WHAT broke, WHERE, WHY, and HOW TO FIX.
   Silent failures are not acceptable.

2. **Trust is designed, not added** — governance,
   audit trail, and data quality checks are
   Day 1 architecture decisions.

3. **Preserve, never fabricate** — NULLs carry
   business meaning. Never substitute values
   without documented reasoning.

4. **Read before you build** — domain knowledge
   from Peter J. Bruce's Airline Operations Control
   informed every schema decision in this system.

5. **Idempotency is non-negotiable** — any layer
   can be safely rerun without corrupting
   downstream data or creating duplicates.

6. **Honest scope** — this document states clearly
   what the system does and does not do.
   No inflated claims. No fabricated metrics.

---

> Version 0.2 — Draft Pre-Implementation
> All major decisions tracked through ADRs.
> This document will be updated to Version 1.0
> after Gold layer implementation (August–September 2026)
> and Version 2.0 after validation and performance
> proof (October 2026).
