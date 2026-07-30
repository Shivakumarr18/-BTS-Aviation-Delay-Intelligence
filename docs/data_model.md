# Logical Data Model

**Project:** BTS Aviation Delay Intelligence Platform

Version: 0.2 | Status: Draft — Pre-Implementation
Last updated: July 2026

Note: This document describes the planned logical
data model. Sections marked [PLANNED] will be
updated with verified facts after Gold layer
implementation in August-September 2026.

---

## 1. Purpose

This document describes the logical data model for the
BTS Aviation Delay Intelligence Platform.

It defines the analytical grain, fact and dimension tables,
SCD type decisions, NULL handling rules, index strategy,
and key modeling decisions used to build the Gold layer.

This document will evolve as the Gold layer is implemented.
Major modeling decisions are documented through ADRs.

---

## 2. Analytical Grain

### Grain — Locked

> "One record in fact_delays represents one scheduled
> flight on one calendar day as published by BTS."

**What this means:**

- Flight AA-101 on January 15 2024 = ONE row
- Flight AA-101 on January 16 2024 = DIFFERENT row
- Same flight number, different date = always different rows
- One flight with one origin and one destination per row

**Why this grain:**

- Matches BTS data structure exactly
- Enables route-level, carrier-level, and
  time-series analysis
- Cannot go more atomic without segment-level
  data not available in BTS

---

## 3. Star Schema Overview

```text
                    dim_date
                       │
                       │
dim_carrier ──── fact_delays ──── dim_airport (Origin)
                       │    ──── dim_airport (Destination)
                       │
              dim_delay_reason
                       │
              dim_aircraft
```

**Notes:**

- dim_airport appears TWICE in fact_delays
  (origin_key and dest_key) — conformed dimension
- dim_date is conformed across all future fact tables
- dim_aircraft tracks tail number across flights
  (critical for Late Aircraft cascade analysis)

---

## 4. SCD Type Decisions

| Dimension        | SCD Type | Reason                                      |
| ---------------- | -------- | ------------------------------------------- |
| dim_carrier      | Type 2   | Carrier names and hubs change (mergers)     |
| dim_airport      | Type 2   | Hub status and city classification changes  |
| dim_date         | Static   | Calendar dates never change                 |
| dim_delay_reason | Type 1   | BTS delay categories are stable definitions |
| dim_aircraft     | Type 4   | Tail numbers change registration frequently |

**SCD Type 2 extra columns (dim_carrier, dim_airport):**

- effective_from DATE NOT NULL
- effective_to DATE NOT NULL (9999-12-31 = currently active)
- is_current TINYINT NOT NULL (1 = current, 0 = historical)

**SCD Type 4 (dim_aircraft):**

- Current table holds one row per tail number (latest config)
- History table holds all versions over time
- Queried separately depending on need

---

## 5. Fact Table — fact_delays

**Grain:** One scheduled flight per calendar day
**Rows:** ~18 million (2023-2025)
**Partitioned by:** year + month (36 partitions)

| Column              | Type      | Source              | Notes                                                |
| ------------------- | --------- | ------------------- | ---------------------------------------------------- |
| date_key            | INT       | Generated           | FK → dim_date.date_key                               |
| carrier_key         | INT       | Generated           | FK → dim_carrier.carrier_key (SCD Type 2)            |
| origin_key          | INT       | Generated           | FK → dim_airport.airport_key                         |
| dest_key            | INT       | Generated           | FK → dim_airport.airport_key                         |
| aircraft_key        | INT       | Generated           | FK → dim_aircraft.aircraft_key (-1 = Unknown)        |
| delay_reason_key    | INT       | Generated           | FK → dim_delay_reason.reason_key (-1 = No Delay)     |
| fl_date             | DATE      | FL_DATE             | Actual flight date. Used for partition pruning       |
| flight_number       | VARCHAR   | OP_CARRIER_FL_NUM   | Natural identifier. Kept for traceability            |
| crs_dep_time        | INT       | CRS_DEP_TIME        | Scheduled departure (HHMM format)                    |
| dep_time            | INT       | DEP_TIME            | Actual departure. NULL if cancelled                  |
| dep_delay           | DOUBLE    | DEP_DELAY           | Minutes early (negative) or late. NULL if cancelled  |
| dep_delay_minutes   | DOUBLE    | DEP_DELAY_NEW       | Absolute delay. 0 if early. NULL if cancelled        |
| dep_del15           | TINYINT   | DEP_DEL15           | 1 = delayed 15+ mins. 0 = on time. NULL if cancelled |
| crs_arr_time        | INT       | CRS_ARR_TIME        | Scheduled arrival (HHMM format)                      |
| arr_time            | INT       | ARR_TIME            | Actual arrival. NULL if cancelled or diverted        |
| arr_delay           | DOUBLE    | ARR_DELAY           | Minutes early or late. NULL if cancelled             |
| arr_delay_minutes   | DOUBLE    | ARR_DELAY_NEW       | Absolute delay. 0 if early. NULL if cancelled        |
| arr_del15           | TINYINT   | ARR_DEL15           | 1 = delayed 15+ mins. 0 = on time. ← CRITICAL        |
| cancelled           | TINYINT   | CANCELLED           | 1 = cancelled. 0 = operated                          |
| cancellation_code   | VARCHAR   | CANCELLATION_CODE   | A/B/C/D or NULL when not cancelled                   |
| diverted            | TINYINT   | DIVERTED            | 1 = diverted. 0 = not diverted                       |
| crs_elapsed_time    | DOUBLE    | CRS_ELAPSED_TIME    | Scheduled duration (minutes)                         |
| actual_elapsed_time | DOUBLE    | ACTUAL_ELAPSED_TIME | Actual duration. NULL if cancelled                   |
| air_time            | DOUBLE    | AIR_TIME            | Wheels off to wheels on. NULL if cancelled           |
| distance            | DOUBLE    | DISTANCE            | Route distance in miles. Never NULL                  |
| carrier_delay       | DOUBLE    | CARRIER_DELAY       | NULL when arr_del15 = 0. Expected 80.1% NULL         |
| weather_delay       | DOUBLE    | WEATHER_DELAY       | NULL when arr_del15 = 0. Expected 80.1% NULL         |
| nas_delay           | DOUBLE    | NAS_DELAY           | NULL when arr_del15 = 0. Expected 80.1% NULL         |
| security_delay      | DOUBLE    | SECURITY_DELAY      | NULL when arr_del15 = 0. Expected 80.1% NULL         |
| late_aircraft_delay | DOUBLE    | LATE_AIRCRAFT_DELAY | NULL when arr_del15 = 0. Expected 80.1% NULL         |
| pipeline_load_dt    | TIMESTAMP | Pipeline            | When row was loaded. Used for watermark tracking     |

**Critical business rule:**

> When arr_del15 = 0, all five delay cause columns
> MUST be NULL. Validated on Q1 2024: 0 violations
> across 1,658,259 rows. This is correct behaviour —
> not dirty data. Never fill with 0.

---

## 6. Dimension Tables

### dim_carrier (SCD Type 2)

| Column         | Type    | Notes                                            |
| -------------- | ------- | ------------------------------------------------ |
| carrier_key    | INT     | Surrogate PK (auto-increment)                    |
| carrier_code   | VARCHAR | IATA 2-letter code e.g. "AA". Natural key stored |
| carrier_name   | VARCHAR | Full airline name                                |
| carrier_type   | VARCHAR | "Legacy", "LCC", "Regional"                      |
|                |         | Enables hub vs LCC cascade analysis              |
|                |         | (Chapter 3: same delay code, different story)    |
| hub_airport    | VARCHAR | Primary hub IATA code                            |
| effective_from | DATE    | SCD Type 2 — version start date                  |
| effective_to   | DATE    | SCD Type 2 — 9999-12-31 = currently active       |
| is_current     | TINYINT | 1 = current version, 0 = historical              |

**Index:** carrier_code, is_current

---

### dim_airport (SCD Type 2, Conformed)

| Column           | Type    | Notes                                        |
| ---------------- | ------- | -------------------------------------------- |
| airport_key      | INT     | Surrogate PK                                 |
| airport_code     | VARCHAR | IATA 3-letter code e.g. "HYD". Natural key   |
| airport_name     | VARCHAR | Full airport name                            |
| city_name        | VARCHAR | City                                         |
| state_code       | VARCHAR | 2-letter state code                          |
| state_name       | VARCHAR | Full state name                              |
| latitude         | DECIMAL | Geographic coordinate                        |
| longitude        | DECIMAL | Geographic coordinate                        |
| elevation_ft     | INT     | Altitude above sea level in feet             |
|                  |         | Chapter 3: DEN at 5,400ft reduces engine     |
|                  |         | performance → higher CARRIER_DELAY in summer |
| slot_coordinated | TINYINT | 1 = IATA Level 3 slot airport                |
|                  |         | Chapter 3: JFK, LAX, ORD — IOC obsessed with |
|                  |         | slot compliance. Missing slot = NAS_DELAY    |
| has_curfew       | TINYINT | 1 = nighttime curfew applies                 |
|                  |         | Chapter 3: IOC cancels late flights rather   |
|                  |         | than risk curfew violation → CANCELLED=1     |
|                  |         | Pattern: late evening cancellations at JFK   |
| effective_from   | DATE    | SCD Type 2                                   |
| effective_to     | DATE    | SCD Type 2 — 9999-12-31 = currently active   |
| is_current       | TINYINT | 1 = current version, 0 = historical          |

**Index:** airport_code, is_current

**Note on aviation domain columns:**

> elevation_ft, slot_coordinated, has_curfew are NOT
> in BTS source data. They are enriched from public
> airport databases. These columns exist because of
> domain knowledge from Peter J. Bruce's Airline
> Operations Control — Chapter 3 directly informed
> this schema design decision.

---

### dim_date (Static — never changes)

| Column        | Type    | Notes                                        |
| ------------- | ------- | -------------------------------------------- |
| date_key      | INT     | Surrogate PK. Format: YYYYMMDD e.g. 20240115 |
| full_date     | DATE    | Calendar date                                |
| year          | INT     | Calendar year                                |
| quarter       | INT     | 1–4                                          |
| month         | INT     | 1–12                                         |
| month_name    | VARCHAR | "January", "February" etc.                   |
| day_of_month  | INT     | 1–31                                         |
| day_of_week   | INT     | 1 = Monday, 7 = Sunday                       |
| day_name      | VARCHAR | "Monday", "Tuesday" etc.                     |
| is_weekend    | TINYINT | 1 = Saturday or Sunday, 0 = weekday          |
| is_us_holiday | TINYINT | 1 = US federal holiday or peak travel day    |
|               |         | Thanksgiving, Christmas = peak delays        |
| season        | VARCHAR | "Winter", "Spring", "Summer", "Fall"         |
| week_of_year  | INT     | 1–53                                         |

**Population:**

> Generate once: 2020-01-01 to 2030-12-31 = 3,653 rows.
> Load once. Never update. Never delete.

**Why pre-compute these attributes:**

> "Show delays by day of week" without dim_date requires
> EXTRACT(DOW FROM fl_date) computed across 18M rows
> at query time — expensive. With dim_date: join on
> date_key, filter day_of_week = 1 — milliseconds.

---

### dim_delay_reason (SCD Type 1)

| Column               | Type    | Notes                                                                                                   |
| -------------------- | ------- | ------------------------------------------------------------------------------------------------------- |
| reason_key           | INT     | Surrogate PK (-1 = No Delay)                                                                            |
| reason_code          | VARCHAR | BTS delay code (CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY, NO_DELAY) |
| reason_name          | VARCHAR | Human-readable delay category                                                                           |
| ioc_pillar           | VARCHAR | IOC operational pillar: Safety, Legality, Efficiency, or N/A                                            |
| airline_controllable | BOOLEAN | 1 = airline-controlled, 0 = external factor                                                             |
| description          | VARCHAR | Operational explanation of the delay                                                                    |

### Business Mapping

| BTS Delay Code      | IOC Pillar | Airline Controllable |
| ------------------- | ---------- | -------------------- |
| CARRIER_DELAY       | Efficiency | Yes                  |
| LATE_AIRCRAFT_DELAY | Efficiency | Yes                  |
| WEATHER_DELAY       | Safety     | No                   |
| NAS_DELAY           | Legality   | No                   |
| SECURITY_DELAY      | Legality   | No                   |
| NO_DELAY            | N/A        | No                   |

**Sample data:**

| reason_key | reason_code         | ioc_pillar | airline_controllable | description                                          |
| ---------- | ------------------- | ---------- | -------------------- | ---------------------------------------------------- |
| -1         | NO_DELAY            | N/A        | 0                    | Flight operated on time. arr_del15 = 0               |
| 1          | CARRIER_DELAY       | Efficiency | 1                    | Airline ops: MEL fault, crew breach, GPU failure     |
| 2          | WEATHER_DELAY       | Safety     | 0                    | IOC chose Safety over Efficiency — unsafe to operate |
| 3          | NAS_DELAY           | Legality   | 0                    | ATC/regulatory constraint — airline had no control   |
| 4          | SECURITY_DELAY      | Legality   | 0                    | Regulatory compliance — security screening delay     |
| 5          | LATE_AIRCRAFT_DELAY | Efficiency | 1                    | Cascade: previous flight late, same aircraft delayed |

**Note on ioc_pillar column:**

> This column is unique to this project. No standard BTS
> pipeline includes IOC pillar mapping. It exists because
> domain knowledge from Peter J. Bruce (Chapter 1) was
> applied directly to schema design. This enables queries
> like "what % of delays are Safety-pillar driven vs
> Efficiency-pillar driven?" — an insight no generic
> analytics pipeline can produce.

---

### dim_aircraft (SCD Type 4)

**Current table — dim_aircraft (one row per tail number):**

| Column         | Type    | Notes                                           |
| -------------- | ------- | ----------------------------------------------- |
| aircraft_key   | INT     | Surrogate PK (-1 = Unknown tail number)         |
| tail_number    | VARCHAR | BTS TAIL_NUM. 6,570 NULLs in Q1 2024 → key = -1 |
| aircraft_type  | VARCHAR | "B737", "A320", "B777" etc.                     |
| manufacturer   | VARCHAR | "Boeing", "Airbus" etc.                         |
| seat_config    | INT     | Approximate seat count                          |
| effective_from | DATE    | When this configuration became active           |
| is_current     | TINYINT | 1 = current configuration                       |

**History table — dim_aircraft_history:**

| Column         | Type    | Notes                          |
| -------------- | ------- | ------------------------------ |
| history_key    | INT     | Surrogate PK                   |
| aircraft_key   | INT     | FK → dim_aircraft.aircraft_key |
| tail_number    | VARCHAR | Tail number for this version   |
| aircraft_type  | VARCHAR | Aircraft type for this version |
| manufacturer   | VARCHAR | Manufacturer for this version  |
| effective_from | DATE    | When this version started      |
| effective_to   | DATE    | When this version ended        |

**Special record:**

> aircraft_key = -1, tail_number = "UNKNOWN"
> Handles 6,570 NULL tail numbers in Q1 2024 BTS data.
> These rows are preserved — never dropped.
> NULL TAIL_NUM is expected for some BTS records.

**Why dim_aircraft matters:**

> Tracking TAIL_NUM across a single day reveals the
> Late Aircraft cascade effect. Aircraft N131EV delayed
> at 8am propagates through its 10am, 1pm, and 4pm
> flights. This is the most powerful insight the Gold
> layer can surface — and it requires dim_aircraft.

---

## 7. NULL Handling Rules

| Column              | NULL Meaning              | Handling      | Validation Rule                     |
| ------------------- | ------------------------- | ------------- | ----------------------------------- |
| ARR_DELAY           | Cancelled or diverted     | Preserve NULL | NULL when CANCELLED=1 or DIVERTED=1 |
| DEP_DELAY           | Cancelled                 | Preserve NULL | NULL when CANCELLED=1               |
| CANCELLATION_CODE   | Flight not cancelled      | Expected NULL | NULL when CANCELLED=0               |
| ARR_DEL15           | Cancelled or diverted     | Preserve NULL | NULL when CANCELLED=1 or DIVERTED=1 |
| CARRIER_DELAY       | No delay (flight on time) | Preserve NULL | Must be NULL when ARR_DEL15=0 ✅    |
| WEATHER_DELAY       | No delay (flight on time) | Preserve NULL | Must be NULL when ARR_DEL15=0 ✅    |
| NAS_DELAY           | No delay (flight on time) | Preserve NULL | Must be NULL when ARR_DEL15=0 ✅    |
| SECURITY_DELAY      | No delay (flight on time) | Preserve NULL | Must be NULL when ARR_DEL15=0 ✅    |
| LATE_AIRCRAFT_DELAY | No delay (flight on time) | Preserve NULL | Must be NULL when ARR_DEL15=0 ✅    |
| TAIL_NUM            | Not reported by airline   | Preserve NULL | Use aircraft_key = -1 (Unknown)     |

**Validation confirmed:**

> ARR_DEL15=0 → delay cause columns NULL rule
> validated on Q1 2024 BTS data: 1,658,259 rows,
> 0 violations. This is correct behaviour — 80.1%
> of flights are on time and have NULL delay causes.
> These are NOT data quality issues.

### Principle

> NULL values are preserved whenever they carry
> business meaning. They are never replaced with 0
> or any substitute value simply to simplify
> downstream analytics. Replacing ARR_DELAY NULL
> with 0 would imply cancelled flights arrived on
> time — which is operationally incorrect and would
> corrupt every delay metric downstream.

---

## 8. Index Strategy

Indexes are mandatory at 18M rows.
Without indexes every dashboard query scans
the full fact table. With indexes: milliseconds.

| Table       | Index Columns            | Reason                            |
| ----------- | ------------------------ | --------------------------------- |
| fact_delays | carrier_key              | Filter/group by airline           |
| fact_delays | origin_key               | Filter by departure airport       |
| fact_delays | dest_key                 | Filter by arrival airport         |
| fact_delays | date_key                 | Filter by time period             |
| fact_delays | arr_del15                | Filter delayed vs on-time flights |
| fact_delays | cancelled                | Filter cancelled flights          |
| dim_carrier | carrier_code, is_current | Fast carrier lookup by code       |
| dim_airport | airport_code, is_current | Fast airport lookup by code       |

---

## 9. Partitioning Strategy

**Partition key:** year + month
**Number of partitions:** 36 (Jan 2023 → Dec 2025)
**Rows per partition:** ~500,000

**Path structure:**

**Why year + month (not day, not carrier):**

| Option          | Partitions | Rows/partition | Decision      |
| --------------- | ---------- | -------------- | ------------- |
| By day          | 1,095      | ~18,000        | Too small     |
| By year + month | 36         | ~500,000       | ✅ Correct    |
| By carrier      | ~20        | ~900,000       | Hot-spot risk |
| No partitioning | 1          | 18,000,000     | Too large     |

**Partition pruning benefit:**

> Query for January 2024 → scans only
> year=2024/month=01/ partition (~500K rows).
> Without partitioning → scans all 18M rows.
> Estimated 30x query speedup on time-filtered queries.

**Idempotency connection:**

> DELETE partition + INSERT pattern in Silver and Gold
> uses partition boundaries as the unit of reprocessing.
> Reprocessing January 2024: delete year=2024/month=01/,
> rerun transform, write clean partition.
> Safe to retry. No duplicates. No data drift.

---

## 10. Modeling Decisions

### Why surrogate keys?

Surrogate keys provide stable identifiers independent
of source system changes. Carrier code "US" became "AA"
after the American-US Airways merger. Surrogate keys
isolate the warehouse from such changes. Joining on
INT is also faster than VARCHAR at 18M row scale.

### Why Kimball star schema over snowflake?

The analytical workload is read-heavy. Dashboard queries
aggregate by carrier, airport, time, and delay reason.
Star schema provides simpler joins (one hop to each
dimension) and better BI performance than snowflake
(which adds extra joins for normalized sub-dimensions).
Query patterns here are aggregations — not deep
hierarchy traversal. Star wins.

### Why monthly partitioning?

BTS data is naturally organized by month (one CSV per
month). Monthly partitions match the natural data
boundary, enable partition pruning for time-filtered
queries, and make idempotent reprocessing clean
(delete one month, reprocess one month).

### Why SCD Type 2 for carriers and airports?

Historical accuracy requires knowing what the carrier
or airport looked like at the time of the flight —
not today. If American Airlines changes its hub from
DFW to ORD, we need to know which hub was active for
2023 flights vs 2025 flights. Type 2 preserves this.

### Why dim_aircraft uses SCD Type 4?

Tail numbers change configuration, get sold between
airlines, and get reassigned. Type 2 would create
many rows per tail number over time, making the
dimension table large and joins expensive. Type 4
keeps the current table lean (one row per aircraft)
and moves history to a separate table queried only
when needed.

### Why ioc_pillar in dim_delay_reason?

Domain knowledge from Chapter 1 of Airline Operations
Control by Peter J. Bruce maps each delay cause to
one of three IOC decision pillars: Safety, Legality,
Efficiency. This enables analytical queries impossible
in any standard BTS pipeline: "What proportion of
delays are Safety-driven vs Efficiency-driven by
carrier?" This column exists because we read before
we built.

---

## 11. Current Assumptions

- One flight = one fact record (grain locked)
- BTS delay definitions are treated as authoritative
- Historical BTS files remain unchanged after publication
- Cost calculations handled separately (not in data model)
- Aircraft enrichment data sourced from public databases
- dim_date populated for 2020-2030 and never updated

---

## 12. Future Evolution

The following will be finalized during Gold layer implementation:

- Surrogate key generation strategy (sequence vs hash)
- Final referential integrity constraints
- Physical storage optimization per layer
- Full ER diagram with finalized relationships

Major design decisions tracked through ADRs:

- ADR-001: Why surrogate keys over natural keys
- ADR-002: Why star schema over snowflake
- ADR-003: Why SCD Type 2 for dim_carrier
- ADR-004: Aviation domain columns in dim_airport
- ADR-005: NULL preservation policy (80.1% pattern)
- ADR-006: Why ioc_pillar in dim_delay_reason
- ADR-007: Why SCD Type 4 for dim_aircraft

---

> This document represents the current logical data model.
> All significant modeling changes tracked through ADRs.
> Every decision has a WHY. No decision is arbitrary.
