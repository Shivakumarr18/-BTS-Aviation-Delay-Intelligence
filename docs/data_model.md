# Logical Data Model

**Project:** BTS Aviation Delay Intelligence Platform

Version: 0.1 | Date: July 26, 2026 | Status: Draft

---

## 1. Purpose

This document describes the logical data model for the BTS Aviation Delay Intelligence Platform.

It defines the analytical grain, fact and dimension tables, NULL handling rules, and key modeling decisions used to build the Gold layer.

This document will evolve as the Gold layer is implemented. Major modeling decisions will be documented through ADRs.

## 2. Analytical Grain

### Grain

One record in fact_flights represents one scheduled flight published by the BTS dataset.

All measures, dimensions, and business metrics are modeled at this level.

## 3. Logical Data Model

> **Status:** Draft conceptual model. Final schema, relationships, and attributes will be finalized during Gold layer implementation.

```text
                dim_date
                    │
                    │

dim_carrier ─── fact_flights ─── dim_airport (Origin)
                    │
                    │
        dim_airport (Destination)
                    │
                    │
            dim_delay_reason
```

This conceptual logical model represents the planned Gold layer schema. The complete ER diagram with finalized attributes and relationships will be added during Gold layer implementation.

## 4. Fact Table — fact_flights

| Column              | Type      | Source              | Notes                   |
| ------------------- | --------- | ------------------- | ----------------------- |
| flight_key          | INT       | Generated           | Surrogate Primary Key   |
| carrier_key         | INT       | Generated           | FK → dim_carrier        |
| origin_key          | INT       | Generated           | FK → dim_airport        |
| destination_key     | INT       | Generated           | FK → dim_airport        |
| date_key            | INT       | Generated           | FK → dim_date           |
| dep_delay           | DOUBLE    | DEP_DELAY           | NULL when cancelled     |
| arr_delay           | DOUBLE    | ARR_DELAY           | NULL when cancelled     |
| distance            | DOUBLE    | DISTANCE            | Miles                   |
| is_cancelled        | INT       | CANCELLED           | 0 or 1                  |
| cancellation_code   | STRING    | CANCELLATION_CODE   | A/B/C/D or NULL         |
| carrier_delay       | DOUBLE    | CARRIER_DELAY       | Delay minutes           |
| weather_delay       | DOUBLE    | WEATHER_DELAY       | Delay minutes           |
| nas_delay           | DOUBLE    | NAS_DELAY           | Delay minutes           |
| security_delay      | DOUBLE    | SECURITY_DELAY      | Delay minutes           |
| late_aircraft_delay | DOUBLE    | LATE_AIRCRAFT_DELAY | Delay minutes           |
| ingestion_ts        | TIMESTAMP | Pipeline            | ETL ingestion timestamp |

## 5. Dimension Tables

### dim_carrier

| Column       | Type   | Notes                 |
| ------------ | ------ | --------------------- |
| carrier_key  | INT    | Surrogate Primary Key |
| carrier_code | STRING | e.g. AA, DL, WN       |
| carrier_name | STRING | Airline name          |

### dim_airport

| Column       | Type   | Notes                 |
| ------------ | ------ | --------------------- |
| airport_key  | INT    | Surrogate Primary Key |
| airport_code | STRING | Airport code          |
| city         | STRING | City                  |
| state        | STRING | State                 |

### dim_date

| Column       | Type   | Notes                           |
| ------------ | ------ | ------------------------------- |
| date_key     | INT    | Surrogate Primary Key           |
| full_date    | DATE   | Calendar date                   |
| year         | INT    | Calendar year                   |
| month        | INT    | Month                           |
| day_of_month | INT    | Day                             |
| day_of_week  | INT    | 1–7                             |
| quarter      | INT    | Quarter                         |
| season       | STRING | WINTER / SPRING / SUMMER / FALL |

### dim_delay_reason

| Column          | Type    | Notes                                                  |
| --------------- | ------- | ------------------------------------------------------ |
| reason_key      | INT     | Surrogate Primary Key                                  |
| reason_code     | STRING  | CARRIER / WEATHER / NAS / SECURITY / LATE_AIRCRAFT     |
| description     | STRING  | Human-readable explanation                             |
| is_controllable | BOOLEAN | Business classification (derived, not supplied by BTS) |

## 6. NULL Handling Rules

| Column              | NULL Meaning           | Handling      |
| ------------------- | ---------------------- | ------------- |
| ARR_DELAY           | Flight cancelled       | Preserve NULL |
| DEP_DELAY           | Flight cancelled       | Preserve NULL |
| CANCELLATION_CODE   | Flight not cancelled   | Expected NULL |
| CARRIER_DELAY       | No carrier delay       | Expected NULL |
| WEATHER_DELAY       | No weather delay       | Expected NULL |
| NAS_DELAY           | No NAS delay           | Expected NULL |
| SECURITY_DELAY      | No security delay      | Expected NULL |
| LATE_AIRCRAFT_DELAY | No late aircraft delay | Expected NULL |

### Principle

NULL values are preserved whenever they represent business meaning. They are never replaced simply to simplify downstream analytics.

## 7. Modeling Decisions

### Why surrogate keys?

Surrogate keys provide stable identifiers for warehouse entities, simplify joins, and isolate downstream analytical models from changes in source-system identifiers.

### Why a Kimball star schema?

The analytical workload is primarily read-heavy.

A star schema provides:

- Simpler analytical queries
- Reusable dimensions
- Better BI performance
- Easier long-term extensibility

### Why monthly partitioning?

The BTS dataset is naturally organized by month, and most analytical queries are time-based.

Monthly partitioning enables partition pruning, reducing unnecessary data scans for time-filtered workloads.

## 8. Current Assumptions

- One flight corresponds to one fact record.
- BTS delay definitions are treated as authoritative.
- Historical BTS files remain unchanged after publication.
- Cost calculations are handled separately from the logical data model.

## 9. Future Evolution

The following items will be finalized during Gold layer implementation:

- Surrogate key generation strategy
- Final dimension attributes
- Referential integrity constraints
- Physical storage optimization
- Indexing and partition strategy

Major design decisions will be documented through ADRs.

### Note

This document represents the current logical data model and will evolve as the Gold layer is implemented. All significant modeling changes will be tracked through Architecture Decision Records (ADRs).
