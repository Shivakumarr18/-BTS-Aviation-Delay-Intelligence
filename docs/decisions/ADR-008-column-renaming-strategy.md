# ADR-008: Column Renaming Strategy

## Context

BTS source columns use cryptic abbreviations:
ARR_DEL15, OP_UNIQUE_CARRIER, CRS_DEP_TIME etc.
These are difficult to read for analysts and
new engineers joining the project.

## Decision

Bronze layer preserves ALL original BTS column
names exactly as received from source.
Silver layer renames all columns to human-readable
business names with explicit units where applicable.

## Column Mapping

| Bronze (BTS Original) | Silver (Business Name)   |
| --------------------- | ------------------------ |
| YEAR                  | flight_year              |
| MONTH                 | flight_month             |
| DAY_OF_MONTH          | flight_day               |
| DAY_OF_WEEK           | day_of_week              |
| FL_DATE               | flight_date              |
| OP_UNIQUE_CARRIER     | carrier_code             |
| TAIL_NUM              | tail_number              |
| OP_CARRIER_FL_NUM     | flight_number            |
| ORIGIN                | origin_airport           |
| ORIGIN_CITY_NAME      | origin_city              |
| ORIGIN_STATE_ABR      | origin_state             |
| DEST                  | dest_airport             |
| DEST_CITY_NAME        | dest_city                |
| DEST_STATE_ABR        | dest_state               |
| CRS_DEP_TIME          | scheduled_dep_time       |
| DEP_TIME              | actual_dep_time          |
| DEP_DELAY             | dep_delay_mins           |
| DEP_DELAY_NEW         | dep_delay_abs_mins       |
| DEP_DEL15             | dep_delayed_flag         |
| CRS_ARR_TIME          | scheduled_arr_time       |
| ARR_TIME              | actual_arr_time          |
| ARR_DELAY             | arr_delay_mins           |
| ARR_DELAY_NEW         | arr_delay_abs_mins       |
| ARR_DEL15             | arr_delayed_flag         |
| CANCELLED             | is_cancelled             |
| CANCELLATION_CODE     | cancellation_code        |
| DIVERTED              | is_diverted              |
| CRS_ELAPSED_TIME      | scheduled_elapsed_mins   |
| ACTUAL_ELAPSED_TIME   | actual_elapsed_mins      |
| AIR_TIME              | air_time_mins            |
| FLIGHTS               | flights                  |
| DISTANCE              | distance_miles           |
| CARRIER_DELAY         | carrier_delay_mins       |
| WEATHER_DELAY         | weather_delay_mins       |
| NAS_DELAY             | nas_delay_mins           |
| SECURITY_DELAY        | security_delay_mins      |
| LATE_AIRCRAFT_DELAY   | late_aircraft_delay_mins |

## Naming Principles

→ No abbreviations — full readable words
→ Units explicit in name (_mins, \_miles, \_flag)
→ Boolean columns prefixed with is_
→ Time columns suffixed with \_time
→ Delay columns suffixed with \_mins

## Consequences

→ Bronze and Silver have different column names
→ Full mapping documented here for traceability
→ Any analyst can trace Silver name back to
Bronze name back to BTS source column
→ Silver code uses this ADR as reference

## Data Type Changes — Bronze to Silver

### Why types change in Silver:

Bronze preserves raw BTS types exactly.
Silver corrects them to production types.
inferSchema guesses incorrectly on NULL-heavy
columns — promoting integers to doubles.
Silver fixes this explicitly.

| Column    | Bronze Type | Silver Type | Why Changed                          |
| --------- | ----------- | ----------- | ------------------------------------ |
| FL_DATE   | StringType  | DateType    | BTS stores as quoted string          |
| DEP_DEL15 | DoubleType  | IntegerType | Binary flag 0/1 — Double unnecessary |
| ARR_DEL15 | DoubleType  | IntegerType | Binary flag 0/1 — Double unnecessary |
| CANCELLED | DoubleType  | IntegerType | Binary flag 0/1 — Double unnecessary |
| DIVERTED  | DoubleType  | IntegerType | Binary flag 0/1 — Double unnecessary |
| FLIGHTS   | DoubleType  | DROPPED     | Constant 1.0 — no value (ADR-010)    |

### All other columns:

No type changes needed.
Types are correct in Bronze.
Silver preserves them as-is.
