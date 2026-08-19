"""
BTS Aviation Delay Intelligence System
=======================================
Script  : silver_transform.py
Layer   : Silver
Author  : Narsing Shiva Kumar

Purpose:
    Transform Bronze Parquet into a trusted, validated analytical dataset.
    Silver is the trust boundary of the platform.
    Gold receives ONLY Silver-validated data.
    All 36 partitions must pass all gates before Gold runs.

Standard:
    Every failure explains WHAT broke, WHERE, WHY, and HOW TO FIX.
    Silent failures are not acceptable. Errors are a UI.

Grain:
    One row = one scheduled flight on one calendar day.
    Unchanged from Bronze. Silver transforms -- never re-grains.

Run:
    .venv/Scripts/python.exe pipeline/silver/silver_transform.py

Input:
    data/bronze/YEAR=YYYY/MONTH=MM/
    Parquet format. 38 columns (37 BTS + ingestion_ts).

Output:
    data/silver/flight_year=YYYY/flight_month=MM/
    Parquet format. 37 columns (36 renamed + silver_processed_ts).

Column count:
    Bronze  : 38 (37 BTS + ingestion_ts)
    Silver  : 37 (37 renamed - FLIGHTS - ingestion_ts + silver_processed_ts)
    Dropped : FLIGHTS (constant 1.0, ADR-010), ingestion_ts (Bronze audit)
    Added   : silver_processed_ts (Silver audit trail)

FL_DATE Format Discovery (August 10 2026):
    BTS published FL_DATE in TWO observed formats across 36 files:
    Format 1: "M/d/yyyy h:mm:ss a"   e.g. "1/4/2023 12:00:00 AM"
    Format 2: "MM-dd-yyyy HH:mm"     e.g. "12-02-2024 00:00"
    Silver normalises both to DateType using coalesce().
    A third defensive parser "M-d-yyyy HH:mm" handles non-zero-padded
    hyphenated dates if they appear in future files.
    Gate 06 catches any new format not yet handled.
    Gate 06b independently verifies parsed date against YEAR/MONTH/DAY_OF_MONTH.

Idempotency -- DELETE + INSERT with documented limitation:
    Full sequence: Read -> validate source -> transform -> validate Silver
                   -> DELETE existing partition -> APPEND replacement.
    DELETE only happens AFTER the replacement data passes all validation.
    Provides rerun idempotency. Does NOT provide atomic replacement.
    Limitation: crash between DELETE and APPEND loses partition.
    Future: Delta Lake / Azure replaceWhere for atomic replacement.

Validation Architecture -- Two Passes:
    Gate 00  : Schema contract (input column presence + YEAR/MONTH restored)
    Pass 1   : Source validation on df_bronze (Gates 01-05b)
    Pass 2   : Post-transformation on df_silver (Gates 06-09)
    SCG      : Silver Completion Gate after all 36 partitions (physical + row count)

    Principle: valid input does not guarantee valid output.
    Pass 1 validates the source. Pass 2 validates the artifact Gold receives.

Gold Contract:
    Exactly 36 Silver partitions must exist and pass all gates.
    35/36 != Gold ready.
    Silver Completion Gate (SCG) performs final physical reconciliation.

Key ADRs:
    ADR-008: Column renaming -- Silver only
    ADR-009: Parquet over CSV
    ADR-010: FLIGHTS column dropped in Silver
"""

import os
import sys
import glob
import shutil
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, to_date, to_timestamp,
    coalesce, current_timestamp,
    year as spark_year,
    month as spark_month,
    dayofmonth as spark_dayofmonth
)
from pyspark.sql.types import IntegerType
from pyspark.storagelevel import StorageLevel

# ── Environment ───────────────────────────────────────────────────────────────
os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PATH"]                  = os.environ["PATH"] + r";C:\hadoop\bin"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# ── Constants ─────────────────────────────────────────────────────────────────
BRONZE_PATH            = "data/bronze/"
SILVER_PATH            = "data/silver/"
MIN_ROWS_PER_PARTITION = 400_000
EXPECTED_PARTITIONS    = 36
EXPECTED_YEARS         = [2023, 2024, 2025]
EXPECTED_TOTAL_ROWS    = 20_928_599

# ── Column Rename Map (ADR-008) ───────────────────────────────────────────────
COLUMN_RENAME_MAP = {
    "YEAR"                : "flight_year",
    "MONTH"               : "flight_month",
    "DAY_OF_MONTH"        : "flight_day",
    "DAY_OF_WEEK"         : "day_of_week",
    "FL_DATE"             : "flight_date",
    "OP_UNIQUE_CARRIER"   : "carrier_code",
    "TAIL_NUM"            : "tail_number",
    "OP_CARRIER_FL_NUM"   : "flight_number",
    "ORIGIN"              : "origin_airport",
    "ORIGIN_CITY_NAME"    : "origin_city",
    "ORIGIN_STATE_ABR"    : "origin_state",
    "DEST"                : "dest_airport",
    "DEST_CITY_NAME"      : "dest_city",
    "DEST_STATE_ABR"      : "dest_state",
    "CRS_DEP_TIME"        : "scheduled_dep_time",
    "DEP_TIME"            : "actual_dep_time",
    "DEP_DELAY"           : "dep_delay_mins",
    "DEP_DELAY_NEW"       : "dep_delay_abs_mins",
    "DEP_DEL15"           : "dep_delayed_flag",
    "CRS_ARR_TIME"        : "scheduled_arr_time",
    "ARR_TIME"            : "actual_arr_time",
    "ARR_DELAY"           : "arr_delay_mins",
    "ARR_DELAY_NEW"       : "arr_delay_abs_mins",
    "ARR_DEL15"           : "arr_delayed_flag",
    "CANCELLED"           : "is_cancelled",
    "CANCELLATION_CODE"   : "cancellation_code",
    "DIVERTED"            : "is_diverted",
    "CRS_ELAPSED_TIME"    : "scheduled_elapsed_mins",
    "ACTUAL_ELAPSED_TIME" : "actual_elapsed_mins",
    "AIR_TIME"            : "air_time_mins",
    "DISTANCE"            : "distance_miles",
    "CARRIER_DELAY"       : "carrier_delay_mins",
    "WEATHER_DELAY"       : "weather_delay_mins",
    "NAS_DELAY"           : "nas_delay_mins",
    "SECURITY_DELAY"      : "security_delay_mins",
    "LATE_AIRCRAFT_DELAY" : "late_aircraft_delay_mins",
}

COLUMNS_TO_DROP = ["FLIGHTS", "ingestion_ts"]

MANDATORY_COLUMNS = [
    "YEAR", "MONTH", "FL_DATE",
    "OP_UNIQUE_CARRIER", "ORIGIN", "DEST",
    "CANCELLED", "DIVERTED", "DISTANCE"
]

DELAY_CAUSE_COLUMNS = [
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
]

GRAIN_KEY_COLUMNS = [
    "flight_date", "carrier_code", "flight_number",
    "origin_airport", "dest_airport"
]

EXPECTED_BRONZE_COLUMNS = set(COLUMN_RENAME_MAP.keys()) | \
                           {"FLIGHTS", "ingestion_ts"}

# Raw flag columns -- validated BEFORE casting (validate-before-cast principle)
# WHY validate before cast:
# CANCELLED=1.7 casts to is_cancelled=1 -- domain violation silently lost.
# Validate raw DoubleType values first. Cast only after source domain confirmed.
RAW_FLAG_COLUMNS = {
    "CANCELLED" : [0.0, 1.0],        # never NULL
    "DIVERTED"  : [0.0, 1.0],        # never NULL
    "ARR_DEL15" : [0.0, 1.0, None],  # NULL when cancelled/diverted
    "DEP_DEL15" : [0.0, 1.0, None],  # NULL when cancelled
}


# ── Logging ───────────────────────────────────────────────────────────────────

def log_info(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] INFO  : {msg}")

def log_pass(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] PASS  : {msg}")

def log_warn(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] WARN  : {msg}")

def log_fail(what: str, where: str, why: str, fix: str) -> None:
    """Structured failure. Every failure is actionable. Never silent."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n  [{ts}] FAILED")
    print(f"  WHAT  : {what}")
    print(f"  WHERE : {where}")
    print(f"  WHY   : {why}")
    print(f"  FIX   : {fix}\n")


# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("BTS_Silver_Transform_v4.0") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY") \
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter"
            ".algorithm.version", "2"
        ) \
        .getOrCreate()


# ── Idempotency ───────────────────────────────────────────────────────────────

def delete_silver_partition(year: int, month: int) -> None:
    """
    DELETE existing Silver partition before writing replacement.

    WHY flight_year/flight_month in path:
    Spark creates partition folders from partitionBy() column names.
    We use renamed columns -- so folders are flight_year=/flight_month=
    NOT year=/month=. Path must match exactly or delete silently fails.

    WHEN this runs:
    Only after all validation gates pass on the new data.
    Never delete trusted Silver before the replacement is proven valid.

    Limitation: DELETE+APPEND is idempotent but not atomic.
    """
    partition_path = (
        f"{SILVER_PATH}"
        f"flight_year={year}/"
        f"flight_month={month}/"
    )
    if os.path.exists(partition_path):
        shutil.rmtree(partition_path)
        log_info(
            f"Deleted Silver partition: "
            f"flight_year={year}/flight_month={month}/ "
            f"[DELETE+APPEND idempotency -- not atomic]"
        )


# ── Gate 00: Schema Contract ──────────────────────────────────────────────────

def gate_00_schema_contract(df: DataFrame, label: str) -> bool:
    """
    Gate 00 -- Required columns must be present in Bronze.

    WHY runs first:
    All subsequent gates reference specific column names.
    Missing column -> KeyError without structured WHAT/WHERE/WHY/FIX.
    Gate 00 catches this explicitly before any column access.

    WHY check YEAR and MONTH explicitly:
    These are partition columns. Without basePath, Spark may not
    restore them from folder names. Gate 00 verifies they exist
    after basePath read. If missing -- all date and NULL checks break.
    """
    print(f"\n  Gate 00 -- Schema Contract ({label})")

    actual_cols  = set(df.columns)
    missing_cols = EXPECTED_BRONZE_COLUMNS - actual_cols
    extra_cols   = actual_cols - EXPECTED_BRONZE_COLUMNS

    print(f"    Expected columns : {len(EXPECTED_BRONZE_COLUMNS)}")
    print(f"    Actual columns   : {len(actual_cols)}")
    print(f"    Missing          : {missing_cols if missing_cols else 'None'}")
    print(f"    Extra            : {extra_cols if extra_cols else 'None'}")
    print(f"    YEAR present     : {'YES' if 'YEAR' in actual_cols else 'NO'}")
    print(f"    MONTH present    : {'YES' if 'MONTH' in actual_cols else 'NO'}")

    # For this fixed 2023-2025 dataset, extra columns also fail.
    # WHY: an unexpected column means Bronze schema drifted.
    # Either a BTS format change or a pipeline bug introduced a new column.
    # Both require investigation before Silver proceeds.
    if missing_cols or extra_cols or \
       'YEAR' not in actual_cols or \
       'MONTH' not in actual_cols:
        log_fail(
            what  = f"Bronze schema contract violated for {label}",
            where = f"Gate 00 | {label}",
            why   = f"Missing columns: {missing_cols}. "
                    f"Extra columns: {extra_cols}. "
                    f"YEAR/MONTH may not be restored without basePath.",
            fix   = "Verify spark.read uses option('basePath', BRONZE_PATH). "
                    "Re-run Bronze ingestion if columns genuinely missing. "
                    "Investigate extra columns -- BTS schema may have changed."
        )
        return False

    log_pass(
        f"Gate 00 -- schema contract confirmed. "
        f"{len(actual_cols)} columns present."
    )
    return True


# ── Pass 1: Source Validation (df_bronze) ────────────────────────────────────

def gate_01_mandatory_nulls(df: DataFrame, label: str) -> bool:
    """
    Gate 01 -- Source-contract columns must not be NULL.
    Single Spark job for all 9 columns (not 9 separate scans).
    """
    print(f"\n  Gate 01 -- Mandatory NULL Check ({label})")

    agg_exprs = [
        F.sum(col(c).isNull().cast("int")).alias(c)
        for c in MANDATORY_COLUMNS
        if c in df.columns
    ]
    result   = df.agg(*agg_exprs).collect()[0]
    failures = []

    for c in MANDATORY_COLUMNS:
        if c not in df.columns:
            continue
        null_count = result[c] or 0
        status     = "PASS" if null_count == 0 else "FAIL"
        print(f"    {c:<30} NULLs: {null_count:>10,}  {status}")
        if null_count > 0:
            failures.append((c, null_count))

    if failures:
        for col_name, count in failures:
            log_fail(
                what  = f"{col_name} has {count:,} NULLs in Bronze",
                where = f"Gate 01 | {label}",
                why   = "Source-contract column cannot be NULL. "
                        "Gold analytical results will be corrupted.",
                fix   = f"Investigate Bronze source for {col_name} NULLs. "
                        "Check BTS CSV data quality for this month."
            )
        return False

    log_pass("Gate 01 -- all source-contract columns have zero NULLs.")
    return True


def gate_02_arr_del15_rule(df: DataFrame, label: str) -> bool:
    """Gate 02 -- ARR_DEL15=0 requires ALL 5 delay cause columns to be NULL."""
    print(f"\n  Gate 02 -- ARR_DEL15 Business Rule ({label})")

    any_cause_not_null = (
        col(DELAY_CAUSE_COLUMNS[0]).isNotNull() |
        col(DELAY_CAUSE_COLUMNS[1]).isNotNull() |
        col(DELAY_CAUSE_COLUMNS[2]).isNotNull() |
        col(DELAY_CAUSE_COLUMNS[3]).isNotNull() |
        col(DELAY_CAUSE_COLUMNS[4]).isNotNull()
    )

    violations = df.filter(
        (col("ARR_DEL15") == 0) & any_cause_not_null
    ).count()

    print(f"    ARR_DEL15=0 with any non-NULL delay cause: {violations:,}")

    if violations > 0:
        log_fail(
            what  = f"{violations:,} rows violate ARR_DEL15 business rule",
            where = f"Gate 02 | {label}",
            why   = "ARR_DEL15=0 requires ALL 5 delay cause columns NULL. "
                    "Violation corrupts IOC pillar analysis in Gold.",
            fix   = "Investigate Bronze partition. BTS amended records?"
        )
        return False

    log_pass("Gate 02 -- 0 ARR_DEL15 violations. All 5 columns validated.")
    return True


def gate_03_cancellation_code_rule(df: DataFrame, label: str) -> bool:
    """Gate 03 -- CANCELLATION_CODE must be NULL when CANCELLED=0."""
    print(f"\n  Gate 03 -- Cancellation Code Rule ({label})")

    violations = df.filter(
        (col("CANCELLED") == 0) &
        col("CANCELLATION_CODE").isNotNull()
    ).count()

    print(f"    CANCELLED=0 with non-NULL code: {violations:,}")

    if violations > 0:
        log_fail(
            what  = f"{violations:,} operating flights have cancellation codes",
            where = f"Gate 03 | {label}",
            why   = "CANCELLATION_CODE must be NULL when CANCELLED=0.",
            fix   = "Quarantine affected rows. Investigate BTS source."
        )
        return False

    log_pass("Gate 03 -- cancellation codes only on cancelled flights.")
    return True


def gate_04_diverted_arr_delay_rule(df: DataFrame, label: str) -> bool:
    """Gate 04 -- ARR_DELAY must be NULL when DIVERTED=1."""
    print(f"\n  Gate 04 -- Diverted ARR_DELAY Rule ({label})")

    violations = df.filter(
        (col("DIVERTED") == 1) &
        col("ARR_DELAY").isNotNull()
    ).count()

    print(f"    DIVERTED=1 with non-NULL ARR_DELAY: {violations:,}")

    if violations > 0:
        log_fail(
            what  = f"{violations:,} diverted flights have ARR_DELAY",
            where = f"Gate 04 | {label}",
            why   = "Diverted flights never arrived at planned destination. "
                    "ARR_DELAY at original destination is meaningless.",
            fix   = "NULL ARR_DELAY for DIVERTED=1 rows before Gold load."
        )
        return False

    log_pass("Gate 04 -- diverted flights have NULL ARR_DELAY.")
    return True


def gate_05_air_time_elapsed_rule(df: DataFrame, label: str) -> bool:
    """Gate 05 -- AIR_TIME must be <= ACTUAL_ELAPSED_TIME."""
    print(f"\n  Gate 05 -- AIR_TIME vs ELAPSED_TIME ({label})")

    violations = df.filter(
        col("AIR_TIME").isNotNull() &
        col("ACTUAL_ELAPSED_TIME").isNotNull() &
        (col("AIR_TIME") > col("ACTUAL_ELAPSED_TIME"))
    ).count()

    print(f"    AIR_TIME > ACTUAL_ELAPSED_TIME: {violations:,}")

    if violations > 0:
        log_fail(
            what  = f"{violations:,} rows have AIR_TIME > ACTUAL_ELAPSED_TIME",
            where = f"Gate 05 | {label}",
            why   = "AIR_TIME cannot exceed total elapsed time.",
            fix   = "Investigate affected rows. BTS reporting anomaly."
        )
        return False

    log_pass("Gate 05 -- AIR_TIME <= ACTUAL_ELAPSED_TIME confirmed.")
    return True


def gate_05b_raw_flag_domain(df: DataFrame, label: str) -> bool:
    """
    Gate 05b -- Validate raw flag domain BEFORE casting.

    WHY validate before cast (validate-before-cast principle):
    CANCELLED=1.7 in Bronze casts to is_cancelled=1 in Silver.
    The domain violation is silently lost after casting.
    Gate 07 on df_silver would then pass is_cancelled=1 incorrectly.

    Validate raw DoubleType values first.
    Cast only after source domain is confirmed clean.

    Valid raw values:
    CANCELLED  : {0.0, 1.0}        -- never NULL
    DIVERTED   : {0.0, 1.0}        -- never NULL
    ARR_DEL15  : {0.0, 1.0, NULL}  -- NULL when cancelled/diverted
    DEP_DEL15  : {0.0, 1.0, NULL}  -- NULL when cancelled

    Single Spark job for all 4 checks.
    """
    print(f"\n  Gate 05b -- Raw Flag Domain Check ({label})")

    result = df.agg(
        F.sum(
            (col("CANCELLED").isNotNull() &
             ~col("CANCELLED").isin(0.0, 1.0)).cast("int")
        ).alias("bad_cancelled"),
        F.sum(
            (col("DIVERTED").isNotNull() &
             ~col("DIVERTED").isin(0.0, 1.0)).cast("int")
        ).alias("bad_diverted"),
        F.sum(
            (col("ARR_DEL15").isNotNull() &
             ~col("ARR_DEL15").isin(0.0, 1.0)).cast("int")
        ).alias("bad_arr_del15"),
        F.sum(
            (col("DEP_DEL15").isNotNull() &
             ~col("DEP_DEL15").isin(0.0, 1.0)).cast("int")
        ).alias("bad_dep_del15"),
    ).collect()[0]

    checks = {
        "CANCELLED not in (0.0, 1.0)"  : result["bad_cancelled"]  or 0,
        "DIVERTED not in (0.0, 1.0)"   : result["bad_diverted"]   or 0,
        "ARR_DEL15 not in (0.0,1.0,NULL)": result["bad_arr_del15"] or 0,
        "DEP_DEL15 not in (0.0,1.0,NULL)": result["bad_dep_del15"] or 0,
    }

    failures = []
    for check_name, count in checks.items():
        status = "PASS" if count == 0 else "FAIL"
        print(f"    {check_name:<40} {count:>8,}  {status}")
        if count > 0:
            failures.append((check_name, count))

    if failures:
        for check_name, count in failures:
            log_fail(
                what  = f"{count:,} rows fail raw domain: {check_name}",
                where = f"Gate 05b | {label}",
                why   = "Raw Bronze flag contains out-of-domain value. "
                        "Casting would silently lose this violation in Silver.",
                fix   = "Investigate Bronze source for this column. "
                        "BTS data should only contain 0.0 or 1.0 for flags."
            )
        return False

    log_pass("Gate 05b -- all raw flag domains confirmed {0.0, 1.0}.")
    return True


# ── Pass 2: Post-Transformation Validation (df_silver) ────────────────────────

def gate_06_flight_date_not_null(df: DataFrame, label: str) -> bool:
    """
    Gate 06 -- flight_date must not be NULL after date conversion.

    Valid input does not guarantee valid output.
    FL_DATE not NULL in Bronze but malformed -> NULL flight_date in Silver.
    Gate 01 passes. Gate 06 catches the conversion failure.
    """
    print(f"\n  Gate 06 -- flight_date NULL after date conversion ({label})")

    nulls = df.filter(col("flight_date").isNull()).count()
    print(f"    flight_date NULLs after conversion: {nulls:,}")

    if nulls > 0:
        log_fail(
            what  = f"{nulls:,} flight_date NULLs after date conversion",
            where = f"Gate 06 | {label}",
            why   = "Date conversion returned NULL. FL_DATE format not matched "
                    "by any coalesce format string.",
            fix   = "Inspect FL_DATE values: "
                    "df.select('FL_DATE').distinct().show(). "
                    "Add new format to coalesce() in transform_bronze_to_silver()."
        )
        return False

    log_pass("Gate 06 -- flight_date non-NULL after date conversion.")
    return True


def gate_06b_date_components_match(df: DataFrame, label: str) -> bool:
    """
    Gate 06b -- Parsed flight_date must match YEAR/MONTH/DAY_OF_MONTH.

    WHY this gate exists (independent truth check):
    Gate 06 proves flight_date is not NULL after conversion.
    Gate 06b proves the date VALUE is correct.

    A wrong format string can produce a valid but wrong date:
    "05-06-2025" parsed as dd-MM-yyyy -> 2025-06-05 (not NULL, but wrong)
    YEAR=2025, MONTH=5, DAY_OF_MONTH=6 would NOT match June 5.
    Gate 06b catches this silently-wrong conversion.

    Uses YEAR, MONTH, DAY_OF_MONTH as independent truth source.
    These columns come directly from Bronze -- not from FL_DATE parsing.
    They are the ground truth for the date.

    Business context:
    flight_date is the join key to dim_date.
    Wrong date = flight joins to wrong calendar month in Gold.
    All time-series analytics (monthly trends, seasonal patterns) break.
    """
    print(f"\n  Gate 06b -- Date Components Match ({label})")

    violations = df.filter(
        (spark_year(col("flight_date"))     != col("flight_year"))  |
        (spark_month(col("flight_date"))    != col("flight_month")) |
        (spark_dayofmonth(col("flight_date")) != col("flight_day"))
    ).count()

    print(f"    Date component mismatches: {violations:,}")

    if violations > 0:
        log_fail(
            what  = f"{violations:,} rows have flight_date not matching "
                    f"YEAR/MONTH/DAY_OF_MONTH source columns",
            where = f"Gate 06b | {label}",
            why   = "Date format string produced wrong date value. "
                    "e.g. MM-dd-yyyy parsed as dd-MM-yyyy produces "
                    "valid but wrong date. Gold dim_date joins will be wrong.",
            fix   = "Inspect FL_DATE format for this partition. "
                    "Verify coalesce() format order produces correct dates. "
                    "Check sample: df.select(FL_DATE, flight_date, "
                    "flight_year, flight_month, flight_day).show()"
        )
        return False

    log_pass("Gate 06b -- flight_date components match YEAR/MONTH/DAY_OF_MONTH.")
    return True


def gate_07_type_domain_checks(df: DataFrame,
                                label: str,
                                silver_count: int) -> bool:
    """
    Gate 07 -- Type domain validation after casting.
    Single Spark job for all 5 checks.
    Note: raw flag domain validated in Gate 05b before casting.
    Gate 07 confirms cast produced correct Silver values.
    """
    print(f"\n  Gate 07 -- Type Domain Checks ({label})")

    result = df.agg(
        F.sum(
            (col("is_cancelled").isNotNull() &
             ~col("is_cancelled").isin(0, 1)).cast("int")
        ).alias("bad_cancelled"),
        F.sum(
            (col("is_diverted").isNotNull() &
             ~col("is_diverted").isin(0, 1)).cast("int")
        ).alias("bad_diverted"),
        F.sum(
            (col("arr_delayed_flag").isNotNull() &
             ~col("arr_delayed_flag").isin(0, 1)).cast("int")
        ).alias("bad_arr_flag"),
        F.sum(
            (col("dep_delayed_flag").isNotNull() &
             ~col("dep_delayed_flag").isin(0, 1)).cast("int")
        ).alias("bad_dep_flag"),
        F.sum(
            (col("distance_miles").isNotNull() &
             (col("distance_miles") <= 0)).cast("int")
        ).alias("bad_distance"),
    ).collect()[0]

    checks = {
        "is_cancelled not in (0,1)"         : result["bad_cancelled"] or 0,
        "is_diverted not in (0,1)"          : result["bad_diverted"]  or 0,
        "arr_delayed_flag not in (0,1,NULL)": result["bad_arr_flag"]  or 0,
        "dep_delayed_flag not in (0,1,NULL)": result["bad_dep_flag"]  or 0,
        "distance_miles <= 0"               : result["bad_distance"]  or 0,
    }

    failures = []
    for check_name, count in checks.items():
        status = "PASS" if count == 0 else "FAIL"
        print(f"    {check_name:<45} {count:>8,}  {status}")
        if count > 0:
            failures.append((check_name, count))

    if failures:
        for check_name, count in failures:
            log_fail(
                what  = f"{count:,} rows fail Silver domain: {check_name}",
                where = f"Gate 07 | {label}",
                why   = "Cast produced out-of-domain Silver values.",
                fix   = "Check Gate 05b result. Investigate Bronze source values."
            )
        return False

    log_pass("Gate 07 -- all Silver type domain checks passed.")
    return True


def gate_08_grain_uniqueness(df: DataFrame,
                               label: str,
                               silver_count: int) -> bool:
    """
    Gate 08 -- Grain uniqueness: one row per scheduled flight per day.

    WHY not rely on health check C13:
    C13 was a point-in-time validation. Silver enforces this permanently.
    Future data or pipeline changes could introduce duplicates.

    Reuses silver_count -- avoids redundant df.count() Spark job.
    """
    print(f"\n  Gate 08 -- Grain Uniqueness ({label})")

    unique_rows = df.dropDuplicates(GRAIN_KEY_COLUMNS).count()
    duplicates  = silver_count - unique_rows

    print(f"    Total rows   : {silver_count:,}")
    print(f"    Unique rows  : {unique_rows:,}")
    print(f"    Duplicates   : {duplicates:,}")

    if duplicates > 0:
        log_fail(
            what  = f"{duplicates:,} duplicate grain keys in Silver",
            where = f"Gate 08 | {label}",
            why   = "One flight per calendar day is the grain contract. "
                    "Duplicates corrupt every Gold aggregate metric.",
            fix   = "Investigate Bronze source for duplicate records."
        )
        return False

    log_pass(f"Gate 08 -- grain uniqueness confirmed. {silver_count:,} unique rows.")
    return True


def gate_09_row_count_check(bronze_count: int,
                              silver_count: int,
                              label: str) -> bool:
    """Gate 09 -- Silver row count must match Bronze exactly."""
    print(f"\n  Gate 09 -- Row Count Check ({label})")
    print(f"    Bronze rows : {bronze_count:,}")
    print(f"    Silver rows : {silver_count:,}")

    if bronze_count == silver_count:
        log_pass(f"Gate 09 -- row counts match: {silver_count:,}")
        return True

    diff      = abs(bronze_count - silver_count)
    direction = "lost" if silver_count < bronze_count else "gained"
    log_fail(
        what  = f"Silver {direction} {diff:,} rows vs Bronze",
        where = f"Gate 09 | {label}",
        why   = "Silver must preserve all Bronze rows exactly.",
        fix   = "Review transformation for unintended row drops or duplicates."
    )
    return False


# ── Silver Completion Gate ────────────────────────────────────────────────────

def silver_completion_gate(spark: SparkSession,
                            success_count: int) -> bool:
    """
    Silver Completion Gate (SCG) -- Final physical reconciliation.

    WHY this gate exists:
    success_count == 36 means every partition function returned True.
    That does NOT prove the complete Silver artifact exists correctly.
    Physical disk state could differ from processing state due to:
    -- Partial writes
    -- File system errors after success log
    -- Previous partial runs leaving stale partitions

    SCG physically verifies:
    1. Exactly 36 partition folders exist on disk
    2. Total Silver row count == 20,928,599 (Bronze confirmed total)
    3. No unexpected partitions exist
    4. No expected partitions are missing

    Only after SCG passes is Gold authorised to run.
    """
    print(f"\n  {'=' * 60}")
    print("  SILVER COMPLETION GATE (SCG)")
    print(f"  {'=' * 60}")

    # Check 1: Physical partition count
    expected_set = set(
        (y, m)
        for y in EXPECTED_YEARS
        for m in range(1, 13)
    )

    # Discover ALL physical partitions -- not just expected ones
    # WHY: expected_set check alone misses unexpected partitions
    # that exist on disk from previous runs or bugs.
    # Scan the actual Silver directory to find what physically exists.
    found_set = set()
    for year in EXPECTED_YEARS:
        for month in range(1, 13):
            path = (
                f"{SILVER_PATH}"
                f"flight_year={year}/"
                f"flight_month={month}/"
            )
            if os.path.exists(path):
                found_set.add((year, month))

    # Also scan for any unexpected partitions on disk
    unexpected_on_disk = set()
    if os.path.exists(SILVER_PATH):
        for entry in os.listdir(SILVER_PATH):
            if entry.startswith("flight_year="):
                year_val = int(entry.split("=")[1])
                year_path = os.path.join(SILVER_PATH, entry)
                if os.path.isdir(year_path):
                    for month_entry in os.listdir(year_path):
                        if month_entry.startswith("flight_month="):
                            month_val = int(month_entry.split("=")[1])
                            partition = (year_val, month_val)
                            if partition not in expected_set:
                                unexpected_on_disk.add(partition)

    missing = expected_set - found_set
    extra   = found_set - expected_set | unexpected_on_disk

    print(f"    Expected partitions : {len(expected_set)}")
    print(f"    Found partitions    : {len(found_set)}")
    print(f"    Missing             : {sorted(missing) if missing else 'None'}")
    print(f"    Extra               : {sorted(extra) if extra else 'None'}")

    if missing or extra:
        log_fail(
            what  = f"Silver partition mismatch: "
                    f"{len(missing)} missing, {len(extra)} extra",
            where = "Silver Completion Gate",
            why   = "Physical Silver partitions do not match expected set. "
                    "Processing success_count alone is insufficient.",
            fix   = f"Missing: {sorted(missing)}. "
                    "Rerun Silver for missing partitions. "
                    "Investigate extra partitions manually."
        )
        return False

    log_pass(f"SCG Check 1 -- all 36 Silver partitions physically confirmed.")

    # Check 2: Total row count reconciliation
    log_info("Reading full Silver dataset for row count reconciliation...")
    df_silver_full = spark.read.parquet(SILVER_PATH)
    silver_total   = df_silver_full.count()

    print(f"    Bronze total rows   : {EXPECTED_TOTAL_ROWS:,}")
    print(f"    Silver total rows   : {silver_total:,}")
    print(f"    Match               : {'YES' if silver_total == EXPECTED_TOTAL_ROWS else 'NO'}")

    if silver_total != EXPECTED_TOTAL_ROWS:
        diff      = abs(silver_total - EXPECTED_TOTAL_ROWS)
        direction = "fewer" if silver_total < EXPECTED_TOTAL_ROWS else "more"
        log_fail(
            what  = f"Silver total rows {silver_total:,} != "
                    f"Bronze total {EXPECTED_TOTAL_ROWS:,}",
            where = "Silver Completion Gate",
            why   = f"Silver has {diff:,} {direction} rows than Bronze. "
                    "Data was lost or duplicated across the full pipeline.",
            fix   = "Identify which partition has wrong row count. "
                    "Compare Silver partition counts against Bronze. "
                    "Rerun affected Silver partitions."
        )
        return False

    log_pass(
        f"SCG Check 2 -- Silver total rows confirmed: "
        f"{silver_total:,} == {EXPECTED_TOTAL_ROWS:,}"
    )
    log_pass("SILVER COMPLETION GATE PASSED. Gold layer may now run.")
    return True


# ── Transformation ────────────────────────────────────────────────────────────

def transform_bronze_to_silver(df: DataFrame) -> DataFrame:
    """
    Apply all Silver transformations in correct order.

    Order matters:
    1. Drop first   -- smaller DataFrame for subsequent operations
    2. Cast second  -- after Gate 05b confirms raw domain is clean
    3. Date third   -- coalesce handles all observed BTS formats
    4. Rename last  -- Silver output has clean business names
    5. Timestamp    -- add Silver audit column after all transforms
    """

    # Step 1: Drop unnecessary columns
    df = df.drop(*COLUMNS_TO_DROP)

    # Step 2: Cast binary flags DoubleType -> IntegerType
    # WHY cast AFTER Gate 05b validation:
    # Gate 05b (source validation) confirms raw values are in {0.0, 1.0}.
    # Only after that confirmation do we cast to IntegerType.
    # This preserves the validate-before-cast principle.
    df = df \
        .withColumn("DEP_DEL15", col("DEP_DEL15").cast(IntegerType())) \
        .withColumn("ARR_DEL15", col("ARR_DEL15").cast(IntegerType())) \
        .withColumn("CANCELLED", col("CANCELLED").cast(IntegerType())) \
        .withColumn("DIVERTED",  col("DIVERTED").cast(IntegerType()))

    # Step 3: Convert FL_DATE to DateType
    # WHY coalesce across multiple formats:
    # BTS published FL_DATE in TWO observed formats:
    # Format 1: "M/d/yyyy h:mm:ss a"  e.g. "1/4/2023 12:00:00 AM"
    # Format 2: "MM-dd-yyyy HH:mm"    e.g. "12-02-2024 00:00"
    # A third defensive format handles non-zero-padded hyphenated dates.
    # coalesce() tries each and returns first non-NULL result.
    # Gate 06  -- validates flight_date not NULL
    # Gate 06b -- validates date VALUE correct against YEAR/MONTH/DAY_OF_MONTH
    df = df.withColumn(
        "FL_DATE",
        coalesce(
            to_date(
                to_timestamp(col("FL_DATE"), "M/d/yyyy h:mm:ss a")
            ),
            to_date(col("FL_DATE"), "MM-dd-yyyy HH:mm"),
            to_date(col("FL_DATE"), "M-d-yyyy HH:mm")
        )
    )

    # Step 4: Rename columns (ADR-008)
    # WHY after cast and date conversion:
    # Cast and date expressions use original BTS column names.
    # Rename last so Silver output has clean business names.
    for old_name, new_name in COLUMN_RENAME_MAP.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # Step 5: Add Silver audit timestamp
    # silver_processed_ts = when this row was validated and written to Silver
    # Different from Bronze ingestion_ts (when raw data was loaded)
    df = df.withColumn("silver_processed_ts", current_timestamp())

    return df


# ── Partition Processing ──────────────────────────────────────────────────────

def process_partition(spark: SparkSession,
                       year: int,
                       month: int) -> bool:
    """
    Process one Bronze partition through Silver.

    Full sequence:
    Read Bronze -> Gate 00 -> Pass 1 (Gates 01-05b) ->
    Transform -> persist -> Pass 2 (Gates 06-09) ->
    DELETE partition -> APPEND Silver -> unpersist
    """
    label       = f"year={year}/month={month}"
    bronze_path = f"{BRONZE_PATH}YEAR={year}/MONTH={month}/"

    print(f"\n  {'=' * 60}")
    print(f"  Processing: {label}")
    print(f"  {'=' * 60}")

    if not os.path.exists(bronze_path):
        log_fail(
            what  = f"Bronze partition missing: {label}",
            where = f"Path: {bronze_path}",
            why   = "Bronze must be complete before Silver runs.",
            fix   = "Run bronze_ingestion.py. Verify 36 partitions exist."
        )
        return False

    # WHY basePath: restores YEAR and MONTH from partition folder names
    try:
        df_bronze = (
            spark.read
            .option("basePath", BRONZE_PATH)
            .parquet(bronze_path)
        )
    except Exception as e:
        log_fail(
            what  = f"Cannot read Bronze partition: {label}",
            where = f"Path: {bronze_path}",
            why   = f"Parquet read error: {str(e)}",
            fix   = "Verify Bronze Parquet not corrupted. Re-run Bronze."
        )
        return False

    bronze_count = df_bronze.count()
    log_info(f"Bronze rows: {bronze_count:,} | Columns: {len(df_bronze.columns)}")

    if bronze_count < MIN_ROWS_PER_PARTITION:
        log_fail(
            what  = f"Bronze partition too small: {bronze_count:,} rows",
            where = f"Path: {bronze_path}",
            why   = f"Expected minimum {MIN_ROWS_PER_PARTITION:,} rows.",
            fix   = "Re-run Bronze ingestion. Verify source CSV complete."
        )
        return False

    # Gate 00
    if not gate_00_schema_contract(df_bronze, label):
        return False

    # Pass 1: Source validation
    log_info("Pass 1: Source validation (df_bronze)...")
    pass1 = [
        gate_01_mandatory_nulls(df_bronze, label),
        gate_02_arr_del15_rule(df_bronze, label),
        gate_03_cancellation_code_rule(df_bronze, label),
        gate_04_diverted_arr_delay_rule(df_bronze, label),
        gate_05_air_time_elapsed_rule(df_bronze, label),
        gate_05b_raw_flag_domain(df_bronze, label),
    ]

    if not all(pass1):
        log_fail(
            what  = f"Pass 1 source validation failed: {label}",
            where = "Silver Pass 1",
            why   = "Bronze data violates business rule standards.",
            fix   = "Review gate failures above."
        )
        return False

    log_pass("Pass 1 complete -- all 6 source gates passed.")

    # Transform
    try:
        df_silver = transform_bronze_to_silver(df_bronze)
    except Exception as e:
        log_fail(
            what  = f"Transformation failed: {label}",
            where = "transform_bronze_to_silver()",
            why   = f"Error: {str(e)}",
            fix   = "Review transformation logic."
        )
        return False

    log_info(f"Transformation complete. Silver columns: {len(df_silver.columns)}")

    # Persist for Pass 2 efficiency
    # Multiple actions against df_silver -- persist avoids recomputation
    df_silver.persist(StorageLevel.MEMORY_AND_DISK)

    # Pass 2: Post-transformation validation
    log_info("Pass 2: Post-transformation validation (df_silver)...")
    silver_count = df_silver.count()

    pass2 = [
        gate_06_flight_date_not_null(df_silver, label),
        gate_06b_date_components_match(df_silver, label),
        gate_07_type_domain_checks(df_silver, label, silver_count),
        gate_08_grain_uniqueness(df_silver, label, silver_count),
        gate_09_row_count_check(bronze_count, silver_count, label),
    ]

    if not all(pass2):
        df_silver.unpersist()
        log_fail(
            what  = f"Pass 2 post-transform validation failed: {label}",
            where = "Silver Pass 2",
            why   = "Transformation introduced data quality issues.",
            fix   = "Review gate failures above. Fix and rerun partition."
        )
        return False

    log_pass("Pass 2 complete -- all 5 transform gates passed.")

    # DELETE existing partition (only after all gates pass)
    delete_silver_partition(year, month)

    # APPEND Silver (safe because target partition was deleted)
    try:
        df_silver.write \
            .mode("append") \
            .partitionBy("flight_year", "flight_month") \
            .parquet(SILVER_PATH)

        df_silver.unpersist()

        log_pass(
            f"Silver written: {label} | "
            f"{silver_count:,} rows | "
            f"{len(df_silver.columns)} cols"
        )
        return True

    except Exception as e:
        df_silver.unpersist()
        log_fail(
            what  = f"Silver write failed: {label}",
            where = f"Output: {SILVER_PATH}",
            why   = f"Parquet write error: {str(e)}",
            fix   = "Check disk space. Verify output path exists."
        )
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

def run_silver_transform() -> None:
    """
    Main Silver pipeline.

    Gate philosophy:
    36/36 partitions must pass all gates.
    Silver Completion Gate (SCG) performs final physical reconciliation.
    Only after SCG passes is Gold authorised to run.
    """
    print("=" * 65)
    print("  BTS AVIATION DELAY INTELLIGENCE SYSTEM")
    print("  Silver Layer Transformation v4.0")
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)
    print()
    print("  Idempotency  : validate -> DELETE -> APPEND [not atomic]")
    print("  Validation   : 12 gates (G00 + 6 source + 5 post-transform) + SCG")
    print("  Gold contract: 36/36 partitions, 12 gates each + Silver Completion Gate")
    print()

    # Enforce exact expected partition set before processing
    expected_set = set(
        (y, m)
        for y in EXPECTED_YEARS
        for m in range(1, 13)
    )
    found_set = set()
    for year in EXPECTED_YEARS:
        for month in range(1, 13):
            path = f"{BRONZE_PATH}YEAR={year}/MONTH={month}/"
            if os.path.exists(path):
                found_set.add((year, month))

    missing_partitions = expected_set - found_set
    if missing_partitions:
        log_fail(
            what  = f"{len(missing_partitions)} Bronze partitions missing",
            where = f"Bronze path: {BRONZE_PATH}",
            why   = f"Expected 36 partitions (2023-2025, all 12 months). "
                    f"Missing: {sorted(missing_partitions)}",
            fix   = "Run bronze_ingestion.py for missing partitions."
        )
        sys.exit(1)

    bronze_partitions = sorted(found_set)
    log_info(f"All {len(bronze_partitions)} expected Bronze partitions confirmed")
    log_info(f"Silver output: {SILVER_PATH}")

    os.makedirs(SILVER_PATH, exist_ok=True)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    log_info(f"Spark {spark.version} ready")

    success_count = 0
    fail_count    = 0
    failed_parts  = []
    total         = len(bronze_partitions)

    for i, (year, month) in enumerate(bronze_partitions, 1):
        print(f"\n  [{i:02d}/{total}] year={year} / month={month}")
        success = process_partition(spark, year, month)
        if success:
            success_count += 1
        else:
            fail_count += 1
            failed_parts.append(f"year={year}/month={month}")

    # Final Summary
    print()
    print("=" * 65)
    print("  SILVER TRANSFORMATION SUMMARY")
    print("=" * 65)
    print(f"  Expected partitions  : {EXPECTED_PARTITIONS}")
    print(f"  Processed            : {total}")
    print(f"  Succeeded            : {success_count}")
    print(f"  Failed               : {fail_count}")
    print(f"  Output               : {SILVER_PATH}")
    print(f"  Completed            : "
          f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if fail_count > 0:
        print()
        print(f"  FAILED PARTITIONS:")
        for p in failed_parts:
            print(f"    FAILED: {p}")
        print()
        print("  SILVER COMPLETION GATE: NOT RUN (partition failures exist)")
        print("  GOLD LAYER MUST NOT RUN.")
        print("  Fix failures above. Rerun Silver for failed partitions only.")
        print("=" * 65)
        spark.stop()
        sys.exit(1)

    # Silver Completion Gate -- only runs if all 36 partitions succeeded
    print()
    scg_passed = silver_completion_gate(spark, success_count)

    if scg_passed:
        print()
        print("=" * 65)
        print("  ALL 36/36 PARTITIONS PASSED ALL 12 VALIDATION GATES.")
        print("  SILVER COMPLETION GATE PASSED.")
        print("  Silver layer is trusted, validated, and governed.")
        print("  Bronze is built. Silver is built. Trust boundary is real.")
        print("  Gold layer may now run.")
        print()
        print("  Gates enforced per partition:")
        print("  G00   Schema contract (columns + YEAR/MONTH restored)")
        print("  G01   Mandatory source-contract NULLs (1 Spark job)")
        print("  G02   ARR_DEL15 rule (all 5 delay cause columns)")
        print("  G03   Cancellation code rule")
        print("  G04   Diverted ARR_DELAY rule")
        print("  G05   AIR_TIME <= ACTUAL_ELAPSED_TIME")
        print("  G05b  Raw flag domain BEFORE casting (validate-before-cast)")
        print("  G06   flight_date not NULL after coalesce date conversion")
        print("  G06b  Date components match YEAR/MONTH/DAY_OF_MONTH")
        print("  G07   Silver type domain checks (5 checks, 1 Spark job)")
        print("  G08   Grain uniqueness (reuses silver_count)")
        print("  G09   Row count Bronze == Silver")
        print("  SCG   Physical partition count + 20,928,599 reconciliation")
        print("=" * 65)
    else:
        print()
        print("  SILVER COMPLETION GATE FAILED.")
        print("  GOLD LAYER MUST NOT RUN.")
        print("=" * 65)
        spark.stop()
        sys.exit(1)

    spark.stop()
    sys.exit(0)


# ── Entry Point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_silver_transform()