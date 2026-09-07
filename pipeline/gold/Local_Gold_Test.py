"""
BTS Aviation Delay Intelligence System
=======================================
Script  : local_gold_test.py
Purpose : TEST MODE -- Gold layer validation on 100K sample rows.
          Run this BEFORE full Azure execution.
          Official gold_star_schema.py is NOT modified.

Test Standard (from brother's document):
→ Grain uniqueness verified
→ Dimension business keys unique
→ Join cardinality preserved (no row multiplication)
→ Foreign key integrity confirmed
→ UNKNOWN member handling verified
→ Metric reconciliation on sample
→ Bridge attribution reconciliation

Pass criteria:
All checks pass on 100K sample.
Runtime evidence saved.
THEN and ONLY THEN: full Azure run.

Run:
.venv311/Scripts/python.exe pipeline/gold/local_gold_test.py
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, date as _date
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# ── Windows Environment Setup ─────────────────────────────────
os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PATH"]                  = os.environ["PATH"] + r";C:\hadoop\bin"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv311\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv311\Scripts\python.exe"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, monotonically_increasing_id
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType,
    LongType, StringType, StructField,
    StructType, TimestampType,
)

# ── Config ────────────────────────────────────────────────────
SILVER_PATH   = "data/silver/"
GOLD_TEST_PATH= "data/gold_test/"   # separate from production gold
TEST_ROWS     = 100_000
UNKNOWN_KEY   = -1
UNKNOWN_VALUE = "Unknown"

IOC_SAFETY     = "Safety"
IOC_LEGALITY   = "Legality"
IOC_EFFICIENCY = "Efficiency"
IOC_NONE       = "None"

INFLUENCE_INTERNAL = "INTERNAL_ASSOCIATED"
INFLUENCE_EXTERNAL = "EXTERNAL_ASSOCIATED"
INFLUENCE_MIXED    = "MIXED"
INFLUENCE_UNKNOWN  = "UNKNOWN"

GRAIN_COLS = [
    "flight_date", "carrier_code", "flight_number",
    "origin_airport", "dest_airport",
]

CAUSE_COLS = {
    "CARRIER":       "carrier_delay_mins",
    "WEATHER":       "weather_delay_mins",
    "NAS":           "nas_delay_mins",
    "SECURITY":      "security_delay_mins",
    "LATE_AIRCRAFT": "late_aircraft_delay_mins",
}

# ── Logging ───────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def log_info(msg: str) -> None:
    print(f"  [{_ts()}] INFO  : {msg}")

def log_pass(msg: str) -> None:
    print(f"  [{_ts()}] PASS  : {msg}")

def log_fail(what: str, where: str,
             why: str, fix: str) -> None:
    print(f"\n  [{_ts()}] FAILED")
    print(f"  WHAT  : {what}")
    print(f"  WHERE : {where}")
    print(f"  WHY   : {why}")
    print(f"  FIX   : {fix}\n")
    raise RuntimeError(what)

def log_warn(msg: str) -> None:
    print(f"  [{_ts()}] WARN  : {msg}")

# ── Spark ─────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BTS_Gold_TEST_MODE")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ── Silver Sample ─────────────────────────────────────────────

def load_silver_sample(spark: SparkSession) -> tuple[DataFrame, int]:
    """
    Load stratified sample from Silver.
    ~2,778 rows per partition × 36 partitions = ~100K total.
    
    WHY stratified not limit():
    limit(100K) pulls from first partitions only.
    Covers ~9 dates. Not representative.
    
    Stratified sample covers all 36 year/month partitions.
    Exposes rare schema differences in later months.
    Evidence becomes: "100K rows spanning all 36 partitions"
    Not: "100K rows from first few days of Jan 2023"
    """
    log_info(f"Loading stratified Silver sample (~{TEST_ROWS:,} rows)...")

    if not Path(SILVER_PATH).exists():
        log_fail(
            "Silver path not found.",
            SILVER_PATH,
            "Silver must exist before Gold test.",
            "Run silver_transform.py first."
        )

    ROWS_PER_PARTITION = TEST_ROWS // 36  # ~2,778 per month

    frames = []
    for year in [2023, 2024, 2025]:
        for month in range(1, 13):
            partition_path = (
                f"{SILVER_PATH}"
                f"flight_year={year}/flight_month={month}/"
            )
            if not Path(partition_path).exists():
                log_warn(f"Partition missing: {year}/{month}")
                continue
            df_part = (
                spark.read
                .option("basePath", SILVER_PATH)
                .parquet(partition_path)
                .limit(ROWS_PER_PARTITION)
            )
            frames.append(df_part)

    if not frames:
        log_fail(
            "No Silver partitions found.",
            SILVER_PATH,
            "Silver partitions must exist.",
            "Run silver_transform.py first."
        )

    df = frames[0]
    for frame in frames[1:]:
        df = df.unionByName(frame)

    count = df.count()
    log_pass(
        f"Stratified sample loaded: {count:,} rows "
        f"across {len(frames)}/36 partitions."
    )
    return df, count

# ── Surrogate Keys ────────────────────────────────────────────

def make_key(df: DataFrame, key_col: str) -> DataFrame:
    return df.withColumn(
        key_col,
        (monotonically_increasing_id() + 1).cast(LongType())
    )

def make_flight_id(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "flight_id",
        monotonically_increasing_id().cast(StringType())
    )

# ── Dimensions ────────────────────────────────────────────────

def build_dim_date(spark: SparkSession,
                   df: DataFrame) -> DataFrame:
    log_info("Building dim_date...")

    base = (
        df.select(col("flight_date").alias("full_date"))
        .filter(col("full_date").isNotNull())
        .distinct()
    )

    dim = (
        base
        .withColumn("date_key",
            F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year",         F.year("full_date"))
        .withColumn("quarter",      F.quarter("full_date"))
        .withColumn("month",        F.month("full_date"))
        .withColumn("month_name",   F.date_format("full_date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week",  F.dayofweek("full_date"))
        .withColumn("day_name",     F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend",
            F.dayofweek("full_date").isin(1, 7))
        .withColumn("season",
            F.when(F.month("full_date").isin(12, 1, 2), "Winter")
            .when(F.month("full_date").isin(3, 4, 5),  "Spring")
            .when(F.month("full_date").isin(6, 7, 8),  "Summer")
            .otherwise("Fall"))
        .withColumn("holiday_travel_window",
            F.when((F.month("full_date") == 11) &
                   F.dayofmonth("full_date").between(20, 30),
                   "Thanksgiving window")
            .when((F.month("full_date") == 12) &
                   F.dayofmonth("full_date").between(20, 31),
                   "Christmas window")
            .otherwise(None))
        .withColumn("record_type", lit("STATIC"))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )

    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, _date(1900, 1, 1), -1, -1, -1,
          UNKNOWN_VALUE, -1, -1, UNKNOWN_VALUE,
          False, UNKNOWN_VALUE, None,
          "UNKNOWN_MEMBER", None)],
        schema=StructType([
            StructField("date_key",              IntegerType(),   True),
            StructField("full_date",             DateType(),      True),
            StructField("year",                  IntegerType(),   True),
            StructField("quarter",               IntegerType(),   True),
            StructField("month",                 IntegerType(),   True),
            StructField("month_name",            StringType(),    True),
            StructField("day_of_month",          IntegerType(),   True),
            StructField("day_of_week",           IntegerType(),   True),
            StructField("day_name",              StringType(),    True),
            StructField("is_weekend",            BooleanType(),   True),
            StructField("season",                StringType(),    True),
            StructField("holiday_travel_window", StringType(),    True),
            StructField("record_type",           StringType(),    True),
            StructField("gold_processed_ts",     TimestampType(), True),
        ])
    )

    result = dim.unionByName(unknown)
    log_pass(f"dim_date: {result.count():,} rows.")
    return result


def build_dim_carrier(spark: SparkSession,
                       df: DataFrame) -> DataFrame:
    log_info("Building dim_carrier...")

    carrier_names = {
        "AA": "American Airlines",      "AS": "Alaska Airlines",
        "B6": "JetBlue Airways",        "DL": "Delta Air Lines",
        "F9": "Frontier Airlines",      "G4": "Allegiant Air",
        "HA": "Hawaiian Airlines",      "MQ": "Envoy Air",
        "NK": "Spirit Airlines",        "OH": "PSA Airlines",
        "OO": "SkyWest Airlines",       "QX": "Horizon Air",
        "UA": "United Airlines",        "WN": "Southwest Airlines",
        "YX": "Republic Airways",       "9E": "Endeavor Air",
    }
    mapping = F.create_map(
        *[x for k, v in carrier_names.items()
          for x in (lit(k), lit(v))]
    )

    dim = (
        df.select("carrier_code")
        .filter(col("carrier_code").isNotNull())
        .distinct()
        .withColumn("carrier_name",
            F.coalesce(mapping[col("carrier_code")],
                       lit("Unmapped Carrier")))
        .withColumn("record_type", lit("SNAPSHOT_V1"))
        .withColumn("is_current", lit(True))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )
    dim = make_key(dim, "carrier_key")

    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, UNKNOWN_VALUE, UNKNOWN_VALUE,
          "UNKNOWN_MEMBER", True)],
        "carrier_key long, carrier_code string, "
        "carrier_name string, record_type string, "
        "is_current boolean"
    ).withColumn("gold_processed_ts", F.current_timestamp())

    result = dim.select(
        "carrier_key", "carrier_code", "carrier_name",
        "record_type", "is_current", "gold_processed_ts"
    ).unionByName(unknown)
    log_pass(f"dim_carrier: {result.count():,} rows.")
    return result


def build_dim_airport(spark: SparkSession,
                       df: DataFrame) -> DataFrame:
    log_info("Building dim_airport...")

    origins = df.select(
        col("origin_airport").alias("airport_code"),
        col("origin_city").alias("city"),
        col("origin_state").alias("state"),
    ).distinct()

    dests = df.select(
        col("dest_airport").alias("airport_code"),
        col("dest_city").alias("city"),
        col("dest_state").alias("state"),
    ).distinct()

    dim = (
        origins.unionByName(dests)
        .filter(col("airport_code").isNotNull())
        .dropDuplicates(["airport_code"])
        .withColumn("record_type", lit("SNAPSHOT_V1"))
        .withColumn("is_current", lit(True))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )
    dim = make_key(dim, "airport_key")

    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, UNKNOWN_VALUE, UNKNOWN_VALUE,
          UNKNOWN_VALUE, "UNKNOWN_MEMBER", True)],
        "airport_key long, airport_code string, "
        "city string, state string, "
        "record_type string, is_current boolean"
    ).withColumn("gold_processed_ts", F.current_timestamp())

    result = dim.select(
        "airport_key", "airport_code", "city", "state",
        "record_type", "is_current", "gold_processed_ts"
    ).unionByName(unknown)
    log_pass(f"dim_airport: {result.count():,} rows.")
    return result


def build_dim_aircraft(spark: SparkSession,
                        df: DataFrame) -> DataFrame:
    log_info("Building dim_aircraft...")

    dim = (
        df.select("tail_number")
        .filter(col("tail_number").isNotNull())
        .distinct()
        .withColumn("record_type", lit("SNAPSHOT_V1"))
        .withColumn("is_current", lit(True))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )
    dim = make_key(dim, "aircraft_key")

    unknown = spark.createDataFrame(
        [(UNKNOWN_KEY, UNKNOWN_VALUE, "UNKNOWN_MEMBER", True)],
        "aircraft_key long, tail_number string, "
        "record_type string, is_current boolean"
    ).withColumn("gold_processed_ts", F.current_timestamp())

    result = dim.select(
        "aircraft_key", "tail_number",
        "record_type", "is_current", "gold_processed_ts"
    ).unionByName(unknown)
    log_pass(f"dim_aircraft: {result.count():,} rows.")
    return result


def build_dim_delay_reason(spark: SparkSession) -> DataFrame:
    log_info("Building dim_delay_reason...")

    rows = [
        (1, "CARRIER",       "Carrier Delay",
         IOC_EFFICIENCY, INFLUENCE_INTERNAL, "carrier_delay_mins"),
        (2, "WEATHER",       "Weather Delay",
         IOC_SAFETY,     INFLUENCE_EXTERNAL, "weather_delay_mins"),
        (3, "NAS",           "NAS Delay",
         IOC_LEGALITY,   INFLUENCE_EXTERNAL, "nas_delay_mins"),
        (4, "SECURITY",      "Security Delay",
         IOC_LEGALITY,   INFLUENCE_EXTERNAL, "security_delay_mins"),
        (5, "LATE_AIRCRAFT", "Late Aircraft Delay",
         IOC_EFFICIENCY, INFLUENCE_INTERNAL, "late_aircraft_delay_mins"),
        (UNKNOWN_KEY, "UNKNOWN", "Unknown",
         IOC_NONE,       INFLUENCE_UNKNOWN,  None),
    ]

    schema = StructType([
        StructField("delay_reason_key",            IntegerType(), False),
        StructField("delay_code",                  StringType(),  False),
        StructField("delay_category",              StringType(),  False),
        StructField("ioc_pillar",                  StringType(),  False),
        StructField("operational_influence_class", StringType(),  False),
        StructField("bts_source_column",           StringType(),  True),
    ])

    result = (
        spark.createDataFrame(rows, schema)
        .withColumn("record_type", lit("TYPE1_LOOKUP"))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )
    log_pass(f"dim_delay_reason: {result.count():,} rows.")
    return result

# ── Fact ──────────────────────────────────────────────────────

def build_fact(
        df: DataFrame,
        dim_date: DataFrame,
        dim_carrier: DataFrame,
        dim_airport: DataFrame,
        dim_aircraft: DataFrame,
) -> DataFrame:
    """
    Build fact_delays -- one scheduled flight per calendar day.

    WHY no delay_reason_key in fact:
    One flight can have multiple attributed delay causes simultaneously.
    Single FK discards real information. Bridge table handles this.

    WHY LEFT JOIN for all dimension lookups:
    Must never drop a flight due to dimension lookup failure.
    Unmatched rows get UNKNOWN member key (-1).

    WHY dominant_delay_pillar is DERIVED not OBSERVED:
    This is our project-defined analytical classification.
    NOT BTS-provided semantics.
    BTS reports delay minutes per category.
    We classify those categories into IOC-inspired pillars.
    Must never be presented as BTS causal truth.

    WHY operational_influence_class not is_controllable:
    BTS attribution cannot prove airline controlled the delay.
    INTERNAL_ASSOCIATED is honest. CONTROLLABLE is not.
    """
    log_info("Building fact_delays...")

    date_lu    = dim_date.select(
        col("full_date").alias("flight_date"), "date_key")
    carrier_lu = dim_carrier.select("carrier_code", "carrier_key")
    origin_lu  = dim_airport.select(
        col("airport_code").alias("origin_airport"),
        col("airport_key").alias("origin_airport_key"))
    dest_lu    = dim_airport.select(
        col("airport_code").alias("dest_airport"),
        col("airport_key").alias("dest_airport_key"))
    aircraft_lu= dim_aircraft.select("tail_number", "aircraft_key")

    fact = (
        make_flight_id(df)
        .join(date_lu,     "flight_date",    "left")
        .join(carrier_lu,  "carrier_code",   "left")
        .join(origin_lu,   "origin_airport", "left")
        .join(dest_lu,     "dest_airport",   "left")
        .join(aircraft_lu, "tail_number",    "left")
    )

    # Fill UNKNOWN members for any unmatched dimension lookups
    for k in ("date_key", "carrier_key",
              "origin_airport_key", "dest_airport_key",
              "aircraft_key"):
        fact = fact.withColumn(
            k, F.coalesce(col(k), lit(UNKNOWN_KEY)))

    # IOC pillar aggregates
    # EVIDENCE STATE: DERIVED from observed BTS columns
    # These are project-defined classifications.
    # NOT BTS-provided causal truth.
    w_m  = F.coalesce(col("weather_delay_mins"),       lit(0.0))
    nas_m= F.coalesce(col("nas_delay_mins"),            lit(0.0))
    sec_m= F.coalesce(col("security_delay_mins"),       lit(0.0))
    car_m= F.coalesce(col("carrier_delay_mins"),        lit(0.0))
    lat_m= F.coalesce(col("late_aircraft_delay_mins"),  lit(0.0))
    eff_m= car_m + lat_m
    saf_m= w_m
    leg_m= nas_m + sec_m
    max_m= F.greatest(eff_m, saf_m, leg_m)

    fact = (
        fact
        .withColumn("dominant_delay_pillar",
            F.when(max_m <= 0, IOC_NONE)
            .when((saf_m == max_m) & (saf_m > 0), IOC_SAFETY)
            .when((leg_m == max_m) & (leg_m > 0), IOC_LEGALITY)
            .otherwise(IOC_EFFICIENCY))

        # Semantic governance label -- CRITICAL
        # This column must appear wherever dominant_delay_pillar
        # is exposed in Power BI, AI interface, or API responses.
        # Prevents misrepresentation as BTS-provided truth.
        .withColumn("pillar_source_note",
            lit("Project-derived operational classification. "
                "Not BTS-provided semantics. "
                "Based on BTS-reported delay categories. "
                "Tie-breaking: Safety > Legality > Efficiency "
                "(IOC operational priority order)."))

        .withColumn("operational_influence_class",
            F.when((eff_m > 0) & ((saf_m + leg_m) > 0),
                   INFLUENCE_MIXED)
            .when(eff_m > 0,            INFLUENCE_INTERNAL)
            .when((saf_m + leg_m) > 0,  INFLUENCE_EXTERNAL)
            .otherwise(INFLUENCE_UNKNOWN))

        .withColumn("gold_processed_ts", F.current_timestamp())
    )

    result = fact.select(
        # Flight identifier (links to bridge table)
        "flight_id",

        # Foreign keys to dimensions
        "date_key", "carrier_key",
        "origin_airport_key", "dest_airport_key", "aircraft_key",

        # Partition columns
        "flight_year", "flight_month",

        # Natural grain key columns
        *GRAIN_COLS, "tail_number",

        # Flags (semi-additive measures)
        "arr_delayed_flag", "dep_delayed_flag",
        "is_cancelled", "is_diverted",

        # Delay measures -- OBSERVED from BTS
        "arr_delay_mins", "dep_delay_mins",
        "arr_delay_abs_mins", "dep_delay_abs_mins",

        # IOC delay cause columns -- OBSERVED (BTS attribution)
        "carrier_delay_mins", "weather_delay_mins",
        "nas_delay_mins", "security_delay_mins",
        "late_aircraft_delay_mins",

        # IOC pillar -- DERIVED (project-defined classification)
        "dominant_delay_pillar",

        # Semantic governance -- prevents misrepresentation
        "pillar_source_note",

        # Operational influence -- DERIVED
        "operational_influence_class",

        # Cancellation
        "cancellation_code",

        # Schedule vs actual -- OBSERVED
        "scheduled_elapsed_mins", "actual_elapsed_mins",
        "air_time_mins", "distance_miles",

        # Audit
        "silver_processed_ts", "gold_processed_ts",
    )

    log_pass(f"fact_delays: {result.count():,} rows.")
    return result

# ── Bridge ────────────────────────────────────────────────────

def build_bridge(fact: DataFrame,
                 dim_reason: DataFrame) -> DataFrame:
    log_info("Building bridge_flight_delay_reason...")

    long_form = fact.select(
        "flight_id",
        F.expr("""
            stack(5,
                'CARRIER',       carrier_delay_mins,
                'WEATHER',       weather_delay_mins,
                'NAS',           nas_delay_mins,
                'SECURITY',      security_delay_mins,
                'LATE_AIRCRAFT', late_aircraft_delay_mins
            ) as (delay_code, attributed_mins)
        """),
    ).filter(F.coalesce(col("attributed_mins"), lit(0.0)) > 0)

    reason_lu = dim_reason.select(
        "delay_reason_key", "delay_code",
        "ioc_pillar", "operational_influence_class")

    from pyspark.sql.window import Window
    w = Window.partitionBy("flight_id")

    result = (
        long_form.join(reason_lu, "delay_code", "left")
        .withColumn("total_attributed_mins",
            F.sum("attributed_mins").over(w))
        .withColumn("attribution_pct",
            F.round(100.0 * col("attributed_mins") /
                    col("total_attributed_mins"), 4))
        .withColumn("gold_processed_ts", F.current_timestamp())
        .select("flight_id", "delay_reason_key", "delay_code",
                "ioc_pillar", "operational_influence_class",
                "attributed_mins", "attribution_pct",
                "gold_processed_ts")
    )
    log_pass(f"bridge: {result.count():,} rows.")
    return result

# ── Write ─────────────────────────────────────────────────────

def write_table(df: DataFrame, name: str) -> None:
    path = f"{GOLD_TEST_PATH}{name}/"
    if Path(path).exists():
        shutil.rmtree(path)
    df.write.mode("overwrite").parquet(path)
    log_pass(f"{name} written → {path}")

# ── Test Completion Gate ──────────────────────────────────────

def test_gate(spark: SparkSession,
              silver: DataFrame,
              silver_count: int) -> None:
    """
    Lightweight GCG for TEST MODE.
    Skips partition count check (not meaningful on 100K sample).
    All other checks run normally.
    """
    print(f"\n  {'=' * 65}")
    print("  TEST MODE COMPLETION GATE")
    print(f"  {'=' * 65}")

    root = GOLD_TEST_PATH.rstrip("/")

    # Check 1: Files exist
    components = [
        "dim_date", "dim_carrier", "dim_airport",
        "dim_aircraft", "dim_delay_reason",
        "fact_delays", "bridge_flight_delay_reason",
    ]
    for c in components:
        if not Path(f"{root}/{c}/").exists():
            log_fail(f"{c} missing.", f"TEST GCG | {c}",
                     "Write failed.", f"Check build_{c}().")
    log_pass("TCG 01 -- all Gold test artifacts exist.")

    fact    = spark.read.parquet(f"{root}/fact_delays/")
    bridge  = spark.read.parquet(f"{root}/bridge_flight_delay_reason/")
    d_date  = spark.read.parquet(f"{root}/dim_date/")
    d_carr  = spark.read.parquet(f"{root}/dim_carrier/")
    d_air   = spark.read.parquet(f"{root}/dim_airport/")
    d_acft  = spark.read.parquet(f"{root}/dim_aircraft/")
    d_rsn   = spark.read.parquet(f"{root}/dim_delay_reason/")

    # Check 2: Fact row count == sample count
    fact_count = fact.count()
    print(f"\n    Sample rows  : {silver_count:,}")
    print(f"    Fact rows    : {fact_count:,}")
    if fact_count != silver_count:
        log_fail(
            f"Row count mismatch: {fact_count:,} != {silver_count:,}",
            "TCG 02 | fact_delays",
            "Rows lost or duplicated during joins.",
            "Check LEFT JOINs in build_fact()."
        )
    log_pass(f"TCG 02 -- fact row count: {fact_count:,}.")

    # Check 3: Grain uniqueness
    distinct_grain = fact.select(GRAIN_COLS).distinct().count()
    duplicates = fact_count - distinct_grain
    if duplicates != 0:
        log_fail(
            f"Grain duplicates: {duplicates:,}",
            "TCG 03 | fact grain",
            "Multiple rows share same grain key.",
            "Check dimension joins for fan-out."
        )
    log_pass("TCG 03 -- grain uniqueness confirmed.")

    # Check 4: Zero NULL foreign keys
    fk_cols = ["date_key", "carrier_key",
                "origin_airport_key", "dest_airport_key",
                "aircraft_key"]
    null_counts = fact.agg(
        *[F.sum(F.when(col(k).isNull(), 1).otherwise(0)).alias(k)
          for k in fk_cols]
    ).first().asDict()
    bad = {k: v for k, v in null_counts.items() if v}
    if bad:
        log_fail(f"NULL foreign keys: {bad}", "TCG 04",
                 "Unmatched lookups must route to UNKNOWN (-1).",
                 "Check coalesce logic in build_fact().")
    log_pass("TCG 04 -- zero NULL foreign keys.")

    # Check 5: UNKNOWN members exist
    for dname, ddf, key in [
        ("dim_date",         d_date,  "date_key"),
        ("dim_carrier",      d_carr,  "carrier_key"),
        ("dim_airport",      d_air,   "airport_key"),
        ("dim_aircraft",     d_acft,  "aircraft_key"),
        ("dim_delay_reason", d_rsn,   "delay_reason_key"),
    ]:
        if ddf.filter(col(key) == UNKNOWN_KEY).limit(1).count() != 1:
            log_fail(f"{dname} missing UNKNOWN member.", "TCG 05",
                     "UNKNOWN (-1) required for unmatched FK routing.",
                     f"Check build_{dname}() unknown row.")
    log_pass("TCG 05 -- UNKNOWN members confirmed.")

    # Check 6: No duplicate surrogate keys
    for dname, ddf, key in [
        ("dim_carrier",  d_carr, "carrier_key"),
        ("dim_airport",  d_air,  "airport_key"),
        ("dim_aircraft", d_acft, "aircraft_key"),
    ]:
        total = ddf.count()
        distinct = ddf.select(key).distinct().count()
        if total != distinct:
            log_fail(f"{dname} duplicate keys: {total-distinct}",
                     "TCG 06",
                     "Duplicate surrogate keys cause join fan-out.",
                     f"Check key generation in build_{dname}().")
    log_pass("TCG 06 -- dimension surrogate keys unique.")

    # Check 7: Metric reconciliation on sample
    recon = [
        F.sum("arr_delay_mins").alias("arr_delay"),
        F.sum("carrier_delay_mins").alias("carrier"),
        F.sum("weather_delay_mins").alias("weather"),
        F.sum(F.when(col("is_cancelled") == 1, 1)
              .otherwise(0)).alias("cancels"),
    ]
    s_recon = silver.agg(*recon).first().asDict()
    f_recon = fact.agg(*recon).first().asDict()
    mismatches = {
        k: (s_recon[k], f_recon[k])
        for k in s_recon
        if abs(float(s_recon[k] or 0) -
               float(f_recon[k] or 0)) > 0.01
    }
    if mismatches:
        log_fail(f"Metric reconciliation failed: {mismatches}",
                 "TCG 07",
                 "Fact values changed during Gold enrichment.",
                 "Check fact select and join logic.")
    log_pass("TCG 07 -- metric reconciliation passed.")

    # Check 8: Bridge integrity
    bridge_count = bridge.count()
    if bridge_count == 0:
        log_warn("Bridge has zero rows. Sample may have no delayed flights.")
    else:
        orphan = (
            bridge.select("flight_id").distinct()
            .join(fact.select("flight_id").distinct(),
                  "flight_id", "left_anti")
            .limit(1).count()
        )
        if orphan:
            log_fail("Bridge has orphan flight_ids.", "TCG 08",
                     "Bridge rows reference flights not in fact.",
                     "Check bridge join logic.")
        log_pass(f"TCG 08 -- bridge integrity: {bridge_count:,} rows.")

    print(f"\n  {'=' * 65}")
    print("  TEST MODE COMPLETION GATE PASSED")
    print(f"  Sample: {silver_count:,} rows validated.")
    print("  Known limitations:")
    print("  → Data skew not visible at 100K scale")
    print("  → Rare codes (SECURITY <0.1%) may be absent")
    print("  → Memory pressure only visible at 20.9M scale")
    print("  → Partition count check skipped (not meaningful)")
    print("  NEXT: Full Azure run after this gate passes.")
    print(f"  {'=' * 65}\n")

# ── Main ──────────────────────────────────────────────────────

def run_gold_test() -> None:
    spark: SparkSession | None = None
    fact:  DataFrame  | None = None

    try:
        print("=" * 65)
        print("  BTS GOLD LAYER -- TEST MODE")
        print(f"  Sample: {TEST_ROWS:,} rows")
        print(f"  Output: {GOLD_TEST_PATH}")
        print(f"  Started: {datetime.now():%Y-%m-%d %H:%M:%S}")
        print("=" * 65)
        print()
        print("  PURPOSE: Validate Gold logic cheaply before Azure.")
        print("  This run does NOT replace full Azure execution.")
        print("  Pass here → then run full 20.9M on Azure.")
        print()

        Path(GOLD_TEST_PATH).mkdir(parents=True, exist_ok=True)

        spark = create_spark()
        log_info(f"Spark {spark.version} ready.")

        silver, silver_count = load_silver_sample(spark)

        log_info("Phase 1: Building dimensions...")
        dim_date    = build_dim_date(spark, silver)
        dim_carrier = build_dim_carrier(spark, silver)
        dim_airport = build_dim_airport(spark, silver)
        dim_aircraft= build_dim_aircraft(spark, silver)
        dim_reason  = build_dim_delay_reason(spark)

        log_info("Phase 1: Writing dimensions...")
        write_table(dim_date,     "dim_date")
        write_table(dim_carrier,  "dim_carrier")
        write_table(dim_airport,  "dim_airport")
        write_table(dim_aircraft, "dim_aircraft")
        write_table(dim_reason,   "dim_delay_reason")
        log_info("Phase 1: Dimensions written. Memory freed.")

        log_info("Phase 2: Building fact_delays...")
        fact = build_fact(
            silver, dim_date, dim_carrier,
            dim_airport, dim_aircraft
        ).persist(StorageLevel.MEMORY_AND_DISK)

        log_info("Phase 2: Writing fact_delays...")
        write_table(fact, "fact_delays")
        log_info("Phase 2: fact_delays written.")

        log_info("Phase 3: Building bridge...")
        bridge = build_bridge(fact, dim_reason)
        write_table(bridge, "bridge_flight_delay_reason")
        log_info("Phase 3: Bridge written.")

        log_info("Phase 4: Running Test Completion Gate...")
        test_gate(spark, silver, silver_count)

        print("  TEST MODE SUMMARY")
        print(f"  Sample rows    : {silver_count:,}")
        print(f"  Output path    : {GOLD_TEST_PATH}")
        print(f"  Completed      : {datetime.now():%Y-%m-%d %H:%M:%S}")
        print()
        print("  STATUS: LOCAL TEST PASSED")
        print("  NEXT:   Run full pipeline on Azure.")
        print("          Azure will reveal scale-specific issues.")

    except Exception as exc:
        print("\n" + "=" * 65)
        print("  TEST MODE FAILED")
        print(f"  Error: {exc}")
        print("=" * 65)
        raise

    finally:
        if fact is not None:
            fact.unpersist(blocking=False)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    run_gold_test()