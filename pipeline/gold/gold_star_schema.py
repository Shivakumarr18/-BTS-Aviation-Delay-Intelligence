"""
BTS Aviation Delay Intelligence System
=======================================
Script  : gold_star_schema.py
Layer   : Gold
Version : 3.0 | August 2026
Author  : Narsing Shiva Kumar

Run
---
.venv/Scripts/python.exe pipeline/gold/gold_star_schema.py

═══════════════════════════════════════════════════════════════════
GOLD LAYER PHILOSOPHY
═══════════════════════════════════════════════════════════════════

    Bronze asks : Can I preserve the source reliably?
    Silver asks : Can I prove this data deserves trust?
    Gold asks   : What does this trusted data mean operationally?

    Peter's Principle:
    "The value is not the analytics itself. The value comes from
     whether the intelligence improves how people plan, decide,
     and operate."

    Every Gold metric must survive four questions:
    1. What exactly are we measuring?
    2. What does the data prove?
    3. What historical question does it support?
    4. Where does our evidence stop?

═══════════════════════════════════════════════════════════════════
EVIDENCE STATES
═══════════════════════════════════════════════════════════════════

    OBSERVED  -- directly measured from BTS source data.
    DERIVED   -- computed deterministically from observed data.
    MODELED   -- estimate using an explicit external assumption.
    INFERRED  -- useful interpretation that is not established fact.
    UNKNOWN   -- evidence is insufficient.

    Causation boundary (enforced throughout):
    WRONG : "Aircraft N123 caused 4 downstream delays."
    RIGHT : "Following N123's initial delay, four subsequent
             rotations were also delayed." (observed sequence only)

═══════════════════════════════════════════════════════════════════
PLATFORM BOUNDARY -- WHAT THIS SYSTEM IS AND IS NOT
═══════════════════════════════════════════════════════════════════

    CAN demonstrate (historical BTS data 2023-2025):
    → Historical delay patterns
    → Operational relationships between carriers, airports, routes
    → Delay-driver analysis by IOC pillar
    → Aircraft delay-propagation patterns via tail number tracking
    → Historical decision-support intelligence

    CANNOT claim:
    → Real-time IOC optimization
    → Live crew legality assessment
    → Live aircraft or maintenance status
    → Live weather or ATC constraint handling
    → Passenger connection impact analysis

    We build the trusted data and intelligence foundation upon
    which richer operational decision systems could eventually
    be built. We are NOT pretending to build a complete airline
    operational control system.

═══════════════════════════════════════════════════════════════════
IOC PILLAR MAPPING -- PROJECT-DEFINED CLASSIFICATION
═══════════════════════════════════════════════════════════════════

    This is our analytical classification inspired by IOC decision
    priorities. It is NOT a BTS classification. It is NOT a
    universal IOC standard. It is our project-defined mapping.

    Safety     -- WEATHER_DELAY (external, uncontrollable)
    Legality   -- NAS_DELAY + SECURITY_DELAY (regulatory)
    Efficiency -- CARRIER_DELAY + LATE_AIRCRAFT_DELAY
                  (associated with airline operations)

    Priority order for tie-breaking: Safety > Legality > Efficiency.
    This reflects IOC operational priority, not causal certainty.

═══════════════════════════════════════════════════════════════════
ARCHITECTURE DECISION RECORDS
═══════════════════════════════════════════════════════════════════

ADR-GOLD-001 | SCD Strategy
    Decision  : Snapshot dimensions for v1. Not SCD2 or SCD4.
    WHY CHOSE : BTS does not provide attribute change events.
                Without a change log we cannot build genuine SCD2.
                Claiming SCD2 without version detection is dishonest.
    WHY NOT   : Fake SCD2 gives appearance of history without substance.
    FUTURE    : Azure v2 -- Delta Lake MERGE INTO enables genuine SCD2.

ADR-GOLD-002 | Delay Reason Modeling
    Decision  : Bridge table (bridge_flight_delay_reason).
                No delay_reason_key in fact_delays.
    WHY CHOSE : One flight can have multiple BTS-attributed causes
                simultaneously. A single FK discards real information.
    WHY NOT   : Single delay_reason_key forces arbitrary tie-breaking.
                Lossy model. Architecturally dishonest.

ADR-GOLD-003 | Controllability Language
    Decision  : operational_influence_class replaces is_controllable.
                Values: INTERNAL_ASSOCIATED / EXTERNAL_ASSOCIATED
                        / MIXED / UNKNOWN.
    WHY CHOSE : "Controllable" implies airline could definitely have
                prevented the delay. BTS attribution cannot prove this.
    WHY NOT   : Binary True/False overstates what BTS can prove.

ADR-GOLD-004 | Surrogate Key Strategy
    Decision  : monotonically_increasing_id() for v1 local.
                SHA-256 deterministic keys deferred to Azure v2.
    WHY CHOSE : SHA-256 via Python UDF causes worker crashes on
                Windows local machine with 20.9M rows.
                monotonically_increasing_id() is Spark-native and
                memory-safe for local development.
    WHY NOT   : SHA-256 is non-deterministic across Spark rebuilds
                but also crashes Python workers locally.
    FUTURE    : Azure v2 -- stable key management in Delta Lake.
    LIMITATION: Keys not stable across pipeline rebuilds in v1.
                GCG checks uniqueness on every run.

ADR-GOLD-005 | Cost Model Separation
    Decision  : Financial exposure in separate modeled tables.
                model_cost_scenario + model_delay_cost.
                NOT embedded in fact_delays rows.
    WHY CHOSE : Observed delay_minutes != modeled financial_exposure.
                Separate tables allow scenario analysis.
                Luis's principle: observed != modeled.
    WHY NOT   : Embedding cost in fact blurs evidence boundary.

ADR-GOLD-006 | Aircraft Dimension Key
    Decision  : dim_aircraft keyed by tail_number only.
                carrier_code NOT part of aircraft business key.
    WHY CHOSE : One physical aircraft can appear under different
                operators. Including carrier creates one-to-many
                join risk on fact table.
    WHY NOT   : carrier relationship belongs in fact via carrier_key.

═══════════════════════════════════════════════════════════════════
LOAD ORDER (non-negotiable)
═══════════════════════════════════════════════════════════════════

    dim_date → dim_carrier → dim_airport → dim_aircraft →
    dim_delay_reason → fact_delays → bridge →
    model_cost_scenario → model_delay_cost → GCG

═══════════════════════════════════════════════════════════════════
IDEMPOTENCY
═══════════════════════════════════════════════════════════════════

    Dimensions  : Full reload (delete + rewrite). Small tables.
    Fact/Bridge : Overwrite partitioned by flight_year/flight_month.
    Modeled     : Full overwrite.
"""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, date as _date
from pathlib import Path
from typing import Sequence

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
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Configuration ─────────────────────────────────────────────

@dataclass(frozen=True)
class GoldConfig:
    silver_path:          str = os.getenv("BTS_SILVER_PATH",          "data/silver/")
    gold_path:            str = os.getenv("BTS_GOLD_PATH",            "data/gold/")
    expected_silver_rows: int = int(os.getenv("BTS_EXPECTED_SILVER_ROWS", "20928599"))
    expected_partitions:  int = int(os.getenv("BTS_EXPECTED_PARTITIONS",  "36"))
    shuffle_partitions:   int = int(os.getenv("BTS_GOLD_SHUFFLE_PARTITIONS", "4"))
    unknown_key:          int = -1
    unknown_value:        str = "Unknown"


CFG = GoldConfig()

IOC_SAFETY     = "Safety"
IOC_LEGALITY   = "Legality"
IOC_EFFICIENCY = "Efficiency"
IOC_NONE       = "None"

INFLUENCE_INTERNAL = "INTERNAL_ASSOCIATED"
INFLUENCE_EXTERNAL = "EXTERNAL_ASSOCIATED"
INFLUENCE_MIXED    = "MIXED"
INFLUENCE_UNKNOWN  = "UNKNOWN"

GRAIN_COLS = [
    "flight_date",
    "carrier_code",
    "flight_number",
    "origin_airport",
    "dest_airport",
]

CAUSE_COLUMNS = {
    "CARRIER":      "carrier_delay_mins",
    "WEATHER":      "weather_delay_mins",
    "NAS":          "nas_delay_mins",
    "SECURITY":     "security_delay_mins",
    "LATE_AIRCRAFT":"late_aircraft_delay_mins",
}

REQUIRED_SILVER_COLUMNS = {
    *GRAIN_COLS,
    "flight_year", "flight_month",
    "origin_city", "origin_state",
    "dest_city", "dest_state",
    "tail_number",
    "arr_delayed_flag", "dep_delayed_flag",
    "is_cancelled", "is_diverted",
    "arr_delay_mins", "dep_delay_mins",
    "arr_delay_abs_mins", "dep_delay_abs_mins",
    "carrier_delay_mins", "weather_delay_mins",
    "nas_delay_mins", "security_delay_mins",
    "late_aircraft_delay_mins",
    "cancellation_code",
    "scheduled_elapsed_mins", "actual_elapsed_mins",
    "air_time_mins", "distance_miles",
    "silver_processed_ts",
}

DIMENSION_NAMES = (
    "dim_date", "dim_carrier", "dim_airport",
    "dim_aircraft", "dim_delay_reason",
)

# ── Logging ───────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def log_info(message: str) -> None:
    print(f"  [{_ts()}] INFO  : {message}")

def log_pass(message: str) -> None:
    print(f"  [{_ts()}] PASS  : {message}")

def fail(what: str, where: str, why: str, fix: str) -> None:
    print(f"\n  [{_ts()}] FAILED")
    print(f"  WHAT  : {what}")
    print(f"  WHERE : {where}")
    print(f"  WHY   : {why}")
    print(f"  FIX   : {fix}\n")
    raise RuntimeError(what)

# ── Spark Session ─────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Create local Gold Spark session.

    Why local[2]: reduces parallel memory consumption.
    Why shuffle.partitions=4: reduces shuffle overhead locally.
    Why driver/executor memory 4g: gives enough heap for 20.9M rows.
    Why autoBroadcastJoinThreshold=-1: disables automatic broadcasts
    that consume large memory chunks unexpectedly.
    Azure v2: .master() removed entirely, cluster handles resources.
    """
    spark = (
        SparkSession.builder
        .appName("BTS_Gold_StarSchema_v3")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", str(CFG.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# ── Surrogate Keys ────────────────────────────────────────────

def stable_long_key(*columns: str) -> F.Column:
    """
    Surrogate key using monotonically_increasing_id.

    WHY not SHA-256:
    SHA-256 via F.sha2() uses Python UDF internally.
    On Windows local with 20.9M rows this crashes Python workers
    due to memory pressure. Spark-native monotonically_increasing_id
    avoids Python UDF overhead entirely.

    Known limitation (ADR-GOLD-004):
    Not stable across pipeline rebuilds. GCG checks uniqueness per run.
    Azure v2: persistent key management in Delta Lake.
    """
    return (F.monotonically_increasing_id() + 1).cast(LongType())


def stable_flight_id(df: DataFrame) -> DataFrame:
    """
    Add flight_id using monotonically_increasing_id.

    Same reasoning as stable_long_key -- SHA-256 deferred to Azure v2.
    flight_id links fact_delays to bridge_flight_delay_reason.
    """
    return df.withColumn(
        "flight_id",
        F.monotonically_increasing_id().cast(StringType())
    )

# ── Silver Validation ─────────────────────────────────────────

def validate_silver_input(spark: SparkSession) -> tuple[DataFrame, int]:
    """
    Re-verify Silver trust boundary before Gold starts.

    Why we exit on failure vs warn:
    Gold built on incomplete Silver = corrupted analytics silently.
    Hard stop is safer than silent partial Gold.
    """
    silver_path = Path(CFG.silver_path)

    if not silver_path.exists():
        fail(
            "Silver path does not exist.",
            CFG.silver_path,
            "Gold requires the trusted Silver layer.",
            "Run Silver and confirm its completion gate first.",
        )

    df = spark.read.parquet(CFG.silver_path)

    missing = sorted(REQUIRED_SILVER_COLUMNS - set(df.columns))
    if missing:
        fail(
            f"Silver schema missing {len(missing)} required columns: {missing}",
            "Gold pre-check",
            "Gold contracts depend on these Silver fields.",
            "Fix the Silver/Gold contract before running Gold.",
        )

    row_count = df.count()
    if row_count != CFG.expected_silver_rows:
        fail(
            f"Silver rows {row_count:,} != expected {CFG.expected_silver_rows:,}",
            "Gold pre-check",
            "Partial input would make historical analytics incomplete.",
            "Re-run Silver and confirm 36/36 trusted partitions.",
        )

    log_pass(f"Silver pre-check passed: {row_count:,} rows.")
    return df, row_count

# ── Airport Business Key Check ────────────────────────────────

def validate_airport_business_key(df_silver: DataFrame) -> None:
    """
    Require one city/state tuple per airport code.

    Why: silently DISTINCT-ing code/city/state with conflicts
    creates duplicate airport business keys and fact joins
    become one-to-many causing row count fan-out.
    """
    airports = (
        df_silver.select(
            col("origin_airport").alias("airport_code"),
            col("origin_city").alias("city"),
            col("origin_state").alias("state"),
        )
        .unionByName(
            df_silver.select(
                col("dest_airport").alias("airport_code"),
                col("dest_city").alias("city"),
                col("dest_state").alias("state"),
            )
        )
        .filter(col("airport_code").isNotNull())
    )

    conflict = (
        airports.groupBy("airport_code")
        .agg(
            F.countDistinct(
                F.struct(
                    F.coalesce(col("city"), lit("<NULL>")),
                    F.coalesce(col("state"), lit("<NULL>")),
                )
            ).alias("attribute_versions")
        )
        .filter(col("attribute_versions") > 1)
        .limit(1)
        .count()
    )

    if conflict:
        fail(
            "Airport business-key conflict detected.",
            "dim_airport source validation",
            "One airport_code maps to multiple city/state combinations.",
            "Inspect conflicting airport attributes.",
        )

    log_pass("Airport business-key contract passed.")

# ── dim_date ──────────────────────────────────────────────────

def build_dim_date(spark: SparkSession,
                   df_silver: DataFrame) -> DataFrame:
    """
    Static calendar dimension -- no SCD needed.

    Why no SCD: dates never change their properties.
    Jan 4 2023 will always be a Wednesday. Always Q1. Always Winter.

    Why date_key = YYYYMMDD integer:
    Self-documenting. date_key=20230104 tells you the date instantly.
    Faster joins than DateType columns.

    Why holiday_travel_window not is_holiday_period:
    Our calculation uses approximate windows not exact US federal dates.
    Honest naming prevents misleading analysis.
    """
    base = (
        df_silver.select(col("flight_date").alias("full_date"))
        .filter(col("full_date").isNotNull())
        .distinct()
    )

    dim = (
        base
        .withColumn("date_key",
            F.date_format(col("full_date"), "yyyyMMdd").cast(IntegerType()))
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
            .when((F.month("full_date") == 1) &
                   F.dayofmonth("full_date").between(1, 3),
                   "New Year window")
            .when((F.month("full_date") == 7) &
                   F.dayofmonth("full_date").between(1, 7),
                   "July 4th window")
            .when((F.month("full_date") == 9) &
                   F.dayofmonth("full_date").between(1, 7),
                   "Labor Day window")
            .when((F.month("full_date") == 5) &
                   F.dayofmonth("full_date").between(24, 31),
                   "Memorial Day window"))
        .withColumn("record_type", lit("STATIC"))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )

    # UNKNOWN member -- sentinel values for all columns
    unknown = spark.createDataFrame(
        [(
            CFG.unknown_key,       # date_key        IntegerType
            _date(1900, 1, 1),     # full_date        DateType
            -1,                    # year             IntegerType
            -1,                    # quarter          IntegerType
            -1,                    # month            IntegerType
            CFG.unknown_value,     # month_name       StringType
            -1,                    # day_of_month     IntegerType
            -1,                    # day_of_week      IntegerType
            CFG.unknown_value,     # day_name         StringType
            False,                 # is_weekend       BooleanType
            CFG.unknown_value,     # season           StringType
            None,                  # holiday_travel_window StringType
            "UNKNOWN_MEMBER",      # record_type      StringType
            None,                  # gold_processed_ts TimestampType
        )],
        schema=StructType([
            StructField("date_key",               IntegerType(),   True),
            StructField("full_date",              DateType(),      True),
            StructField("year",                   IntegerType(),   True),
            StructField("quarter",                IntegerType(),   True),
            StructField("month",                  IntegerType(),   True),
            StructField("month_name",             StringType(),    True),
            StructField("day_of_month",           IntegerType(),   True),
            StructField("day_of_week",            IntegerType(),   True),
            StructField("day_name",               StringType(),    True),
            StructField("is_weekend",             BooleanType(),   True),
            StructField("season",                 StringType(),    True),
            StructField("holiday_travel_window",  StringType(),    True),
            StructField("record_type",            StringType(),    True),
            StructField("gold_processed_ts",      TimestampType(), True),
        ])
    )

    return dim.unionByName(unknown)

# ── dim_carrier ───────────────────────────────────────────────

def build_dim_carrier(spark: SparkSession,
                       df_silver: DataFrame) -> DataFrame:
    """
    Snapshot carrier dimension. NOT SCD2.

    WHY snapshot: BTS does not provide carrier attribute change events.
    Fake SCD2 (all records 2023-01-01 to 9999-12-31) is architectural
    dishonesty. v1 is honest snapshot. Azure v2 = real SCD2.
    """
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

    mapping_expr = F.create_map(
        *[item for k, v in carrier_names.items()
          for item in (lit(k), lit(v))]
    )

    dim = (
        df_silver.select("carrier_code")
        .filter(col("carrier_code").isNotNull())
        .distinct()
        .withColumn("carrier_key", stable_long_key("carrier_code"))
        .withColumn("carrier_name",
            F.coalesce(mapping_expr[col("carrier_code")],
                       lit("Unmapped Carrier")))
        .withColumn("record_type", lit("SNAPSHOT_V1"))
        .withColumn("is_current", lit(True))
        .withColumn("gold_processed_ts", F.current_timestamp())
        .select("carrier_key", "carrier_code", "carrier_name",
                "record_type", "is_current", "gold_processed_ts")
    )

    unknown = spark.createDataFrame(
        [(CFG.unknown_key, CFG.unknown_value, CFG.unknown_value,
          "UNKNOWN_MEMBER", True)],
        "carrier_key long, carrier_code string, carrier_name string, "
        "record_type string, is_current boolean",
    ).withColumn("gold_processed_ts", F.current_timestamp())

    return dim.unionByName(unknown)

# ── dim_airport ───────────────────────────────────────────────

def build_dim_airport(spark: SparkSession,
                       df_silver: DataFrame) -> DataFrame:
    """
    Conformed airport snapshot dimension.

    WHY conformed: same airport appears as origin and destination.
    One physical table role-played twice in fact_delays.
    Avoids data duplication. Single source of truth.

    WHY snapshot v1: no airport attribute change log in BTS.
    Azure v2: real SCD2 via Delta Lake MERGE.
    """
    validate_airport_business_key(df_silver)

    airports = (
        df_silver.select(
            col("origin_airport").alias("airport_code"),
            col("origin_city").alias("city"),
            col("origin_state").alias("state"),
        )
        .unionByName(
            df_silver.select(
                col("dest_airport").alias("airport_code"),
                col("dest_city").alias("city"),
                col("dest_state").alias("state"),
            )
        )
        .filter(col("airport_code").isNotNull())
        .dropDuplicates(["airport_code"])
    )

    dim = (
        airports
        .withColumn("airport_key", stable_long_key("airport_code"))
        .withColumn("record_type", lit("SNAPSHOT_V1"))
        .withColumn("is_current", lit(True))
        .withColumn("gold_processed_ts", F.current_timestamp())
        .select("airport_key", "airport_code", "city", "state",
                "record_type", "is_current", "gold_processed_ts")
    )

    unknown = spark.createDataFrame(
        [(CFG.unknown_key, CFG.unknown_value, CFG.unknown_value,
          CFG.unknown_value, "UNKNOWN_MEMBER", True)],
        "airport_key long, airport_code string, city string, "
        "state string, record_type string, is_current boolean",
    ).withColumn("gold_processed_ts", F.current_timestamp())

    return dim.unionByName(unknown)

# ── dim_aircraft ──────────────────────────────────────────────

def build_dim_aircraft(spark: SparkSession,
                        df_silver: DataFrame) -> DataFrame:
    """
    Aircraft snapshot keyed ONLY by tail_number.

    WHY tail_number only (ADR-GOLD-006):
    A physical tail can appear under different operators.
    Including carrier_code creates one-to-many joins.
    Carrier relationship stays in fact via carrier_key.

    WHY UNKNOWN member:
    48,139 flights (0.23%) have NULL tail_number (confirmed Silver C11).
    Valid flights with unreported tails. Must join to UNKNOWN not fail.
    """
    dim = (
        df_silver.select("tail_number")
        .filter(col("tail_number").isNotNull())
        .distinct()
        .withColumn("aircraft_key", stable_long_key("tail_number"))
        .withColumn("record_type", lit("SNAPSHOT_V1"))
        .withColumn("is_current", lit(True))
        .withColumn("gold_processed_ts", F.current_timestamp())
        .select("aircraft_key", "tail_number",
                "record_type", "is_current", "gold_processed_ts")
    )

    unknown = spark.createDataFrame(
        [(CFG.unknown_key, CFG.unknown_value, "UNKNOWN_MEMBER", True)],
        "aircraft_key long, tail_number string, "
        "record_type string, is_current boolean",
    ).withColumn("gold_processed_ts", F.current_timestamp())

    return dim.unionByName(unknown)

# ── dim_delay_reason ──────────────────────────────────────────

def build_dim_delay_reason(spark: SparkSession) -> DataFrame:
    """
    Delay reason lookup -- SCD Type 1.

    WHY SCD Type 1: BTS delay codes are stable government classifications.
    Overwriting acceptable. No version history needed.

    WHY IOC pillar stored here:
    Single place to update if pillar mapping changes.
    Enables simple GROUP BY ioc_pillar without CASE WHEN everywhere.

    WHY operational_influence_class not is_controllable (ADR-GOLD-003):
    BTS attribution cannot prove airline controlled the delay.
    INTERNAL_ASSOCIATED is honest. CONTROLLABLE is not.
    """
    rows = [
        (1, "CARRIER",      "Carrier Delay",
         IOC_EFFICIENCY, INFLUENCE_INTERNAL, "carrier_delay_mins"),
        (2, "WEATHER",      "Weather Delay",
         IOC_SAFETY,     INFLUENCE_EXTERNAL, "weather_delay_mins"),
        (3, "NAS",          "National Aviation System Delay",
         IOC_LEGALITY,   INFLUENCE_EXTERNAL, "nas_delay_mins"),
        (4, "SECURITY",     "Security Delay",
         IOC_LEGALITY,   INFLUENCE_EXTERNAL, "security_delay_mins"),
        (5, "LATE_AIRCRAFT","Late Aircraft Delay",
         IOC_EFFICIENCY, INFLUENCE_INTERNAL, "late_aircraft_delay_mins"),
        (CFG.unknown_key, "UNKNOWN", "Unknown",
         IOC_NONE,       INFLUENCE_UNKNOWN,  None),
    ]

    schema = StructType([
        StructField("delay_reason_key",           IntegerType(), False),
        StructField("delay_code",                 StringType(),  False),
        StructField("delay_category",             StringType(),  False),
        StructField("ioc_pillar",                 StringType(),  False),
        StructField("operational_influence_class",StringType(),  False),
        StructField("bts_source_column",          StringType(),  True),
    ])

    return (
        spark.createDataFrame(rows, schema)
        .withColumn("record_type", lit("TYPE1_LOOKUP"))
        .withColumn("gold_processed_ts", F.current_timestamp())
    )

# ── Cost Scenario Table ───────────────────────────────────────

def build_cost_scenario_table(spark: SparkSession) -> DataFrame:
    """
    Separate MODELED scenario configuration from observed fact.

    WHY separate table (ADR-GOLD-005):
    Observed delay_minutes != modeled financial_exposure.
    Embedding $45/min into 20.9M fact rows blurs this line.
    Separate table allows scenario analysis without rebuilding fact.
    """
    rows = [(
        1, "REFERENCE_45_USD", 45.0, "USD",
        "Directional research-based reference only; "
        "not actual airline loss. "
        "Source: Ferguson et al., FAA/NEXTOR 2010.",
        True,
    )]
    return spark.createDataFrame(
        rows,
        "cost_scenario_key int, scenario_name string, "
        "cost_per_delay_minute double, currency string, "
        "assumption_note string, is_default boolean",
    ).withColumn("gold_processed_ts", F.current_timestamp())

# ── fact_delays ───────────────────────────────────────────────

def build_fact_delays(
        df_silver: DataFrame,
        dim_date: DataFrame,
        dim_carrier: DataFrame,
        dim_airport: DataFrame,
        dim_aircraft: DataFrame,
) -> DataFrame:
    """
    Build one-row-per-scheduled-flight fact_delays.

    WHY no delay_reason_key (ADR-GOLD-002):
    One flight can have multiple BTS-attributed causes simultaneously.
    Single FK discards real information. Bridge table handles this.

    WHY LEFT JOIN for all dimension lookups:
    Must never drop a flight due to dimension lookup failure.
    LEFT JOIN preserves all 20,928,599 Silver rows.
    Unmatched rows get UNKNOWN member key (-1).

    WHY ioc_primary_pillar as DERIVED:
    Enables simple GROUP BY without multi-column CASE WHEN every query.
    Evidence state: DERIVED (not observed directly from BTS).
    Dominant cause by minutes determines pillar.
    Tie-breaking: Safety > Legality > Efficiency (IOC priority).

    WHY operational_influence_class not is_controllable (ADR-GOLD-003):
    INTERNAL_ASSOCIATED / EXTERNAL_ASSOCIATED / MIXED is honest.
    Binary controllable/uncontrollable overclaims BTS attribution.
    """
    date_lu = dim_date.select(
        "date_key", col("full_date").alias("flight_date"))
    carrier_lu = dim_carrier.select("carrier_key", "carrier_code")
    origin_lu = dim_airport.select(
        col("airport_key").alias("origin_airport_key"),
        col("airport_code").alias("origin_airport"))
    dest_lu = dim_airport.select(
        col("airport_key").alias("dest_airport_key"),
        col("airport_code").alias("dest_airport"))
    aircraft_lu = dim_aircraft.select("aircraft_key", "tail_number")

    df = (
        stable_flight_id(df_silver)
        .join(date_lu,    "flight_date",    "left")
        .join(carrier_lu, "carrier_code",   "left")
        .join(origin_lu,  "origin_airport", "left")
        .join(dest_lu,    "dest_airport",   "left")
        .join(aircraft_lu,"tail_number",    "left")
    )

    for key_col in ("date_key", "carrier_key", "origin_airport_key",
                    "dest_airport_key", "aircraft_key"):
        df = df.withColumn(
            key_col,
            F.coalesce(col(key_col), lit(CFG.unknown_key)))

    # IOC pillar aggregates
    carrier_m = F.coalesce(col("carrier_delay_mins"),       lit(0.0))
    weather_m = F.coalesce(col("weather_delay_mins"),        lit(0.0))
    nas_m     = F.coalesce(col("nas_delay_mins"),            lit(0.0))
    security_m= F.coalesce(col("security_delay_mins"),       lit(0.0))
    late_m    = F.coalesce(col("late_aircraft_delay_mins"),  lit(0.0))

    efficiency_m = carrier_m + late_m
    safety_m     = weather_m
    legality_m   = nas_m + security_m
    max_pillar_m = F.greatest(efficiency_m, safety_m, legality_m)

    dominant_tie_count = (
        ((efficiency_m == max_pillar_m) & (efficiency_m > 0)).cast("int")
        + ((safety_m   == max_pillar_m) & (safety_m   > 0)).cast("int")
        + ((legality_m == max_pillar_m) & (legality_m > 0)).cast("int")
    )

    internal_total = efficiency_m
    external_total = safety_m + legality_m

    df = (
        df
        .withColumn("efficiency_attributed_mins", efficiency_m)
        .withColumn("safety_attributed_mins",     safety_m)
        .withColumn("legality_attributed_mins",   legality_m)
        .withColumn("dominant_delay_pillar",
            F.when(max_pillar_m <= 0, IOC_NONE)
            .when((safety_m   == max_pillar_m) & (safety_m   > 0), IOC_SAFETY)
            .when((legality_m == max_pillar_m) & (legality_m > 0), IOC_LEGALITY)
            .otherwise(IOC_EFFICIENCY))
        .withColumn("dominant_pillar_tie", dominant_tie_count > 1)
        .withColumn("has_mixed_influence",
            (internal_total > 0) & (external_total > 0))
        .withColumn("operational_influence_class",
            F.when((internal_total > 0) & (external_total > 0), INFLUENCE_MIXED)
            .when(internal_total > 0, INFLUENCE_INTERNAL)
            .when(external_total > 0, INFLUENCE_EXTERNAL)
            .otherwise(INFLUENCE_UNKNOWN))
        .withColumn("dominant_influence_class",
            F.when((internal_total <= 0) & (external_total <= 0), INFLUENCE_UNKNOWN)
            .when(internal_total > external_total, INFLUENCE_INTERNAL)
            .when(external_total > internal_total, INFLUENCE_EXTERNAL)
            .otherwise(INFLUENCE_MIXED))
        .withColumn("cancellation_pillar",
            F.when(col("is_cancelled") != 1, IOC_NONE)
            .when(col("cancellation_code") == "A", IOC_EFFICIENCY)
            .when(col("cancellation_code") == "B", IOC_SAFETY)
            .when(col("cancellation_code").isin("C", "D"), IOC_LEGALITY)
            .otherwise(IOC_NONE))
        .withColumn("schedule_elapsed_variance_mins",
            F.when(
                col("scheduled_elapsed_mins").isNotNull() &
                col("actual_elapsed_mins").isNotNull(),
                col("scheduled_elapsed_mins") - col("actual_elapsed_mins")))
    )

    return df.select(
        "flight_id",
        "date_key", "carrier_key",
        "origin_airport_key", "dest_airport_key", "aircraft_key",
        "flight_year", "flight_month",
        *GRAIN_COLS, "tail_number",
        "arr_delayed_flag", "dep_delayed_flag",
        "is_cancelled", "is_diverted",
        "arr_delay_mins", "dep_delay_mins",
        "arr_delay_abs_mins", "dep_delay_abs_mins",
        "carrier_delay_mins", "weather_delay_mins",
        "nas_delay_mins", "security_delay_mins",
        "late_aircraft_delay_mins",
        "efficiency_attributed_mins",
        "safety_attributed_mins",
        "legality_attributed_mins",
        "dominant_delay_pillar", "dominant_pillar_tie",
        "operational_influence_class", "dominant_influence_class",
        "has_mixed_influence",
        "cancellation_code", "cancellation_pillar",
        "scheduled_elapsed_mins", "actual_elapsed_mins",
        "schedule_elapsed_variance_mins",
        "air_time_mins", "distance_miles",
        "silver_processed_ts",
        F.current_timestamp().alias("gold_processed_ts"),
    )

# ── Bridge Table ──────────────────────────────────────────────

def build_bridge_delay_reason(
        fact: DataFrame,
        dim_delay_reason: DataFrame,
) -> DataFrame:
    """
    Unpivot five BTS cause columns into one row per positive attribution.

    WHY bridge table (ADR-GOLD-002):
    One delayed flight may have multiple attributed causes simultaneously.
    Bridge preserves all causes. Single FK in fact loses information.

    On-time flights create zero bridge rows.
    Delayed flights create 1-5 bridge rows (one per non-zero cause).
    """
    long_form = fact.select(
        "flight_id",
        F.expr("""
            stack(
                5,
                'CARRIER',       carrier_delay_mins,
                'WEATHER',       weather_delay_mins,
                'NAS',           nas_delay_mins,
                'SECURITY',      security_delay_mins,
                'LATE_AIRCRAFT', late_aircraft_delay_mins
            ) as (delay_code, attributed_mins)
        """),
    ).filter(F.coalesce(col("attributed_mins"), lit(0.0)) > 0)

    reason_lu = dim_delay_reason.select(
        "delay_reason_key", "delay_code",
        "ioc_pillar", "operational_influence_class")

    bridge = long_form.join(reason_lu, "delay_code", "left")

    from pyspark.sql.window import Window
    window = Window.partitionBy("flight_id")

    return (
        bridge
        .withColumn("total_attributed_mins",
            F.sum("attributed_mins").over(window))
        .withColumn("attribution_pct",
            F.round(
                100.0 * col("attributed_mins") /
                col("total_attributed_mins"), 4))
        .withColumn("gold_processed_ts", F.current_timestamp())
        .select(
            "flight_id", "delay_reason_key", "delay_code",
            "ioc_pillar", "operational_influence_class",
            "attributed_mins", "attribution_pct", "gold_processed_ts")
    )

# ── Modeled Cost Output ───────────────────────────────────────

def build_modeled_delay_cost(
        fact: DataFrame,
        cost_scenarios: DataFrame,
) -> DataFrame:
    """
    Scenario-based modeled financial exposure.

    WHY separate from fact (ADR-GOLD-005):
    This table is MODELED. Must never be presented as actual airline loss.
    Observed delay_minutes * cost_assumption = directional estimate only.
    Separate table enforces the evidence boundary structurally.
    """
    scenarios = cost_scenarios.select(
        "cost_scenario_key", "scenario_name",
        "cost_per_delay_minute", "currency")

    return (
        fact.select("flight_id", "arr_delay_abs_mins")
        .crossJoin(scenarios)
        .withColumn("estimated_delay_cost",
            F.round(
                F.coalesce(col("arr_delay_abs_mins"), lit(0.0)) *
                col("cost_per_delay_minute"), 2))
        .withColumn("evidence_state", lit("MODELED"))
        .withColumn("gold_processed_ts", F.current_timestamp())
        .select(
            "flight_id", "cost_scenario_key", "scenario_name",
            "cost_per_delay_minute", "currency",
            "estimated_delay_cost", "evidence_state",
            "gold_processed_ts")
    )

# ── Write Functions ───────────────────────────────────────────

def replace_path(path: str) -> None:
    """Delete folder if exists for idempotent reload."""
    p = Path(path)
    if p.exists():
        shutil.rmtree(p)

def write_dimension(df: DataFrame, name: str) -> None:
    """
    Write dimension -- full reload.

    WHY full reload: dimensions are small. Simple and idempotent.
    WHY not MERGE: no Delta Lake in v1 local.
    Limitation: delete + rewrite not atomic. GCG validates after.
    Azure v2: Delta Lake MERGE is atomic.
    """
    path = f"{CFG.gold_path.rstrip('/')}/{name}/"
    replace_path(path)
    df.write.mode("overwrite").parquet(path)
    log_pass(f"{name} written -> {path}")

def write_fact(df: DataFrame) -> None:
    """
    Write fact_delays partitioned by flight_year and flight_month.

    WHY partitioned: matches Silver strategy. Most queries filter by time.
    Partition pruning eliminates irrelevant partitions.
    Enables independent partition reprocessing.
    """
    path = f"{CFG.gold_path.rstrip('/')}/fact_delays/"
    replace_path(path)
    (df.write
       .mode("overwrite")
       .partitionBy("flight_year", "flight_month")
       .parquet(path))
    log_pass(f"fact_delays written -> {path}")

def write_bridge(df: DataFrame) -> None:
    path = f"{CFG.gold_path.rstrip('/')}/bridge_flight_delay_reason/"
    replace_path(path)
    df.write.mode("overwrite").parquet(path)
    log_pass(f"bridge_flight_delay_reason written -> {path}")

def write_modeled_tables(
        scenarios: DataFrame,
        modeled_cost: DataFrame) -> None:
    s_path = f"{CFG.gold_path.rstrip('/')}/model_cost_scenario/"
    c_path = f"{CFG.gold_path.rstrip('/')}/model_delay_cost/"
    replace_path(s_path)
    replace_path(c_path)
    scenarios.write.mode("overwrite").parquet(s_path)
    modeled_cost.write.mode("overwrite").parquet(c_path)
    log_pass("Modeled cost tables written.")

# ── Gold Completion Gate ──────────────────────────────────────

def _physical_fact_partitions() -> set[tuple[int, int]]:
    """Read physical year/month partition folders from disk."""
    root = Path(f"{CFG.gold_path.rstrip('/')}/fact_delays/")
    found: set[tuple[int, int]] = set()
    if not root.exists():
        return found
    for year_dir in root.glob("flight_year=*"):
        try:
            y = int(year_dir.name.split("=", 1)[1])
        except (ValueError, IndexError):
            continue
        for month_dir in year_dir.glob("flight_month=*"):
            try:
                m = int(month_dir.name.split("=", 1)[1])
            except (ValueError, IndexError):
                continue
            found.add((y, m))
    return found

def _assert_dim_key_unique(df: DataFrame,
                            key_col: str,
                            dim_name: str) -> None:
    dup = (df.groupBy(key_col).count()
             .filter(col("count") > 1).limit(1).count())
    if dup:
        fail(f"Duplicate {key_col} detected.", f"GCG | {dim_name}",
             "Dimension surrogate keys must be unique.",
             "Inspect key generation logic.")

def _assert_fk(fact: DataFrame, dim: DataFrame,
                fact_key: str, dim_key: str, label: str) -> None:
    invalid = (
        fact.filter(col(fact_key) != CFG.unknown_key)
        .select(col(fact_key).alias("fk")).distinct()
        .join(dim.select(col(dim_key).alias("fk")).distinct(),
              "fk", "left_anti")
        .limit(1).count()
    )
    if invalid:
        fail(f"Referential-integrity failure for {fact_key}.",
             f"GCG | {label}",
             "Fact contains a non-UNKNOWN key absent from dimension.",
             "Inspect dimension build and fact lookup logic.")

def gold_completion_gate(
        spark: SparkSession,
        df_silver: DataFrame,
        silver_count: int) -> None:
    """
    Validate physical Gold artifacts after write.

    WHY count alone is not enough:
    Equal row counts can hide offsetting row loss and duplication.
    A bad join can lose 10,000 rows and duplicate 10,000 others.
    Final count = 20,928,599. PASS. But Gold is corrupted.

    10 checks cover: existence, count, grain, FK NULLs,
    UNKNOWN members, key uniqueness, referential integrity,
    partition completeness, metric reconciliation, bridge integrity.
    """
    print("\n" + "=" * 70)
    print("  GOLD COMPLETION GATE")
    print("=" * 70)

    root = CFG.gold_path.rstrip("/")
    required_paths = [
        *(f"{root}/{name}/" for name in DIMENSION_NAMES),
        f"{root}/fact_delays/",
        f"{root}/bridge_flight_delay_reason/",
        f"{root}/model_cost_scenario/",
        f"{root}/model_delay_cost/",
    ]

    missing = [p for p in required_paths if not Path(p).exists()]
    if missing:
        fail(f"Missing Gold artifacts: {missing}", "GCG Check 01",
             "A successful Spark action does not prove artifacts exist.",
             "Review failed writes and rebuild Gold.")
    log_pass("GCG 01 -- all required Gold artifacts physically exist.")

    fact       = spark.read.parquet(f"{root}/fact_delays/")
    bridge     = spark.read.parquet(f"{root}/bridge_flight_delay_reason/")
    dim_date   = spark.read.parquet(f"{root}/dim_date/")
    dim_carrier= spark.read.parquet(f"{root}/dim_carrier/")
    dim_airport= spark.read.parquet(f"{root}/dim_airport/")
    dim_aircraft=spark.read.parquet(f"{root}/dim_aircraft/")
    dim_reason = spark.read.parquet(f"{root}/dim_delay_reason/")

    fact_count = fact.count()
    if fact_count != silver_count:
        fail(f"Gold fact rows {fact_count:,} != Silver {silver_count:,}",
             "GCG Check 02",
             "Rows were lost or duplicated during dimension joins.",
             "Inspect fact joins and business-key uniqueness.")
    log_pass(f"GCG 02 -- fact row reconciliation: {fact_count:,}.")

    dup = (fact.groupBy("flight_id").count()
               .filter(col("count") > 1).limit(1).count())
    if dup:
        fail("Duplicate fact grain / flight_id detected.", "GCG Check 03",
             "One flight grain mapped to multiple Gold rows.",
             "Inspect dimension one-to-many joins.")
    log_pass("GCG 03 -- fact grain uniqueness passed.")

    key_cols = ["date_key", "carrier_key", "origin_airport_key",
                "dest_airport_key", "aircraft_key"]
    null_counts = fact.agg(
        *[F.sum(F.when(col(k).isNull(), 1).otherwise(0)).alias(k)
          for k in key_cols]
    ).first().asDict()
    bad = {k: v for k, v in null_counts.items() if v}
    if bad:
        fail(f"Unexpected NULL foreign keys: {bad}", "GCG Check 04",
             "Unmatched lookups must route to UNKNOWN (-1), never NULL.",
             "Inspect key coalesce logic in build_fact_delays.")
    log_pass("GCG 04 -- all fact foreign keys non-NULL.")

    for dim_name, dim_df, key_col in [
        ("dim_date",         dim_date,    "date_key"),
        ("dim_carrier",      dim_carrier, "carrier_key"),
        ("dim_airport",      dim_airport, "airport_key"),
        ("dim_aircraft",     dim_aircraft,"aircraft_key"),
        ("dim_delay_reason", dim_reason,  "delay_reason_key"),
    ]:
        exists = dim_df.filter(
            col(key_col) == CFG.unknown_key).limit(1).count()
        if exists != 1:
            fail(f"{dim_name} missing UNKNOWN member.", "GCG Check 05",
                 "Unknown-member routing requires stable -1 record.",
                 f"Fix build_{dim_name}() unknown member creation.")
    log_pass("GCG 05 -- UNKNOWN members confirmed in all dimensions.")

    _assert_dim_key_unique(dim_date,    "date_key",        "dim_date")
    _assert_dim_key_unique(dim_carrier, "carrier_key",     "dim_carrier")
    _assert_dim_key_unique(dim_airport, "airport_key",     "dim_airport")
    _assert_dim_key_unique(dim_aircraft,"aircraft_key",    "dim_aircraft")
    _assert_dim_key_unique(dim_reason,  "delay_reason_key","dim_delay_reason")
    log_pass("GCG 06 -- dimension surrogate keys unique.")

    _assert_fk(fact, dim_date,    "date_key",           "date_key",    "date")
    _assert_fk(fact, dim_carrier, "carrier_key",        "carrier_key", "carrier")
    _assert_fk(fact, dim_airport, "origin_airport_key", "airport_key", "origin")
    _assert_fk(fact, dim_airport, "dest_airport_key",   "airport_key", "dest")
    _assert_fk(fact, dim_aircraft,"aircraft_key",       "aircraft_key","aircraft")
    log_pass("GCG 07 -- fact referential integrity passed.")

    physical_parts = _physical_fact_partitions()
    if len(physical_parts) != CFG.expected_partitions:
        fail(f"Found {len(physical_parts)} partitions; "
             f"expected {CFG.expected_partitions}.",
             "GCG Check 08",
             "Missing partitions silently corrupt period comparisons.",
             "Inspect flight_year/flight_month partition folders.")
    log_pass(f"GCG 08 -- {len(physical_parts)}/{CFG.expected_partitions} "
             f"partitions confirmed.")

    recon_exprs = [
        F.sum("arr_delay_mins").alias("arr_delay_sum"),
        F.sum("dep_delay_mins").alias("dep_delay_sum"),
        F.sum(F.when(col("is_cancelled") == 1, 1).otherwise(0))
          .alias("cancelled_count"),
        F.sum("carrier_delay_mins").alias("carrier_sum"),
        F.sum("weather_delay_mins").alias("weather_sum"),
        F.sum("nas_delay_mins").alias("nas_sum"),
        F.sum("security_delay_mins").alias("security_sum"),
        F.sum("late_aircraft_delay_mins").alias("late_aircraft_sum"),
    ]
    silver_recon = df_silver.agg(*recon_exprs).first().asDict()
    fact_recon   = fact.agg(*recon_exprs).first().asDict()

    mismatches = {}
    for key in silver_recon:
        a = silver_recon[key]
        b = fact_recon[key]
        if a is None and b is None:
            continue
        if abs(float(a or 0) - float(b or 0)) > 0.01:
            mismatches[key] = (a, b)
    if mismatches:
        fail(f"Silver-to-Gold metric reconciliation failed: {mismatches}",
             "GCG Check 09",
             "Fact values changed during Gold enrichment.",
             "Inspect fact selection and joins.")
    log_pass("GCG 09 -- operational metric reconciliation passed.")

    dup_bridge = (
        bridge.groupBy("flight_id", "delay_reason_key").count()
        .filter(col("count") > 1).limit(1).count())
    orphan_flight = (
        bridge.select("flight_id").distinct()
        .join(fact.select("flight_id").distinct(), "flight_id", "left_anti")
        .limit(1).count())
    orphan_reason = (
        bridge.select("delay_reason_key").distinct()
        .join(dim_reason.select("delay_reason_key").distinct(),
              "delay_reason_key", "left_anti")
        .limit(1).count())

    bridge_total = (bridge.agg(F.sum("attributed_mins").alias("t"))
                         .first()["t"] or 0.0)
    fact_cause_total = (
        fact.agg(F.sum(
            F.coalesce(col("carrier_delay_mins"),       lit(0.0)) +
            F.coalesce(col("weather_delay_mins"),        lit(0.0)) +
            F.coalesce(col("nas_delay_mins"),            lit(0.0)) +
            F.coalesce(col("security_delay_mins"),       lit(0.0)) +
            F.coalesce(col("late_aircraft_delay_mins"),  lit(0.0))
        ).alias("t")).first()["t"] or 0.0)

    if dup_bridge or orphan_flight or orphan_reason or \
       abs(float(bridge_total) - float(fact_cause_total)) > 0.01:
        fail("Bridge integrity check failed.", "GCG Check 10",
             "Bridge has duplicates, orphans, or attribution mismatch.",
             "Inspect build_bridge_delay_reason() unpivot logic.")
    log_pass("GCG 10 -- bridge integrity and attribution passed.")

    print("\n" + "=" * 70)
    print("  GOLD COMPLETION GATE PASSED")
    print("  Trusted Gold may be used by BI and intelligence layers.")
    print("=" * 70)

# ── Main ──────────────────────────────────────────────────────

def run_gold() -> None:
    """
    Main Gold pipeline orchestrator.

    Load order: dims written first → fact → bridge → modeled → GCG.
    WHY dims first: dimensions are tiny (16-7000 rows).
    Writing them before fact frees memory for 20.9M row fact build.
    Competing with dimension writes causes Python worker crash locally.
    try/finally guarantees spark.stop() even on failure.
    """
    spark: SparkSession | None = None
    fact:  DataFrame  | None = None

    try:
        print("=" * 70)
        print("  BTS AVIATION DELAY INTELLIGENCE SYSTEM")
        print("  Gold Layer -- Star Schema v3.0")
        print(f"  Started : {datetime.now():%Y-%m-%d %H:%M:%S}")
        print("=" * 70)
        print("  Grain      : one scheduled flight on one calendar day")
        print("  Dimensions : date, carrier, airport, aircraft, delay_reason")
        print("  Bridge     : one flight -> many BTS delay reasons")
        print("  Boundary   : historical intelligence; no real-time IOC claims")
        print()

        Path(CFG.gold_path).mkdir(parents=True, exist_ok=True)

        spark = create_spark_session()
        log_info(f"Spark {spark.version} ready.")

        silver, silver_count = validate_silver_input(spark)

        # ── Phase 1: Build + Write dimensions first ────────────
        # WHY: dimensions are tiny (16-7000 rows).
        # Writing them BEFORE fact frees memory.
        # fact (20.9M rows) needs all available RAM.
        # Competing with dimension writes causes Python worker crash.
        log_info("Phase 1: Building dimensions...")
        dim_date     = build_dim_date(spark, silver)
        dim_carrier  = build_dim_carrier(spark, silver)
        dim_airport  = build_dim_airport(spark, silver)
        dim_aircraft = build_dim_aircraft(spark, silver)
        dim_reason   = build_dim_delay_reason(spark)

        log_info("Phase 1: Writing dimensions...")
        write_dimension(dim_date,     "dim_date")
        write_dimension(dim_carrier,  "dim_carrier")
        write_dimension(dim_airport,  "dim_airport")
        write_dimension(dim_aircraft, "dim_aircraft")
        write_dimension(dim_reason,   "dim_delay_reason")
        log_info("Phase 1: All dimensions written. Memory freed.")

        # ── Phase 2: Build + Write fact ────────────────────────
        # Dimensions written and out of memory.
        # fact gets full available RAM now.
        log_info("Phase 2: Building fact_delays...")
        fact = build_fact_delays(
            silver, dim_date, dim_carrier,
            dim_airport, dim_aircraft
        ).persist(StorageLevel.MEMORY_AND_DISK)

        log_info("Phase 2: Writing fact_delays...")
        write_fact(fact)
        log_info("Phase 2: fact_delays written.")

        # ── Phase 3: Build + Write bridge ──────────────────────
        log_info("Phase 3: Building bridge_flight_delay_reason...")
        bridge = build_bridge_delay_reason(fact, dim_reason)

        log_info("Phase 3: Writing bridge...")
        write_bridge(bridge)
        log_info("Phase 3: Bridge written.")

        # ── Phase 4: Build + Write modeled tables ──────────────
        log_info("Phase 4: Building cost scenario and modeled cost...")
        cost_scenarios = build_cost_scenario_table(spark)
        modeled_cost   = build_modeled_delay_cost(fact, cost_scenarios)

        log_info("Phase 4: Writing modeled tables...")
        write_modeled_tables(cost_scenarios, modeled_cost)
        log_info("Phase 4: Modeled tables written.")

        # ── Phase 5: Gold Completion Gate ──────────────────────
        log_info("Phase 5: Running Gold Completion Gate...")
        gold_completion_gate(spark, silver, silver_count)

        print("\n  GOLD SUMMARY")
        print(f"  Silver input   : {silver_count:,} rows")
        print("  Dimensions     : 5")
        print("  Bridge         : bridge_flight_delay_reason")
        print("  Modeled layer  : model_cost_scenario + model_delay_cost")
        print("  Evidence states: OBSERVED / DERIVED / MODELED")
        print("  Status         : TRUSTED GOLD COMPLETE")

    except Exception as exc:
        print("\n" + "=" * 70)
        print("  GOLD PIPELINE FAILED")
        print(f"  Error: {exc}")
        print("=" * 70)
        raise

    finally:
        if fact is not None:
            fact.unpersist(blocking=False)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    run_gold()

