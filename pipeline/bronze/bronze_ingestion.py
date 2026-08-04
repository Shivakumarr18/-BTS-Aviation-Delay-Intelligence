"""
BTS Aviation Delay Intelligence System
=======================================
Script  : bronze_ingestion.py
Layer   : Bronze
Purpose : Ingest raw BTS monthly CSV files
          into partitioned Parquet format.
          Append-only. Never modified after write.
          Raw data preserved exactly as received.

Version : 1.0 | August 2026
Standard: Errors as a UI — every failure explains
          WHAT broke, WHERE, WHY, HOW TO FIX.

Grain   : One row = one scheduled flight
          on one calendar day (BTS definition)

Run     : .venv/Scripts/python.exe
          pipeline/bronze/bronze_ingestion.py

Input   : data/raw/*.csv (36 monthly files)
Output  : data/bronze/year=YYYY/month=MM/
          Parquet format, partitioned by year+month

ADRs    : ADR-001 surrogate keys
          ADR-005 NULL preservation
          ADR-008 column renaming (Silver only)
          ADR-009 parquet over CSV
          ADR-010 FLIGHTS column drop (Silver only)

Key decisions:
  → Bronze is append-only. Never modified.
  → Raw BTS column names preserved exactly.
  → All NULLs preserved — never filled.
  → Defined StructType schema — not inferSchema.
  → unionByName — not union (position-safe).
  → Partitioned by year + month (36 partitions).
  → ingestion_ts added for audit trail.
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')
import os
import sys
import glob
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType
)

# ── Environment Setup ─────────────────────────────────────────
os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PATH"]                  = os.environ["PATH"] + r";C:\hadoop\bin"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# ── Constants ─────────────────────────────────────────────────
RAW_PATH    = "data/raw/"
BRONZE_PATH = "data/bronze/"

# ── BTS Defined Schema ────────────────────────────────────────
# WHY defined schema over inferSchema:
# → inferSchema scans every file TWICE (slow at 20.9M rows)
# → inferSchema guesses types incorrectly for NULL-heavy columns
#   e.g. ARR_DEL15 becomes DoubleType instead of IntegerType
# → Defined schema is predictable and version-controlled
# → Catches schema drift immediately on ingestion
#
# WHY StringType for FL_DATE:
# → BTS stores as "2024-01-15" (quoted string in CSV)
# → Bronze preserves raw format exactly
# → Silver converts to DateType using to_date()
#
# WHY DoubleType for delay columns:
# → BTS publishes these as decimals (e.g. 45.0)
# → NULLs prevent inferSchema from using IntegerType
# → Silver casts flags (DEP_DEL15, ARR_DEL15) to IntegerType
# ─────────────────────────────────────────────────────────────

BTS_SCHEMA = StructType([
    # ── Time Period ───────────────────────────────────────────
    StructField("YEAR",              IntegerType(), True),
    StructField("MONTH",             IntegerType(), True),
    StructField("DAY_OF_MONTH",      IntegerType(), True),
    StructField("DAY_OF_WEEK",       IntegerType(), True),
    StructField("FL_DATE",           StringType(),  True),
    # WHY StringType: BTS stores as quoted string "2024-01-15"
    # Convert to DateType in Silver layer only.

    # ── Airline Identity ──────────────────────────────────────
    StructField("OP_UNIQUE_CARRIER", StringType(),  True),
    StructField("TAIL_NUM",          StringType(),  True),
    # WHY nullable: 48,139 NULLs confirmed (0.23%)
    # Unknown aircraft → aircraft_key=-1 in dim_aircraft
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
    # WHY nullable: 1 NULL confirmed (BTS reporting anomaly)

    # ── Origin Airport ────────────────────────────────────────
    StructField("ORIGIN",            StringType(),  True),
    StructField("ORIGIN_CITY_NAME",  StringType(),  True),
    StructField("ORIGIN_STATE_ABR",  StringType(),  True),

    # ── Destination Airport ───────────────────────────────────
    StructField("DEST",              StringType(),  True),
    StructField("DEST_CITY_NAME",    StringType(),  True),
    StructField("DEST_STATE_ABR",    StringType(),  True),

    # ── Departure Performance ─────────────────────────────────
    StructField("CRS_DEP_TIME",      IntegerType(), True),
    StructField("DEP_TIME",          IntegerType(), True),
    # WHY nullable: 275,298 NULLs — NULL when CANCELLED=1
    StructField("DEP_DELAY",         DoubleType(),  True),
    # WHY nullable: 276,090 NULLs — NULL when CANCELLED=1
    StructField("DEP_DELAY_NEW",     DoubleType(),  True),
    StructField("DEP_DEL15",         DoubleType(),  True),
    # WHY DoubleType not IntegerType: inferSchema promotes to Double when NULLs present. Silver casts to Integer.

    # ── Arrival Performance ───────────────────────────────────
    StructField("CRS_ARR_TIME",      IntegerType(), True),
    StructField("ARR_TIME",          IntegerType(), True),
    # WHY nullable: 292,084 NULLs — NULL when CANCELLED or DIVERTED
    StructField("ARR_DELAY",         DoubleType(),  True),
    # WHY nullable: 340,445 NULLs — NULL when CANCELLED or DIVERTED
    StructField("ARR_DELAY_NEW",     DoubleType(),  True),
    StructField("ARR_DEL15",         DoubleType(),  True),
    # CRITICAL: controls 79.11% NULL pattern on delay cause columns
    # Silver casts to IntegerType

    # ── Cancellation and Diversion ────────────────────────────
    StructField("CANCELLED",         DoubleType(),  True),
    # Silver casts to IntegerType (0 or 1)
    StructField("CANCELLATION_CODE", StringType(),  True),
    # WHY nullable: 98.63% NULL — NULL when CANCELLED=0 (correct)
    # Values: A=Carrier, B=Weather, C=NAS, D=Security
    StructField("DIVERTED",          DoubleType(),  True),
    # Silver casts to IntegerType (0 or 1)

    # ── Flight Summaries ──────────────────────────────────────
    StructField("CRS_ELAPSED_TIME",   DoubleType(), True),
    # WHY nullable: 8 NULLs — BTS reporting anomaly. Preserve.
    StructField("ACTUAL_ELAPSED_TIME",DoubleType(), True),
    # WHY nullable: 340,445 NULLs — NULL when CANCELLED or DIVERTED
    StructField("AIR_TIME",           DoubleType(), True),
    # WHY nullable: 340,445 NULLs — NULL when CANCELLED or DIVERTED
    StructField("FLIGHTS",            DoubleType(), True),
    # NOTE: Always = 1.0 across all 20,928,599 rows.
    # Confirmed by Check C14. Dropped in Silver (ADR-010).
    # Bronze preserves it (append-only principle).
    StructField("DISTANCE",           DoubleType(), True),

    # ── Delay Cause Columns ───────────────────────────────────
    # ALL 5 have identical NULL count: 16,557,293 (79.11%)
    # ALL 5 must be NULL when ARR_DEL15 = 0
    # Validated: 0 violations across 20,928,599 rows
    # NEVER replace these NULLs with 0 — they carry
    # business meaning: flight was on time.
    # IOC Pillar mapping in dim_delay_reason:
    # CARRIER_DELAY       → Efficiency (controllable)
    # WEATHER_DELAY       → Safety (uncontrollable)
    # NAS_DELAY           → Legality (uncontrollable)
    # SECURITY_DELAY      → Legality (uncontrollable)
    # LATE_AIRCRAFT_DELAY → Efficiency (cascade — controllable)
    StructField("CARRIER_DELAY",      DoubleType(), True),
    StructField("WEATHER_DELAY",      DoubleType(), True),
    StructField("NAS_DELAY",          DoubleType(), True),
    StructField("SECURITY_DELAY",     DoubleType(), True),
    StructField("LATE_AIRCRAFT_DELAY",DoubleType(), True),
])

# ── Helper Functions ──────────────────────────────────────────

def log_info(message: str) -> None:
    """Structured info message with timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] INFO  : {message}")

def log_pass(message: str) -> None:
    """Structured pass message."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] PASS  : {message}")

def log_fail(what: str, where: str,
             why: str, fix: str) -> None:
    """
    Structured failure — WHAT / WHERE / WHY / HOW TO FIX.
    Silent failures are not acceptable.
    """
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n  [{ts}] FAILED")
    print(f"  WHAT  : {what}")
    print(f"  WHERE : {where}")
    print(f"  WHY   : {why}")
    print(f"  FIX   : {fix}\n")

# ── Spark Session ─────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """Create and return configured Spark session."""
    return SparkSession.builder \
        .appName("BTS_Bronze_Ingestion_v1.0") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY") \
        .getOrCreate()

# ── Bronze Ingestion ──────────────────────────────────────────

def check_partition_exists(year: int,
                           month: int) -> bool:
    """
    Check if Bronze partition already exists.
    WHY: Idempotency — never ingest same partition twice.
    Pattern: partition existence check (Bronze standard).
    """
    partition_path = (
        f"{BRONZE_PATH}year={year}/month={month}/"
    )
    return os.path.exists(partition_path)

def ingest_file(spark: SparkSession,
                file_path: str) -> None:
    """
    Ingest one BTS monthly CSV file into Bronze.
    Reads with defined schema.
    Adds ingestion timestamp.
    Writes as Parquet partitioned by year + month.
    Skips if partition already exists (idempotent).
    """
    filename = os.path.basename(file_path)

    # ── Step 1: Read with defined schema ─────────────────
    # WHY defined schema: see BTS_SCHEMA comments above
    try:
        df = spark.read \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .schema(BTS_SCHEMA) \
            .csv(file_path)
    except Exception as e:
        log_fail(
            what  = f"Failed to read {filename}",
            where = f"File path: {file_path}",
            why   = f"CSV read error: {str(e)}",
            fix   = "Verify file exists and is not corrupted. "
                    "Re-download from transtats.bts.gov if needed."
        )
        return

    # ── Step 2: Get year and month for partition ──────────
    try:
        first_row = df.select("YEAR", "MONTH").first()
        year  = first_row["YEAR"]
        month = first_row["MONTH"]
    except Exception as e:
        log_fail(
            what  = f"Cannot read YEAR/MONTH from {filename}",
            where = f"File: {file_path}",
            why   = f"Schema mismatch or empty file: {str(e)}",
            fix   = "Run health check C04 to verify schema. "
                    "Confirm file is not empty."
        )
        return

    # ── Step 3: Idempotency check ─────────────────────────
    # WHY: Bronze is append-only. Never overwrite.
    # Running ingestion twice must produce same result.
    if check_partition_exists(year, month):
        log_info(
            f"Partition YEAR={year}/MONTH={month} "
            f"already exists. Skipping {filename}. "
            f"(Idempotency — Bronze never overwrites)"
        )
        return

    # ── Step 4: Validate row count ────────────────────────
    row_count = df.count()
    if row_count < 400_000:
        log_fail(
            what  = f"{filename} has only {row_count:,} rows",
            where = f"File: {file_path}",
            why   = "Expected > 400,000 rows per monthly file. "
                    "File may be incomplete or corrupted.",
            fix   = "Re-download this month from "
                    "transtats.bts.gov and retry."
        )
        return

    # ── Step 5: Add ingestion timestamp ───────────────────
    # WHY: Audit trail — know exactly WHEN each record
    # was loaded into Bronze. Required for watermark tracking.
    df = df.withColumn(
        "ingestion_ts",
        current_timestamp()
    )

    # ── Step 6: Write to Parquet ──────────────────────────
    # WHY Parquet: columnar format, compressed, partition-aware
    # WHY Snappy compression: fast read/write, good compression
    # WHY partitionBy year+month: 36 partitions ~500K rows each
    # Enables partition pruning — 30x faster time-filtered queries
    try:
        output_path = BRONZE_PATH
        df.write \
            .mode("append") \
            .partitionBy("YEAR", "MONTH") \
            .parquet(output_path)

        log_pass(
    f"{filename} | year={year}/month={month} "
    f"| {row_count:,} rows written to Bronze"
)

    except Exception as e:
        log_fail(
            what  = f"Failed to write {filename} to Bronze",
            where = f"Output path: {BRONZE_PATH}",
            why   = f"Parquet write error: {str(e)}",
            fix   = "Check disk space. Verify output path exists. "
                    "Check Spark logs for details."
        )

def run_bronze_ingestion() -> None:
    """
    Main Bronze ingestion function.
    Reads all 36 BTS CSV files.
    Writes each to Bronze Parquet partitioned by year+month.
    Idempotent — safe to run multiple times.
    """
    print("=" * 65)
    print("  BTS AVIATION DELAY INTELLIGENCE SYSTEM")
    print("  Bronze Layer Ingestion v1.0")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ── Find all raw CSV files ────────────────────────────
    files = sorted(glob.glob(f"{RAW_PATH}*.csv"))

    if not files:
        log_fail(
            what  = "No CSV files found in data/raw/",
            where = f"Path: {RAW_PATH}",
            why   = "Raw BTS CSV files missing from directory",
            fix   = "Download 36 monthly CSV files from "
                    "transtats.bts.gov and place in data/raw/"
        )
        sys.exit(1)

    log_info(f"Found {len(files)} CSV files to process")
    log_info(f"Output: {BRONZE_PATH}")
    log_info(f"Schema: Defined StructType ({len(BTS_SCHEMA)} fields)")
    print()

    # ── Create Spark session ──────────────────────────────
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    log_info(f"Spark {spark.version} ready")
    print()

    # ── Create Bronze directory if not exists ─────────────
    os.makedirs(BRONZE_PATH, exist_ok=True)

    # ── Ingest each file ──────────────────────────────────
    success_count = 0
    skip_count    = 0
    fail_count    = 0

    for i, file_path in enumerate(files, 1):
        filename = os.path.basename(file_path)
        print(f"  [{i:02d}/36] Processing: {filename}")

        ingest_file(spark, file_path)

    # ── Final Summary ─────────────────────────────────────
    print()
    print("=" * 65)
    print("  BRONZE INGESTION COMPLETE")
    print("=" * 65)
    print(f"  Files processed : {len(files)}")
    print(f"  Written         : {success_count}")
    print(f"  Skipped         : {skip_count} (already existed)")
    print(f"  Failed          : {fail_count}")
    print(f"  Output path     : {BRONZE_PATH}")
    print(f"  Completed       : "
          f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    spark.stop()

# ── Entry Point ───────────────────────────────────────────────
if __name__ == "__main__":
    run_bronze_ingestion()