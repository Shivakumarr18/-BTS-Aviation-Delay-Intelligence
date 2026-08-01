"""
BTS Aviation Delay Intelligence System
=======================================
Script  : bronze_ingestion.py
Layer   : Bronze
Purpose : Ingest raw BTS monthly CSV files
          into partitioned Parquet format.
          Append-only. Never modified after write.
          Raw data preserved exactly as received.

Author  : Narsing Shiva Kumar
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

ADR     : docs/decisions/ADR-001 through ADR-007

NOTE    : This script currently contains the
          column discovery phase only.
          Full ingestion pipeline follows after
          all 37 columns are understood and
          schema is defined explicitly.
"""

import os
from pyspark.sql import SparkSession

# ── Environment Setup ─────────────────────────────────────────
os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# ── Spark Session ─────────────────────────────────────────────
spark = SparkSession.builder.appName("BTS_Bronze_ColumnDiscovery").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── STEP 1: Column Discovery ──────────────────────────────────
# WHY inferSchema=True here:
# This is a one-time discovery step only.
# We read one file to confirm exact column names
# and data types before defining our schema explicitly.
#
# Production Bronze ingestion will use inferSchema=False
# with a defined StructType schema because:
# → inferSchema scans the file TWICE (slower at scale)
# → Defined schema is predictable and version-controlled
# → Catches schema drift immediately on ingestion
# ─────────────────────────────────────────────────────────────

SAMPLE_FILE = "data/raw/January_2023.csv"

print("=" * 60)
print("  BTS BRONZE LAYER — COLUMN DISCOVERY")
print("=" * 60)

df = spark.read.csv(SAMPLE_FILE,header=True,inferSchema=True)

print(f"\n  File    : {SAMPLE_FILE}")
print(f"  Columns : {len(df.columns)}")
print(f"  Rows    : {df.count():,}")

print(f"\n  {'#':<5} {'Column Name':<30} {'Data Type'}")
print(f"  {'-'*5} {'-'*30} {'-'*15}")

for i, field in enumerate(df.schema.fields, 1):
    print(f"  {i:<5} {field.name:<30} {str(field.dataType)}")

print("\n" + "=" * 60)
print("  Next step: Define explicit StructType schema")
print("  for all 37 columns before Bronze ingestion.")
print("=" * 60)

spark.stop()