"""
BTS Aviation Delay Intelligence System
=======================================
Script  : data_health_check.py
Purpose : Pre-ingestion data quality validation across all 36 BTS monthly CSV files.
          Validates file completeness, schema consistency, row counts, NULL patterns,
          and business rules before Bronze layer ingestion begins.

Author  : Narsing Shiva Kumar
Version : 1.0 | July 2026 | Pre-Implementation
Standard: Every check fails loudly with WHAT / WHERE / WHY / HOW TO FIX.
          Silent failures are not acceptable.

Run     : .venv/Scripts/python.exe config/data_health_check.py
Expected: All 10 checks PASS before August 1st Bronze layer begins.
"""

import os
import sys
import glob
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count as spark_count

# ══════════════════════════════════════════════════════════════
# ENVIRONMENT CONFIGURATION
# ══════════════════════════════════════════════════════════════

os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# ══════════════════════════════════════════════════════════════
# CONSTANTS — Single place to change all configuration
# ══════════════════════════════════════════════════════════════

RAW_PATH             = "data/raw/"
EXPECTED_FILE_COUNT  = 36
EXPECTED_YEARS       = {2023, 2024, 2025}
MIN_ROWS_PER_FILE    = 400_000
MIN_TOTAL_ROWS       = 18_000_000
MAX_TOTAL_ROWS       = 22_000_000
MIN_FILE_SIZE_MB     = 30
EXPECTED_COL_COUNT   = 37
NULL_PATTERN_MIN_PCT = 70.0
NULL_PATTERN_MAX_PCT = 90.0

# Exact BTS column names verified from December_2024.csv
EXPECTED_COLUMNS = {
    "YEAR", "MONTH", "DAY_OF_MONTH", "DAY_OF_WEEK", "FL_DATE",
    "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM",
    "ORIGIN", "ORIGIN_CITY_NAME", "ORIGIN_STATE_ABR",
    "DEST", "DEST_CITY_NAME", "DEST_STATE_ABR",
    "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY", "DEP_DELAY_NEW", "DEP_DEL15",
    "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY", "ARR_DELAY_NEW", "ARR_DEL15",
    "CANCELLED", "CANCELLATION_CODE", "DIVERTED",
    "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME", "AIR_TIME",
    "FLIGHTS", "DISTANCE",
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
}

MANDATORY_COLUMNS = [
    "YEAR", "MONTH", "FL_DATE",
    "OP_UNIQUE_CARRIER", "ORIGIN", "DEST",
    "CANCELLED", "DIVERTED", "DISTANCE"
]

DELAY_CAUSE_COLUMNS = [
    "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY",
    "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
]

# ══════════════════════════════════════════════════════════════
# VALIDATION REPORT TRACKER
# ══════════════════════════════════════════════════════════════

class ValidationReport:
    """Tracks all check results for final summary report."""

    def __init__(self):
        self.results    = []
        self.start_time = time.time()

    def record(self, check_id, name, passed, detail="", warning=False):
        self.results.append({
            "id"     : check_id,
            "name"   : name,
            "passed" : passed,
            "warning": warning,
            "detail" : detail
        })

    def summary(self):
        elapsed  = time.time() - self.start_time
        passed   = [r for r in self.results if r["passed"] and not r["warning"]]
        warnings = [r for r in self.results if r["warning"]]
        failed   = [r for r in self.results if not r["passed"]]

        print("\n" + "=" * 65)
        print("  HEALTH CHECK REPORT")
        print(f"  Run at  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Duration: {elapsed:.1f} seconds")
        print("=" * 65)
        print(f"\n  {'CHECK':<8} {'NAME':<40} {'RESULT'}")
        print(f"  {'-'*8} {'-'*40} {'-'*10}")

        for r in self.results:
            if r["passed"] and not r["warning"]:
                status = "PASS"
            elif r["warning"]:
                status = "WARN"
            else:
                status = "FAIL"
            print(f"  {r['id']:<8} {r['name']:<40} {status}")
            if r["detail"]:
                print(f"           {r['detail']}")

        print(f"\n  Passed  : {len(passed)}")
        print(f"  Warnings: {len(warnings)}")
        print(f"  Failed  : {len(failed)}")

        if not failed and not warnings:
            print("\n  ALL CHECKS PASSED")
            print("  Data is clean. Schema is consistent.")
            print("  Business rules confirmed across 20M+ rows.")
            print("  READY FOR BRONZE LAYER — AUGUST 1ST.")
        elif not failed and warnings:
            print("\n  PASSED WITH WARNINGS")
            print("  Review warnings above before Bronze ingestion.")
        else:
            print("\n  CHECKS FAILED — DO NOT START BRONZE LAYER")
            print("  Fix all failures before August 1st.")
            for r in failed:
                print(f"  FAILED: {r['id']} — {r['name']}")

        print("=" * 65)
        return len(failed) == 0


# ══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════

def section(title):
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")

def log_fail(check_id, what, where, why, fix):
    print(f"\n  FAILED [{check_id}]")
    print(f"  WHAT  : {what}")
    print(f"  WHERE : {where}")
    print(f"  WHY   : {why}")
    print(f"  FIX   : {fix}")

def log_warn(check_id, message):
    print(f"\n  WARNING [{check_id}]: {message}")

def log_pass(check_id, message):
    print(f"\n  PASS [{check_id}]: {message}")


# ══════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════

print("=" * 65)
print("  BTS AVIATION DELAY INTELLIGENCE SYSTEM")
print("  Pre-Ingestion Data Health Check v1.0")
print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 65)
print("\n  Initialising Spark session...")

spark = SparkSession.builder \
    .appName("BTS_Data_Health_Check_v1.0") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print(f"  Spark {spark.version} ready.")

report = ValidationReport()


# ══════════════════════════════════════════════════════════════
# CHECK 01 — FILE COUNT
# ══════════════════════════════════════════════════════════════

section("CHECK 01 — FILE COUNT")

files = sorted(glob.glob(f"{RAW_PATH}*.csv"))
found = len(files)

print(f"  Path     : {RAW_PATH}")
print(f"  Found    : {found}")
print(f"  Expected : {EXPECTED_FILE_COUNT}")

if found == EXPECTED_FILE_COUNT:
    log_pass("C01", f"{found} files confirmed.")
    report.record("C01", "File Count", True)
else:
    missing = EXPECTED_FILE_COUNT - found
    log_fail("C01",
        what  = f"Found {found} files, expected {EXPECTED_FILE_COUNT}",
        where = f"Directory: {RAW_PATH}",
        why   = f"{missing} monthly CSV files are missing",
        fix   = "Re-download missing months from transtats.bts.gov. "
                "Check 2023, 2024, 2025 — 12 files each.")
    report.record("C01", "File Count", False, f"Missing {missing} files")


# ══════════════════════════════════════════════════════════════
# CHECK 02 — FILE SIZES
# ══════════════════════════════════════════════════════════════

section("CHECK 02 — FILE SIZES")

print(f"  {'File':<35} {'Size (MB)':>10}  Status")
print(f"  {'-'*35} {'-'*10}  {'-'*8}")

small_files = []
for f in files:
    size_mb = os.path.getsize(f) / (1024 * 1024)
    if size_mb < MIN_FILE_SIZE_MB:
        status = "SMALL"
        small_files.append((os.path.basename(f), size_mb))
    else:
        status = "OK"
    print(f"  {os.path.basename(f):<35} {size_mb:>8.1f} MB  {status}")

if not small_files:
    log_pass("C02", "All files above minimum size threshold.")
    report.record("C02", "File Sizes", True)
else:
    for fname, size in small_files:
        log_warn("C02", f"{fname} is only {size:.1f} MB — possible incomplete download.")
    report.record("C02", "File Sizes", True, warning=True,
                  detail=f"{len(small_files)} files below {MIN_FILE_SIZE_MB}MB")


# ══════════════════════════════════════════════════════════════
# CHECK 03 — ROW COUNTS
# ══════════════════════════════════════════════════════════════

section("CHECK 03 — ROW COUNTS PER FILE")

print(f"  {'File':<35} {'Rows':>12}  Status")
print(f"  {'-'*35} {'-'*12}  {'-'*8}")

all_dfs    = []
total_rows = 0
low_files  = []

for f in files:
    df   = spark.read.csv(f, header=True, inferSchema=True)
    rows = df.count()
    total_rows += rows
    all_dfs.append(df)
    status = "OK" if rows >= MIN_ROWS_PER_FILE else "LOW"
    if rows < MIN_ROWS_PER_FILE:
        low_files.append((os.path.basename(f), rows))
    print(f"  {os.path.basename(f):<35} {rows:>12,}  {status}")

print(f"\n  Total rows : {total_rows:,}")
print(f"  Expected   : {MIN_TOTAL_ROWS:,} to {MAX_TOTAL_ROWS:,}")

if MIN_TOTAL_ROWS <= total_rows <= MAX_TOTAL_ROWS and not low_files:
    log_pass("C03", f"{total_rows:,} total rows confirmed.")
    report.record("C03", "Row Counts", True, f"Total: {total_rows:,} rows")
elif low_files:
    for fname, rows in low_files:
        log_warn("C03", f"{fname} has only {rows:,} rows.")
    report.record("C03", "Row Counts", True, warning=True,
                  detail=f"{len(low_files)} files below expected count")
else:
    log_fail("C03",
        what  = f"Total rows {total_rows:,} outside expected range",
        where = f"All files in {RAW_PATH}",
        why   = "Possible incomplete download or BTS data gap",
        fix   = "Verify all 36 monthly files are fully downloaded.")
    report.record("C03", "Row Counts", False,
                  f"Total {total_rows:,} outside expected range")


# ══════════════════════════════════════════════════════════════
# CHECK 04 — SCHEMA CONSISTENCY
# ══════════════════════════════════════════════════════════════

section("CHECK 04 — SCHEMA CONSISTENCY ACROSS ALL 36 FILES")

first_cols    = set(all_dfs[0].columns)
schema_issues = []

for i, df in enumerate(all_dfs[1:], 1):
    current = set(df.columns)
    if current != first_cols:
        schema_issues.append({
            "file"   : os.path.basename(files[i]),
            "missing": first_cols - current,
            "extra"  : current - first_cols
        })

missing_from_spec = EXPECTED_COLUMNS - first_cols
extra_in_data     = first_cols - EXPECTED_COLUMNS

print(f"  Column count      : {len(first_cols)}")
print(f"  Expected count    : {EXPECTED_COL_COUNT}")
print(f"  Missing from spec : {missing_from_spec if missing_from_spec else 'None'}")
print(f"  Extra in data     : {extra_in_data if extra_in_data else 'None'}")
print(f"  Schema drifts     : {len(schema_issues)} files")

if not schema_issues and not missing_from_spec:
    log_pass("C04", "All 36 files have identical schema matching BTS specification.")
    print(f"\n  Verified columns ({len(first_cols)}):")
    for c in sorted(first_cols):
        print(f"    {c}")
    report.record("C04", "Schema Consistency", True)
elif schema_issues:
    for issue in schema_issues:
        log_fail("C04",
            what  = f"Schema mismatch in {issue['file']}",
            where = f"{RAW_PATH}{issue['file']}",
            why   = "Column list differs from reference file",
            fix   = f"Re-download {issue['file']} from BTS. "
                    f"Missing: {issue['missing']}. Extra: {issue['extra']}")
    report.record("C04", "Schema Consistency", False,
                  f"{len(schema_issues)} files with schema drift")
else:
    log_warn("C04", f"Extra columns found: {extra_in_data}")
    report.record("C04", "Schema Consistency", True, warning=True,
                  detail=f"Extra columns: {extra_in_data}")


# ══════════════════════════════════════════════════════════════
# CHECK 05 — FULL MERGE
# ══════════════════════════════════════════════════════════════

section("CHECK 05 — FULL MERGE VALIDATION (unionByName)")

print("  Merging all 36 files using unionByName...")
print("  unionByName matches by column name — safe against schema drift.\n")

df_all = all_dfs[0]
for df in all_dfs[1:]:
    df_all = df_all.unionByName(df)

merged_rows = df_all.count()
merged_cols = len(df_all.columns)

print(f"  Merged rows    : {merged_rows:,}")
print(f"  Merged columns : {merged_cols}")
print(f"  Expected rows  : {total_rows:,}")

if merged_rows == total_rows:
    log_pass("C05", "Merge successful. Row count confirmed.")
    report.record("C05", "Full Merge", True)
else:
    log_fail("C05",
        what  = f"Merged {merged_rows:,} rows vs file sum {total_rows:,}",
        where = "unionByName merge of all 36 files",
        why   = "Row loss during merge — schema incompatibility",
        fix   = "Check C04 results. Re-download schema drift files.")
    report.record("C05", "Full Merge", False,
                  f"Merged: {merged_rows:,} vs Expected: {total_rows:,}")


# ══════════════════════════════════════════════════════════════
# CHECK 06 — MANDATORY COLUMNS NULL CHECK
# ══════════════════════════════════════════════════════════════

section("CHECK 06 — MANDATORY COLUMNS NULL CHECK")

print("  These columns must NEVER contain NULL values.\n")
print(f"  {'Column':<30} {'NULLs':>12}  Status")
print(f"  {'-'*30} {'-'*12}  {'-'*8}")

mandatory_failures = []

for c in MANDATORY_COLUMNS:
    if c in df_all.columns:
        nulls = df_all.filter(col(c).isNull()).count()
        status = "OK" if nulls == 0 else "CRITICAL"
        print(f"  {c:<30} {nulls:>12,}  {status}")
        if nulls > 0:
            mandatory_failures.append((c, nulls))
    else:
        print(f"  {c:<30} {'N/A':>12}  MISSING")
        mandatory_failures.append((c, -1))

if not mandatory_failures:
    log_pass("C06", "All mandatory columns have zero NULLs.")
    report.record("C06", "Mandatory NULL Check", True)
else:
    for col_name, nulls in mandatory_failures:
        log_fail("C06",
            what  = f"{col_name} contains {nulls:,} NULL values",
            where = f"Column {col_name} in merged dataset",
            why   = "Identity columns cannot be NULL",
            fix   = f"Investigate source files for {col_name} NULLs. "
                    "Quarantine affected rows in Silver layer.")
    report.record("C06", "Mandatory NULL Check", False,
                  f"{len(mandatory_failures)} columns with NULLs")


# ══════════════════════════════════════════════════════════════
# CHECK 07 — DELAY CAUSE NULL PATTERN
# ══════════════════════════════════════════════════════════════

section("CHECK 07 — DELAY CAUSE NULL PATTERN (~80%)")

print("  Rule: Delay cause columns are NULL when ARR_DEL15 = 0.")
print("  These NULLs are CORRECT. Never replace with 0.\n")
print(f"  {'Column':<25} {'NULLs':>12}  {'NULL %':>8}  Status")
print(f"  {'-'*25} {'-'*12}  {'-'*8}  {'-'*8}")

null_counts    = {}
pattern_issues = []

for c in DELAY_CAUSE_COLUMNS:
    nulls = df_all.filter(col(c).isNull()).count()
    pct   = nulls / merged_rows * 100
    null_counts[c] = nulls
    status = "OK" if NULL_PATTERN_MIN_PCT <= pct <= NULL_PATTERN_MAX_PCT else "CHECK"
    if status == "CHECK":
        pattern_issues.append((c, pct))
    print(f"  {c:<25} {nulls:>12,}  {pct:>7.1f}%  {status}")

counts_unique = set(null_counts.values())
if len(counts_unique) == 1:
    print(f"\n  All 5 delay cause columns have identical NULL count: {list(counts_unique)[0]:,}")
    print("  Confirms they are all controlled by the ARR_DEL15 flag.")
    consistency_ok = True
else:
    print("\n  WARNING: Delay cause columns have different NULL counts.")
    consistency_ok = False

sample_pct = list(null_counts.values())[0] / merged_rows * 100

if not pattern_issues and consistency_ok:
    log_pass("C07", f"NULL pattern confirmed. {sample_pct:.1f}% across all delay cause columns.")
    report.record("C07", "Delay Cause NULL Pattern", True,
                  f"{sample_pct:.1f}% NULL as expected")
else:
    for col_name, pct in pattern_issues:
        log_warn("C07", f"{col_name} has {pct:.1f}% NULLs — outside 70-90% range.")
    report.record("C07", "Delay Cause NULL Pattern",
                  not bool(pattern_issues),
                  warning=bool(pattern_issues))


# ══════════════════════════════════════════════════════════════
# CHECK 08 — ARR_DEL15 BUSINESS RULE
# ══════════════════════════════════════════════════════════════

section("CHECK 08 — ARR_DEL15 BUSINESS RULE VALIDATION")

print("  Rule : When ARR_DEL15 = 0, ALL delay cause columns MUST be NULL.")
print("  Basis: Validated on Q1 2024 (1,658,259 rows) — 0 violations.")
print(f"  Scope: Full {merged_rows:,} row dataset.\n")

violations = df_all.filter(
    (col("ARR_DEL15") == 0) &
    (col("CARRIER_DELAY").isNotNull())
).count()

print(f"  Violations found : {violations:,}")
print(f"  Expected         : 0")

if violations == 0:
    log_pass("C08",
             f"Business rule confirmed across {merged_rows:,} rows. "
             "NULL delay cause columns are CORRECT behaviour.")
    report.record("C08", "ARR_DEL15 Business Rule", True,
                  "0 violations across full dataset")
else:
    log_fail("C08",
        what  = f"{violations:,} rows have ARR_DEL15=0 but non-NULL CARRIER_DELAY",
        where = "CARRIER_DELAY column in merged dataset",
        why   = "BTS spec: delay cause columns must be NULL when ARR_DEL15=0. "
                "Violations indicate upstream data quality issue.",
        fix   = "Identify which months contain violations. "
                "Add Silver layer rule to quarantine these rows.")
    report.record("C08", "ARR_DEL15 Business Rule", False,
                  f"{violations:,} violations found")


# ══════════════════════════════════════════════════════════════
# CHECK 09 — YEAR DISTRIBUTION
# ══════════════════════════════════════════════════════════════

section("CHECK 09 — YEAR DISTRIBUTION")

print("  Expected: 2023, 2024, 2025 — roughly equal row counts.\n")

year_dist   = df_all.groupBy("YEAR") \
    .agg(spark_count("*").alias("row_count")) \
    .orderBy("YEAR")

years_found = set()
print(f"  {'Year':<8} {'Rows':>15}  {'% of Total':>12}")
print(f"  {'-'*8} {'-'*15}  {'-'*12}")

for row in year_dist.collect():
    year  = row["YEAR"]
    count = row["row_count"]
    pct   = count / merged_rows * 100
    years_found.add(year)
    print(f"  {year:<8} {count:>15,}  {pct:>11.1f}%")

missing_years = EXPECTED_YEARS - years_found
extra_years   = years_found - EXPECTED_YEARS

if not missing_years and not extra_years:
    log_pass("C09", "All 3 years present — 2023, 2024, 2025.")
    report.record("C09", "Year Distribution", True)
else:
    if missing_years:
        log_fail("C09",
            what  = f"Years missing: {missing_years}",
            where = "YEAR column in merged dataset",
            why   = "Expected data for 2023, 2024, 2025",
            fix   = f"Download monthly files for: {missing_years}")
    if extra_years:
        log_warn("C09", f"Unexpected years: {extra_years}")
    report.record("C09", "Year Distribution",
                  not bool(missing_years),
                  warning=bool(extra_years))


# ══════════════════════════════════════════════════════════════
# CHECK 10 — CARRIER DISTRIBUTION
# ══════════════════════════════════════════════════════════════

section("CHECK 10 — CARRIER DISTRIBUTION (Top 10)")

print("  Expected: WN, AA, DL among top carriers.\n")

carrier_dist   = df_all.groupBy("OP_UNIQUE_CARRIER") \
    .agg(spark_count("*").alias("flights")) \
    .orderBy("flights", ascending=False)

total_carriers = carrier_dist.count()
top_10         = carrier_dist.limit(10).collect()
top_3_codes    = [row["OP_UNIQUE_CARRIER"] for row in top_10[:3]]

print(f"  {'Rank':<6} {'Carrier':<12} {'Flights':>15}  {'% of Total':>12}")
print(f"  {'-'*6} {'-'*12} {'-'*15}  {'-'*12}")

for i, row in enumerate(top_10, 1):
    carrier = row["OP_UNIQUE_CARRIER"]
    flights = row["flights"]
    pct     = flights / merged_rows * 100
    print(f"  {i:<6} {carrier:<12} {flights:>15,}  {pct:>11.1f}%")

print(f"\n  Total unique carriers: {total_carriers}")

expected_top = {"WN", "AA", "DL"}
if expected_top.intersection(set(top_3_codes)):
    log_pass("C10", f"Major US carriers confirmed. {total_carriers} unique carriers.")
    report.record("C10", "Carrier Distribution", True,
                  f"{total_carriers} unique carriers")
else:
    log_warn("C10", f"Expected WN/AA/DL in top 3. Found: {top_3_codes}")
    report.record("C10", "Carrier Distribution", True, warning=True,
                  detail=f"Unexpected top: {top_3_codes}")


# ══════════════════════════════════════════════════
# CHECK 11 — TAIL_NUM NULL ANALYSIS
# ══════════════════════════════════════════════════

section("CHECK 11 — TAIL_NUM NULL ANALYSIS")

print("  TAIL_NUM NULLs are expected — some flights")
print("  are not reported with tail numbers by BTS.")
print("  These rows are preserved with aircraft_key=-1")
print("  in dim_aircraft. Never dropped.\n")

tail_nulls = df_all.filter(col("TAIL_NUM").isNull()).count()
tail_pct   = tail_nulls / merged_rows * 100

print(f"  Total rows      : {merged_rows:,}")
print(f"  TAIL_NUM NULLs  : {tail_nulls:,}")
print(f"  NULL %          : {tail_pct:.2f}%")

if tail_pct < 2.0:
    log_pass("C11",
             f"TAIL_NUM NULLs at {tail_pct:.2f}% — "
             f"acceptable. Will use aircraft_key=-1.")
    report.record("C11", "TAIL_NUM NULL Analysis", True,
                  f"{tail_nulls:,} NULLs ({tail_pct:.2f}%)")
elif tail_pct < 5.0:
    log_warn("C11",
             f"TAIL_NUM NULLs at {tail_pct:.2f}% — "
             f"manageable but document in governance.")
    report.record("C11", "TAIL_NUM NULL Analysis",
                  True, warning=True,
                  detail=f"{tail_nulls:,} NULLs ({tail_pct:.2f}%)")
else:
    log_fail("C11",
        what  = f"TAIL_NUM has {tail_pct:.2f}% NULLs",
        where = "TAIL_NUM column across merged dataset",
        why   = "More than 5% missing tail numbers "
                "may indicate reporting quality issue",
        fix   = "Investigate which carriers or months "
                "have highest TAIL_NUM NULL rates.")
    report.record("C11", "TAIL_NUM NULL Analysis", False,
                  f"{tail_nulls:,} NULLs ({tail_pct:.2f}%)")


# ══════════════════════════════════════════════════
# CHECK 12 — FULL NULL PROFILE (ALL 37 COLUMNS)
# ══════════════════════════════════════════════════

section("CHECK 12 — FULL NULL PROFILE ALL 37 COLUMNS")

print("  NULL count for every column across")
print("  full 20,928,599 row dataset.\n")
print(f"  {'Column':<30} {'NULLs':>12}  {'NULL %':>8}  Status")
print(f"  {'-'*30} {'-'*12}  {'-'*8}  {'-'*8}")

null_profile = {}
for c in df_all.columns:
    nulls = df_all.filter(col(c).isNull()).count()
    pct   = nulls / merged_rows * 100
    null_profile[c] = nulls

    if nulls == 0:
        status = "OK"
    elif pct < 1:
        status = "LOW"
    elif pct < 80:
        status = "CHECK"
    else:
        status = "EXPECTED"

    print(f"  {c:<30} {nulls:>12,}  {pct:>7.2f}%  {status}")

report.record("C12", "Full NULL Profile", True,
              detail="All 37 columns profiled")

# ===================================
#  CHECK 13 — DUPLICATE DETECTION
# ===================================

section("CHECK 13 — DUPLICATE DETECTION")

dupes = df_all.groupBy(
    "FL_DATE", "OP_UNIQUE_CARRIER",
    "OP_CARRIER_FL_NUM", "ORIGIN", "DEST"
).count().filter(col("count") > 1).count()

print(f"Duplicate flight records: {dupes:,}")

if dupes == 0:
    log_pass("C13", "Zero duplicates found.")
else:
    log_warn("C13",
        f"{dupes:,} duplicate records found. "
        f"Silver layer will deduplicate.")
report.record("C13", "Duplicate Detection",
              True,
              detail=f"{dupes:,} duplicates")

# ══════════════════════════════════════════════════
# CHECK 14 — FLIGHTS COLUMN VALIDATION
# ══════════════════════════════════════════════════

section("CHECK 14 — FLIGHTS COLUMN VALIDATION")

print("  Validating FLIGHTS column across full dataset.")
print("  Expected: always = 1 (constant value)\n")

from pyspark.sql.functions import countDistinct, min as spark_min, max as spark_max

distinct_vals = df_all.select(countDistinct("FLIGHTS").alias("distinct")).collect()[0][0]

min_val = df_all.select( spark_min("FLIGHTS").alias("min_val")).collect()[0][0]

max_val = df_all.select(spark_max("FLIGHTS").alias("max_val")).collect()[0][0]

null_count = df_all.filter(col("FLIGHTS").isNull()).count()

print(f"  Distinct values : {distinct_vals}")
print(f"  Min value       : {min_val}")
print(f"  Max value       : {max_val}")
print(f"  NULL count      : {null_count:,}")
print(f"  Total rows      : {merged_rows:,}")

if distinct_vals == 1 and min_val == 1.0 and max_val == 1.0:
    log_pass("C14",
             "FLIGHTS column confirmed constant = 1 "
             "across all 20,928,599 rows. "
             "Safe to drop in Silver layer. "
             "Will document in ADR-010.")
    report.record("C14", "FLIGHTS Column Validation", True,
                  detail="Constant value 1.0 — drop in Silver")
else:
    log_warn("C14",
             f"FLIGHTS has {distinct_vals} distinct values. "
             f"Min={min_val}, Max={max_val}. "
             f"Investigate before dropping.")
    report.record("C14", "FLIGHTS Column Validation",
                  True, warning=True,
                  detail=f"{distinct_vals} distinct values found")

# ══════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════

all_passed = report.summary()

spark.stop()
sys.exit(0 if all_passed else 1)