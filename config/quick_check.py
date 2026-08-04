"""
Quick Check — Run specific validations without
full health check. Use for rapid investigation.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import max as spark_max

os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

spark = SparkSession.builder \
    .appName("BTS_Quick_Check") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Load all 36 files once ────────────────────────
import glob
files = sorted(glob.glob("data/raw/*.csv"))
df = spark.read.csv(files[0], header=True, inferSchema=True)
for f in files[1:]:
    df = df.unionByName(
        spark.read.csv(f, header=True, inferSchema=True)
    )

print(f"Loaded: {df.count():,} rows\n")

# ── CHECK 14 — FLIGHTS COLUMN ─────────────────────
print("=" * 50)
print("CHECK 14 — FLIGHTS COLUMN VALIDATION")
print("=" * 50)

distinct_vals = df.select(
    countDistinct("FLIGHTS").alias("d")
).collect()[0][0]

min_val = df.select(spark_min("FLIGHTS")).collect()[0][0]
max_val = df.select(spark_max("FLIGHTS")).collect()[0][0]
nulls   = df.filter(col("FLIGHTS").isNull()).count()

print(f"Distinct values : {distinct_vals}")
print(f"Min value       : {min_val}")
print(f"Max value       : {max_val}")
print(f"NULL count      : {nulls:,}")

if distinct_vals == 1 and min_val == 1.0:
    print("\nPASS: Constant value 1.0 confirmed.")
    print("Safe to drop in Silver. Document in ADR-010.")
else:
    print("\nWARN: Unexpected values found. Investigate.")

spark.stop()
