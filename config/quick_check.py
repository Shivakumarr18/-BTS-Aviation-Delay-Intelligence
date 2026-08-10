import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PATH"]                  = os.environ["PATH"] + r";C:\hadoop\bin"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

spark = SparkSession.builder \
    .appName("BTS_Quick_Check") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Check both failed partitions
for year, month in [(2024, 12), (2025, 5)]:
    print(f"\n{'='*50}")
    print(f"Checking year={year}/month={month}")
    print(f"{'='*50}")

    df = spark.read \
        .option("basePath", "data/bronze/") \
        .parquet(f"data/bronze/YEAR={year}/MONTH={month}/")

    print(f"Row count: {df.count():,}")

    # Check FL_DATE format
    print("\nSample FL_DATE values:")
    df.select("FL_DATE").distinct().show(5, truncate=False)

    # Check date conversion
    nulls = df.withColumn(
        "parsed",
        to_date(
            to_timestamp(col("FL_DATE"), "M/d/yyyy h:mm:ss a")
        )
    ).filter(col("parsed").isNull()).count()