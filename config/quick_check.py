import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

from pyspark.sql import SparkSession

os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv\Scripts\python.exe"
os.environ["PATH"]                  = os.environ["PATH"] + r";C:\hadoop\bin"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

# ── Spark Session ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("BTS_Silver_To_MySQL") \
    .master("local[*]") \
    .config("spark.jars",
            r"C:\-BTS-Aviation-Delay-Intelligence\mysql-connector-j-26.7.0.jar") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Read Silver ───────────────────────────────────────────────
print("Reading Silver layer...")
df_silver = spark.read.parquet("data/silver/")
print(f"Silver rows : {df_silver.count():,}")
print(f"Silver cols : {len(df_silver.columns)}")

# ── Write to MySQL ────────────────────────────────────────────
print("\nWriting to MySQL...")
print("This will take 10-20 minutes for 20.9M rows...")

MYSQL_URL  = "jdbc:mysql://localhost:3306/bts_practice"
MYSQL_USER = "root"
MYSQL_PASS = "Shiva@3003"   

df_silver.write \
    .format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "fact_delays_silver") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASS) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("batchsize", "10000") \
    .option("numPartitions", "8") \
    .mode("overwrite") \
    .save()

print("\nDone. Verifying row count in MySQL...")

df_verify = spark.read \
    .format("jdbc") \
    .option("url", MYSQL_URL) \
    .option("dbtable", "fact_delays_silver") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASS) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

print(f"MySQL rows  : {df_verify.count():,}")
print(f"Match       : {'YES' if df_verify.count() == df_silver.count() else 'NO'}")

spark.stop()