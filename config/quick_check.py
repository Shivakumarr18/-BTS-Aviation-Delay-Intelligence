import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

os.environ["JAVA_HOME"]             = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"]           = r"C:\hadoop"
os.environ["PYSPARK_PYTHON"]        = r"C:\-BTS-Aviation-Delay-Intelligence\.venv311\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\-BTS-Aviation-Delay-Intelligence\.venv311\Scripts\python.exe"
os.environ["PATH"]                  = os.environ["PATH"] + r";C:\hadoop\bin"
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BTS_Silver_Check") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/silver/")
print(f"Silver rows : {df.count():,}")
print(f"Silver cols : {len(df.columns)}")
spark.stop()
