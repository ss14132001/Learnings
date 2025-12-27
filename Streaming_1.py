import os
import urllib.request
import ssl
from inspect import stack

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
project_root = os.getcwd()
os.environ['HADOOP_HOME'] = os.path.join(project_root, "hadoop")
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_462'
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:///C:/kinjar/* pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("Error")

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

print()
from pyspark.sql.functions import *

#kinesisdf = (

    #spark
    #.readStream
    #.format("aws-kinesis")
    #.option("kinesis.streamName","zeyokin")
    #.option("kinesis.endpointUrl","https://kinesis.ap-south-1.amazonaws.com")
    #.option("kinesis.startingposition","TRIM_HORIZON")
   # .load()
  #  .withColumn("data",expr("cast(data as string)"))
 #   .select("data")

#)


#kinesisdf.writeStream.format("console").start().awaitTermination()
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
import os
import urllib.request
import ssl

from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg




from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

from pyspark.sql import functions as F

import time

data = [
    (1, "Alice", 3),
    (2, "Bob", 3),
    (3, "Charlie", 5),
    (4, "David", 5),
    (5, "Evelyn", None),
    (6, "Frank", 3),
]

columns = ["emp_id", "emp_name", "manager_id"]

emp_df = spark.createDataFrame(data, columns)

emp_df.show()

result = (emp_df.alias("e").join(
    emp_df.alias("m"),
    col("e.manager_id") == col("m.emp_id"),
    "left"
))
result.show()
result= result.select(
    col("e.emp_id"),
    col("e.emp_name"),
    col("m.emp_name").alias("manager_name"))

result.show()



attendance = [
    (1, "2025-01-01", "09:15"),
    (1, "2025-01-02", "09:10"),
    (1, "2025-01-03", "09:05"),
    (1, "2025-01-04", "08:55"),
    (1, "2025-01-05", "09:20"),
    (2, "2025-01-01", "08:50"),
    (2, "2025-01-02", "09:12"),
]

att_df = spark.createDataFrame(attendance, ["emp_id", "att_date", "arrival_time"]) \
    .withColumn("att_date", F.to_date("att_date")) \
    .withColumn("arrival_time", F.to_timestamp("arrival_time", "HH:mm"))

w_att = Window.partitionBy("emp_id").orderBy("att_date")

att_late = (
    att_df
    .withColumn("is_late", F.when(F.col("arrival_time") > F.lit("09:00:00"), 1).otherwise(0))
    .withColumn("rn", F.row_number().over(w_att))
    .withColumn("grp_key", F.expr("date_sub(att_date, rn)"))
    .filter(F.col("is_late") == 1)
)

streaks = (
    att_late
    .groupBy("emp_id", "grp_key")
    .agg(F.count("*").alias("streak_len"))
    .groupBy("emp_id")
    .agg(F.max("streak_len").alias("max_late_streak"))
)

streaks.show()

data = [
    ("Delhi", "Bangalore", 6000),
    ("Bangalore", "Delhi", 6000),
    ("Chennai", "Hyderabad", 4000),
    ("Hyderabad", "Chennai", 5000),
    ("Goa", "Mumbai", 6000),
    ("Mumbai", "Goa", 7000),
    ("Pune", "Nagpur", 4500),
    ("Nagpur", "Pune", 4500)
]

df = spark.createDataFrame(data, ["Source", "Destination", "Price"])
df.show()
# Self join for forward & reverse route
joined = (
    df.alias("f") \
        .join(df.alias("r"),
              (F.col("f.Source") == F.col("r.Destination")) &
              (F.col("f.Destination") == F.col("r.Source")),
              "inner"))
joined.show()
joined = joined.select(
    F.col("f.Source").alias("Source"),
    F.col("f.Destination").alias("Destination"),
    F.col("f.Price").alias("ForwardPrice"),
    F.col("r.Price").alias("ReversePrice"))

joined.show()
# Filter only where price differs
result = joined.filter(F.col("ForwardPrice") != F.col("ReversePrice"))
result.show()
# Avoid duplicate pairs by ordering source alphabetically
result = result.filter(F.col("Source") < F.col("Destination"))

result.show()

