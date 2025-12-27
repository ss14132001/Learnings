from pyspark.sql.functions import count
from pyspark.sql.window import Window
import os
import urllib.request
import ssl
import json
from pyspark.sql.functions import count
from pyspark.sql.window import Window
import os
import urllib.request
import ssl
import json
import os

from setuptools.command.alias import alias
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import count as f_count
from pyspark.sql.functions import row_number

import os

from setuptools.command.alias import alias
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import count as f_count
from pyspark.sql.functions import row_number


os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

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
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r'C:\Program Files\Amazon Corretto\jdk1.8.0_462'
######################🔴🔴🔴################################
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when, expr
from pyspark.sql.window import Window

spark=SparkSession.builder.appName("time").getOrCreate()

data=[(1111, "2021-01-15", 10),
      (1111, "2021-01-16", 15),
      (1111, "2021-01-17", 30),
      (1112, "2021-01-15", 10),
      (1112, "2021-01-15", 20),
      (1112, "2021-01-15", 30)]
schema=(["sensorid","timestamp","values"])
df=spark.createDataFrame(data,schema=schema)
df.show()

windp=Window.partitionBy("sensorid").orderBy("values")

rnk=df.withColumn("ne",row_number().over(windp)).filter(col("ne")<=2)
rnk.show()

final=rnk.withColumn(
    "values",
    expr("""
CASE
WHEN (sensorid = 1111 AND timestamp = '2021-01-15') THEN values - 5
WHEN (sensorid = 1112 AND ne = 2) THEN values - 10
ELSE values
END


   """)).drop("ne")
final.show()

import os
import urllib.request
import ssl
from pyspark.sql import SparkSession

urldata = urllib.request.urlopen("https://randomuser.me/api/0.8/?results=5",context=ssl._create_unverified_context()).read().decode('utf-8')

print(urldata)

urldf = spark.read.json(spark.sparkContext.parallelize([urldata]))
urldf.show()

flat= urldf.withColumn("results",expr("explode(results)"))
flat.show()
flat.printSchema()

rd=flat.rdd
print (rd.collect())

source_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (3, "C"),
    (4, "D")
],1)

target_rdd = spark.sparkContext.parallelize([
    (1, "A"),
    (2, "B"),
    (4, "X"),
    (5, "F")
],2)

# Convert RDDs to DataFrames using toDF()
df1 = source_rdd.toDF(["id", "name"])
df2 = target_rdd.toDF(["id", "name1"])

# Show the DataFrames
df1.show()
df2.show()

joindf  = df1.join(df2, "id" , "full")
joindf.show()



