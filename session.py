
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
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-21'
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)




data = """
{
	"Name": "Vasu",
	"Mobile": 12345678,
	"Boolean": true,
	"Pets": ["Dog", "cat"],
	"state": ["TamilNadu","Kerala"]
}
  
"""

df = spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()



flatdf = df.selectExpr(

    "Boolean",
    "Mobile",
    "Name",
    "explode(Pets) as Pets",
    "explode(state) as state"
)

flatdf.show()
flatdf.printSchema()

withcolumn= (

    df.withColumn("pets",expr("explode(pets)"))
    .withColumn("state",expr("explode(state)"))

)
withcolumn.show()

print("//////array with struct///////")
data = """
{
  "country" : "US",
  "version" : "0.6",
  "Actors": [
    {
      "name": "Tom Cruise",
      "age": 56,
      "BornAt": "Syracuse, NY",
      "Birthdate": "July 3, 1962",
      "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
      "wife": null,
      "weight": 67.5,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/73.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
                }
    },
    {
      "name": "Robert Downey Jr.",
      "age": 53,
      "BornAt": "New York City, NY",
      "Birthdate": "April 4, 1965",
      "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
      "wife": "Susan Downey",
      "weight": 77.1,
      "hasChildren": true,
      "hasGreyHair": false,
      "picture": {
                    "large": "https://randomuser.me/api/portraits/men/78.jpg",
                    "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
                    "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
                }
    }
  ]
}
"""

df = spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()


explodedf = df.withColumn(  "Actors" ,    expr("explode(Actors)")   )
explodedf.show()
explodedf.printSchema()


seldf = explodedf.select(

    "Actors.Birthdate",
    "Actors.BornAt",
    "Actors.age",
    "Actors.hasChildren",
    "Actors.hasGreyHair",
    "Actors.name",
    "Actors.photo",
    "Actors.picture.large",
    "Actors.picture.medium",
    "Actors.picture.thumbnail",
    "Actors.weight",
    "Actors.wife",
    "country",
    "version"

)
seldf.show()
seldf.printSchema()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ParentChildDF").getOrCreate()

data = [("A", "AA"),
        ("B", "BB"),
        ("C", "CC"),
        ("AA", "AAA"),
        ("BB", "BBB"),
        ("CC", "CCC")]

df = spark.sparkContext.parallelize(data).toDF(["child", "parent"])
df.show()


df1   =    df
df1.show()

df2   =    df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
df2.show()


innerjoin = df1.join (df2, df1["parent"] == df2["child1"],"inner").withColumnRenamed("parent1","Grand Parent").drop("child1")
innerjoin.show()


data = [
    ("hadoop spark",),
    ("spark hadoop",),
    ("hive spark",),
    ("hadoop hive",)
]

from pyspark.sql.functions import *

df = spark.createDataFrame(data, ["value"])

df.show()

explod=df.withColumn("value",expr("explode(split(value,' '))"))
explod.show()

add=explod.groupBy('value').agg(count("value").alias("word count"))
add.show()

data = [
    (1, "1-Jan", "Ordered"),
    (1, "2-Jan", "dispatched"),
    (1, "3-Jan", "dispatched"),
    (1, "4-Jan", "Shipped"),
    (1, "5-Jan", "Shipped"),
    (1, "6-Jan", "Delivered"),
    (2, "1-Jan", "Ordered"),
    (2, "2-Jan", "dispatched"),
    (2, "3-Jan", "shipped")
]
myschema = ["orderid", "statusdate", "status"]
df = spark.createDataFrame(data, schema=myschema)
df.show()

fil=df.rdd.flatMap(lambda x: x).collect()

fil.show()



spark = SparkSession.builder.appName("lef").getOrCreate()
da=[
    (1,),
    (2,),
    (3,)
]
df= spark.createDataFrame(da,["id"])
df.show()

df1=df.alias("df")
df2=df.alias("df")
df1.show()
df2.show()
result = df1.filter(~df1["id"].isin([1, 2, 3]) | df1["id"].isNull())
result.show()

