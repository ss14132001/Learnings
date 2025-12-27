

import os
import urllib.request
import ssl

from setuptools.command.alias import alias

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


##################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################

data = [
    (1, 'Spark'),
    (1, 'Scala'),
    (1, 'Hive'),
    (2, 'Scala'),
    (3, 'Spark'),
    (3, 'Scala')
]
columns = ['id', 'subject']
df = spark.createDataFrame(data, columns)
df.show()

aggdf=df.groupby("id").agg(

                  collect_list("subject").alias("subject")
)
aggdf.show()


data = [(1, "Mark Ray", "AB"),
        (2, "Peter Smith", "CD"),
        (1, "Mark Ray", "EF"),
        (2, "Peter Smith", "GH"),
        (2, "Peter Smith", "CD"),
        (3, "Kate", "IJ")]
myschema = ["custid", "custname", "address"]
gf = spark.createDataFrame(data, schema=myschema)
df.show()


aggf = gf.groupBy("custid","custname").agg(

    collect_set("address").alias("address")


).orderBy("custid")

aggf.show()


data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()
cut = spark.createDataFrame(data4, ["cid", "name"])
cut.show()


data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()
prd = spark.createDataFrame(data3, ["pid", "product"])
prd.show()

print ("//joins//")

innerjoin=cust.join(prod,["id"],"inner").orderBy("id")
innerjoin.show()

leftjoin=cust.join(prod,["id"],"left").orderBy("id")
leftjoin.show()
rightjoin=cust.join(prod,["id"],"right").orderBy("id")
rightjoin.show()
fulljoin=cust.join(prod,["id"],"full").orderBy("id")
fulljoin.show()
print("/////Joins with different column///////// ")
cut.join(prd,cut["cid"]==prd["pid"],"inner").drop("pid").orderBy("cid").show()
cut.join(prd,cut["cid"]==prd["pid"],"left") .drop("pid").orderBy("cid").show()
cut.join(prd,cut["cid"]==prd["pid"], "right").drop("cid").orderBy("pid").show()
cut.join(prd,cut["cid"]==prd["pid"], "full").orderBy("cid","pid").show()
print("//////list filtering//////")
idlist  = prod.select("id").rdd.flatMap(lambda x : x).collect()
print(idlist)
fildf = cust.filter( ~cust.id.isin(idlist))
fildf.show()
print("/////left anti join ///////")

joindf = cust.join(prod, ["id"] , "leftanti").orderBy("id")
joindf.show()

print("////cross join //////")

cust.crossJoin(prod).show()

print("///// second highest salary using windowing////")

from pyspark.sql.window import Window
from pyspark.sql.functions import *

data = [

    ("customs", 1000),
    ("customs", 700),
    ("customs", 500),
    ("Defence", 400),
    ("Defence", 200),
    ("Navy", 500),
    ("Navy", 200)
]

columns = ["department", "salary"]

df = spark.createDataFrame(data, columns)

df.show()

denserank=df.withColumn("rnk", dense_rank().over(Window.partitionBy("department").orderBy(col("salary").desc())))
denserank.show()

finaldf =  denserank.filter("rnk = 2")
finaldf.show()
finaldf.drop("rnk").show()

print("//////// spliting the data scenario//////")


data = [
    (1, "Henry", "henry12@gmail.com"),
    (2, "Smith", "smith@yahoo.com"),
    (3, "Martin", "martin221@hotmail.com")
]
columns = ["id", "name", "email"]

df1 = spark.createDataFrame(data, columns)
df1.show()

spt=df1.withColumn("domain",expr("split(email,'@')[1]")).drop("email")
spt.show()


print("/////// aggregation with collect set Scenario/////////")
data = [('2020-05-30','Headphone'),('2020-06-01','Pencil'),('2020-06-02','Mask'),('2020-05-30','Basketball'),('2020-06-01','Book'),('2020-06-02','Mask'),('2020-05-30','T-Shirt')]
columns = ["sell_date",'product']

df = spark.createDataFrame(data,schema=columns)
df.show()

agt=df.groupBy("sell_date").agg(collect_set("product").alias("products"),size(collect_set("product")).alias("count of selldate"))
agt.show()


data = [
    ('A', 'D', 'D'),
    ('B', 'A', 'A'),
    ('A', 'D', 'A')
]

df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")

df.show()

a=df.select(col("TeamA").alias("team"))
a.show()
b=df.select(col("TeamB").alias("Teamname"))
b.show()
union=a.union(b).dropDuplicates()
union.show()

print("Scenario 3")

data1 = [
    (1, "abc", 31, "abc@gmail.com"),
    (2, "def", 23, "defyahoo.com"),
    (3, "xyz", 26, "xyz@gmail.com"),
    (4, "qwe", 34, "qwegmail.com"),
    (5, "iop", 24, "iop@gmail.com")
]
myschema1 = ["id", "name", "age", "email"]
df3 = spark.createDataFrame(data1, schema=myschema1)
df3.show()

data2 = [
    (11, "jkl", 22, "abc@gmail.com", 1000),
    (12, "vbn", 33, "vbn@yahoo.com", 3000),
    (13, "wer", 27, "wer", 2000),
    (14, "zxc", 30, "zxc.com", 2000),
    (15, "lkj", 29, "lkj@outlook.com", 2000)
]
myschema2 = ["id", "name", "age", "email", "salary"]
df2 = spark.createDataFrame(data2, schema=myschema2)
df2.show()

df3.filter("email=@gmail.com").show()





data = """
     {
    "id": 1,
    "trainer": "sai",
    "zeyoAddress":{ 
    "user":{
        "TemporaryAddress": "chennai",
        "PermanentAddress": "Hyderabad"
    }
    }
}
"""

df = spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()

flatten=df.select(
    "id",
    "trainer",
    "zeyoAddress.user.TemporaryAddress",
    "zeyoAddress.user.PermanentAddress"
)
flatten.show()
flatten.printSchema()


data = """
{
	"id": "000",
	"type": "donut",
	"name": "Non cream",
	"image": {
		"url": "images/0001.jpg",
		"width": 200,
		"height": 200
	},
	"thumbnail": {
		"url": "images/thumbnails/0001.jpg",
		"width": 33,
		"height": 33
	}
}
"""

df = spark.read.json(sc.parallelize([data]))
df.show()
df.printSchema()


flattendf = df.selectExpr(
    "id",
    "image.height as image_height",
    "image.url as image_url",
    "image.width as image_width",
    "name",
    "thumbnail.height as thumbnail_height",
    "thumbnail.url as thumbnail_url",
    "thumbnail.width as thumbnail_width",
    "type"
)

flattendf.show()
flattendf.printSchema()




