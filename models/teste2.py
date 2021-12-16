
from pyspark.sql import SparkSession
import datetime
import time
import os
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

print("========= INICIO DO PROCESSO =========")

SPARKMASTER_IP = 'spark://172.30.0.4:7077'

spark = SparkSession.builder.master(SPARKMASTER_IP).appName('teste-Mercado')\
   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")\
   .config("spark.driver.host", "172.30.0.1")\
   .getOrCreate()

print("========= CONEXÃO ABERTA =========")
# df = spark.read.format("csv").option("sep",";").option("header", "true").load("file:///.//data//mercadoDM_BARUEL_20211101.csv")

# df = spark.read.format("csv").option("sep",";").option("header", "true").load("mercadoDM_BARUEL_20211101.csv")
# df.show()

data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1),
    ("Caio","Mary","Brown","","F",-1),
    ("Mauro","Mary","Brown","","F",-1),
    ("Reinaldo","Mary","Brown","","F",-1),
    ("João","Mary","Brown","","F",-1),
    ("Marcelo","Mary","Brown","","F",-1)
  ]

schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])

df = spark.createDataFrame(data=data2,schema=schema)
# df.printSchema()

df.createOrReplaceTempView("teste")

df.show()