from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from deep_translator import GoogleTranslator

credentials_location = './gcs.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    .set("spark.driver.extraClassPath", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.executor.extraClassPath", "./lib/gcs-connector-hadoop3-2.2.5.jar")

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df_formacode = spark.read\
                         .option("header", "true") \
                         .csv("./data/formacode_description.csv")

@F.udf(returnType=T.StringType())
def translate(input):
    return GoogleTranslator(source='auto', target='en').translate(input)

df_formacode.withColumn('translation', translate(F.col('Description')))

df_formacode.coalesce(1).write.parquet('gs://jugnu-france-course-enrollments/formacode_converted', mode='overwrite')