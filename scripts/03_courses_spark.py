import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql import types
import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_file = args.input
output_file = args.output

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

df_courses = spark.read.option("header", "true").parquet(input_file)

df_courses = df_courses\
    .withColumn("code_formacode_1", df_courses["code_formacode_1"].cast(types.IntegerType()))\
    .withColumn("code_formacode_2", df_courses["code_formacode_2"].cast(types.IntegerType()))\
    .withColumn("code_formacode_3", df_courses["code_formacode_3"].cast(types.IntegerType()))\
    .withColumn("code_formacode_4", df_courses["code_formacode_4"].cast(types.IntegerType()))\
    .withColumn("code_formacode_5", df_courses["code_formacode_5"].cast(types.IntegerType()))\
    .withColumn("code_nsf_1", df_courses["code_nsf_1"].cast(types.IntegerType()))\
    .withColumn("code_nsf_2", df_courses["code_nsf_2"].cast(types.IntegerType()))\
    .withColumn("code_nsf_3", df_courses["code_nsf_3"].cast(types.IntegerType()))\
    .withColumn("frais_ttc_tot_max", df_courses["frais_ttc_tot_max"].cast(types.DecimalType(10, 2)))\
    .withColumn("frais_ttc_tot_min", df_courses["frais_ttc_tot_min"].cast(types.DecimalType(10, 2)))\
    .withColumn("frais_ttc_tot_mean", df_courses["frais_ttc_tot_mean"].cast(types.DecimalType(10, 2)))

df_courses_date = df_courses.withColumn('date_extract', F.to_date(F.col('date_extract'), 'yyyy-MM-dd'))

df_courses_date.coalesce(1).write.parquet(output_file, mode='overwrite')

spark.stop()