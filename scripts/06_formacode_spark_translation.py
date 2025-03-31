from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T
from deep_translator import GoogleTranslator
import time

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

df_formacode = spark.read\
                        .option("header", "true") \
                        .csv(input_file)

def translate(input_str):
    if input_str and len(input_str) > 0: # Check for empty or None
        try:
            time.sleep(0.3)
            return GoogleTranslator(source='auto', target='en').translate(input_str)
        except Exception as e:
            print(f"Translation error: {e}")
            return input_str # Return original string on error
    else:
        return "" # Return empty string for empty or None input

@F.udf(returnType=T.StringType())
def translate_udf(input_str): # udf, for dataframe columns.
    return translate(input_str)

# 1. Replace empty fields
df_formacode = df_formacode.withColumn("field", F.coalesce(F.col("field"), F.lit("000 unknown")))

df_formacode = df_formacode.repartition(8, "field")

# 1. Collect unique values
unique_fields = [x[0] for x in df_formacode.select("field").distinct().collect()]

# 2. Translate unique values
field_translations = {field: translate(field) for field in unique_fields if field is not None}

# 3. Broadcast dictionary
broadcast_fields = sc.broadcast(field_translations)

# 4. Create UDF to lookup values
@F.udf(returnType=T.StringType())
def lookup_translation(field):
    return broadcast_fields.value.get(field, "")

df_formacode = df_formacode.withColumn('description_en', translate_udf(F.col('description')))\
    .withColumn("field_en", lookup_translation(F.col("field")))

df_formacode.coalesce(1).write.parquet(output_file, mode='overwrite')