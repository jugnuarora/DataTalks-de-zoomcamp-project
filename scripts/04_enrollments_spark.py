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

df_enrollments = spark.read.option("header", "true").parquet(input_file)

# Cast the 'code_inventaire' column to a string
df_enrollments = df_enrollments\
    .withColumn('annee_mois', F.to_date(df_enrollments["annee_mois"], "yyyy-MM"))\
    .withColumn("code_certifinfo", df_enrollments["code_certifinfo"].cast(types.IntegerType()))\
    .withColumn("date_chargement", F.to_date(df_enrollments["date_chargement"], "yyyy-MM-dd"))

# Define the columns to rename and their new names
# columns_to_rename = {
#         'annee_mois': 'year_month',
#         'annee': 'year',
#         'mois': 'month',
#         'type_referentiel': 'type_referential',
#         'code_certifinfo': 'code_certification',
#         'intitule_certification': 'certification_title',
#         'siret_of_contractant': 'provider_id',
#         'raison_sociale_of_contractant': 'provider',
#         'entrees_formation': 'training_entries',
#         'sorties_realisation_partielle': 'partial_completion_exits',
#         'sorties_realisation_totale': 'total_completion_exits',
#         'date_chargement': 'load_date'
#     }

# # Rename the columns
# for old_name, new_name in columns_to_rename.items():
#     if old_name in df_enrollments.columns:
#         df_enrollments = df_enrollments.withColumnRenamed(old_name, new_name)
#     else:
#         print(f"Column '{old_name}' not found, skipping rename.")

df_enrollments.coalesce(1).write.parquet(output_file, mode='overwrite')

spark.stop()