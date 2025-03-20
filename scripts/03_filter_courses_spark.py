import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql import functions as F
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

df_courses_date = df_courses.withColumn('date_extract', F.to_date(F.col('date_extract'), 'yyyy-MM-dd'))

filter_values = [
    "Informatique, traitement de l'information, réseaux de transmission",
    "Enseignement, formation",
    "Commerce, vente",
    "Comptabilite, gestion",
    "Spécialités pluri-scientifiques",
    "Spécialites plurivalentes de la communication et de l'information"
]

df_courses_filtered = df_courses_date.filter(col("libelle_nsf_1").isin(filter_values))

# Define the columns to rename and their new names
columns_to_rename = {
    'nom_of': 'provider',
    'siret': 'provider_ID',
    'nom_region': 'region',
    'nom_departement': 'department',
    'intitule_certification': 'certification_title',
    'libelle_niveau_sortie_formation': 'training_exit_level',
    'libelle_code_formacode_principal': 'main_formacode_desc',
    'libelle_nsf_1': 'nsf_code_1_desc',
    'libelle_nsf_2': 'nsf_code_2_desc',
    'libelle_nsf_3': 'nsf_code_3_desc',
    'numero_formation': 'training_ID',
    'intitule_formation': 'title',
    'points_forts': 'strengths',
    'nb_session_active': 'nb_active_session',
    'nb_session_a_distance': 'nb_distant_session',
    'nombre_heures_total_min': 'duration_min',
    'nombre_heures_total_max': 'duration_max',
    'nombre_heures_total_mean': 'duration_mean',
    'frais_ttc_tot_min': 'cost_min',
    'frais_ttc_tot_max': 'cost_max',
    'frais_ttc_tot_mean': 'cost_mean'
}

# Rename the columns
for old_name, new_name in columns_to_rename.items():
    if old_name in df_courses_filtered.columns:
        df_courses_filtered = df_courses_filtered.withColumnRenamed(old_name, new_name)
    else:
        print(f"Column '{old_name}' not found, skipping rename.")

df_courses_filtered.coalesce(1).write.parquet(output_file, mode='overwrite')

spark.stop()