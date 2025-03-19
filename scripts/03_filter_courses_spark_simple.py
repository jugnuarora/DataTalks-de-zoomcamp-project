import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F

#credentials_location = './gcs.json'

spark = SparkSession.builder \
        .master("local[*]") \
        .appName('courses') \
        .getOrCreate()

df_courses = spark.read.option("header", "true").parquet('gs://jugnu-france-course-enrollments/courses_data/courses_raw_parquet/*.parquet')

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

df_courses_filtered.coalesce(1).write.parquet('gs://jugnu-france-course-enrollments/courses_data/courses_raw_parquet/france_courses_en.parquet', mode='overwrite')

spark.stop()