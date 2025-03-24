# france_courses_enrollments
Data Pipeline creation of france courses enrollments. Every month the providers report the enrollments in their programs. The idea is to get the courses listed as well as the enrollments every month and find the most enrolled courses and the provider that reported the most enrolled courses. 

# Setting up the Cloud

__Step 1:__ Create a project on Google Cloud. The name of the project is `france-courses-enrollments`

__Step 2:__ Create a service account by clicking on IAM. I have kept the service account name as `france-courses-enrollments`.
Select roles `Storage Admin`and `BigQuery Admin`.

__Step 3:__ Add a billing account to this project.

__Step 4:__ Create a cloud storage bucket `jugnu-france-course-enrollments`. Select suitable region. I have selected `europe-west1 (Belgium)`

# Kestra Set-up

__Step 1:__ Create `docker-compose.yml` file for running kestra. Run `docker compose up`. Access Kestra on `localhost:8080`

Below steps were just to debug and create requirements.txt file
__START__
__Step 1:__ In terminal `docker exec -it kestra bash`
__Step 2:__ In terminal `docker cp data_upload.py kestra:/tmp/data_upload.py`
__Step 3:__ In terminal `docker cp .dlt/secrets.toml kestra:/tmp/.dlt/secrets.toml`
__Step 4:__ In terminal (kestra bash) install dlt, pandas, dlt[gs], dlt[parquet]
__Step 5:__ In terminal (kestra bash) `python data_upload.py`
__Step 6:__ In terminal (kestra bash) `pip freeze > requirements.txt`
__Step 7:__ In terminal `docker cp kestra:/tmp/requirements.txt ./requirements.txt`
__END__

__Step 2:__ Execute `01_gcp_kv.yaml` to set up the key value pair. Later on you can modify them with the values that corresponds to your set-up by going to namespaces, selecting `france-courses-enrollments` and then selecting `KV Store`. You will need following key value pairs:
    . GCP_CREDS - It has the same content as the json file generated from google cloud.
    . GCP_DATASET - It is the same name as database in Bigquery.
    . GCP_BUCKET_NAME - It is the same name as Bucket in Google Cloud Storage.
    . GCP_PROJECT_ID - It is the Project Id that is automatically generated when creating the new Project on Google Cloud.
    . GCP_LOCATION - I had chosen europe-west1
    . SECRET_PROJECT_ID - Same as GCP_PROJECT_ID but needed for dlt
    . SECRET_CLIENT_EMAIL - Retrieve it from the json file downloaded
    . SECRET_PRIVATE_KEY - Retrieve it from the json file downloaded
    . SECRET_BUCKET_URL - URL for GCS `gs://....`

# Courses Data Retrieval using dlt, spark and bigquery queries.
__Step 1:__ 

__Step 2: __ Execute 02_fetch_data.yaml in kestra. It can be run with 3 inputs - `courses`, `enrollments` and `formacode`. `formacode`will generate the seed file by translating the french data into english using spark UDF. This is one time job and might be refreshed once in 6 months. But due to translation, it will take some time. Around 15 mins.

__Step 3:__ Execute 02_fetch_data.yaml in kestra with input as courses. This will generate the staging file courses.

__Step 4:__ 


# DBT Set-up

__Step 1:__ Create a new account `france-market-research`
__Step 2:__ Create a new connection to `BigQuery`
__Step 3:__ Create a new project and give details of the repository and the project subfolder as `dbt`. Also, create a development environment. Please note that the location of the dataset creation is europe-west1
__Step 4:__ 

