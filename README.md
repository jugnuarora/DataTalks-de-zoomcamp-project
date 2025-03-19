# france_courses_enrollments
Data Pipeline creation of france courses enrollments. Every month the providers report the enrollments in their programs. The idea is to get the courses listed as well as the enrollments every month and find the most enrolled courses and the provider that reported the most enrolled courses. 

# Setting up the Cloud

__Step 1:__ Create a project on Google Cloud. The name of the project is `france-courses-enrollments`

__Step 2:__ Create a service account by clicking on IAM. I have kept the service account name as `france-courses-enrollments`.
Select roles `Storage Admin`and `BigQuery Admin`.

__Step 3:__ Add a billing account to this project.

__Step 4:__ Create a cloud storage bucket `jugnu-france-course-enrollments`. Select suitable region. I have selected `europe-west1 (Belgium)`

# Kestra Set-up

__Step 1:__ Add secrets.toml 
__Step 1:__ Create `docker-compose.yml` file for running kestra. Run `docker compose up`. Access Kestra on `localhost:8080`

__Step 2:__ Execute `01_gcp_kv.yaml` to set up the key value pair. Later on you can modify them with the values that corresponds to your set-up by going to namespaces, selecting `open-payments-cms` and then selecting `KV Store`.

__Step 3:__ Add 
# Data Retrieval using API


