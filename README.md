## Project Overview

This project focuses on building an end-to-end data pipeline to analyze training course and enrollment data within the French market. The goal is to track course trends over time, understand enrollment patterns, and provide actionable insights through a dashboard.

**Datasets Used:**

1.  **Courses Data:** [Mon Compte Formation Catalogue](https://opendata.caissedesdepots.fr/explore/dataset/moncompteformation_catalogueformation/information/?disjunctive.libelle_niveau_sortie_formation&disjunctive.nom_region&disjunctive.nom_departement&disjunctive.type_referentiel&disjunctive.code_region) - Contains information about available training courses, including provider details, course titles, and regions.
2.  **Enrollments Data:** [Entree Sortie Formation](https://opendata.caissedesdepots.fr/explore/dataset/entree_sortie_formation/information/?disjunctive.type_referentiel&disjunctive.mois) - Provides monthly enrollment data, showing the number of trainees enrolled in specific courses.
3.  **Formacode Data:** [Formacode Centre Inffo](https://formacode.centre-inffo.fr/telechargements-54.html) - Classifies training programs by skills (using Formacode codes) and includes descriptions and semantic fields. There are 5 formacodes allowed to be assigned to `Courses`.Example with their description is as below:
    - 31025: Data Analytics
    - 31026: Data Science
    - 31028: Artificial Intelligence
    - 31035: Data Visualization

    Other than this there is a column `field`which contains 65 semantic field that helps in grouping the formacodes at higher level. For example, all the above would be part of ÌT and information systems. Other Example wold be Financial Management account. 

**Problem Statement:**

The primary objective is to develop a dashboard that visualizes:

* The monthly evolution of training courses, allowing for trend analysis and identification of new course launches.
* Enrollment patterns over time, showing the popularity of different courses and providers.

A key challenge is the lack of historical course launch dates. To address this, the pipeline is designed to capture monthly snapshots of course data, enabling retrospective analysis. So, if in the data today it shows there are 7 providers giving courses on data analytics, there is no way to find out what was the number 3 months back.

Additionally, the enrollment dataset's potential for data resets necessitates an incremental data ingestion approach. 

The formacode dataset, in French, requires translation and processing for broader stakeholder understanding.

## Data Pipeline Architecture

**Technologies Used:**

* **Cloud:** Google Cloud Platform (GCP)
* **Workflow Orchestration:** Kestra
* **Data Lake:** Google Cloud Storage (GCS)
* **Data Warehouse:** BigQuery
* **Batch Processing:** Apache Spark (Databricks Delta Live Tables(DLT))
* **Data Transformation:** dbt Cloud
* **Data Transformation:** Looker

**Pipeline Components:**

1.  **Data Ingestion (Batch):**
    * **Courses Data:** DLT is used to extract the course data from the source, store it as raw data in GCS, perform initial Spark transformations, and load it into BigQuery, partitioned by `data_extracted` and clustered by `code-formacode-1`.
    * **Enrollments Data:** DLT is used to extract the enrollments data from the source, store it as raw data in GCS, perform initial Spark transformations, and load it into BigQuery, partitioned by `year_month` and clustered by `provider`.
    * **Formacode Data:** Kestra downloads the Formacode ZIP file, extracts relevant data, translates descriptions and field columns using Spark, and stores the processed data in BigQuery. Broadcast variables in Spark are used for semantic fields translation.
2.  **Workflow Orchestration (Kestra):**
    * A single Kestra workflow is created to orchestrate the courses and enrollments data pipelines, scheduled to run on the first Sunday of each month at 3 AM.
    * A seperate kestra workflow is created to handle the formacode download, extraction, translation and upload. It is once in a while job as it remains more or less constant. 
3.  **Data Transformation (dbt Cloud):**
    * dbt Cloud is used to build data models for the final fact and dimension tables in BigQuery.
    * A dbt macro is implemented to remove leading numbers from the `field` column in the Formacode data. This macro could be leveraged in future to remove leading digits in `generic_term`.
    * An environment variable `DBT_ENVIRONMENT` is used to control data limiting in the `stg_courses` model (development: limit to 100 rows, production: full dataset). This can be overridden using local variables `limit_data` and further by passing false to the variable in command-bar.
    * Tests are included in the dbt project to ensure data integrity. There are warnings issued showing that there are few formacodes listed in courses which are not part of formacode dimension file. 
    * The project includes both development and production environments, with CI/CD job for deployment.
4.  **Data Warehouse (BigQuery):**
    * BigQuery is used as the data warehouse, with tables partitioned and clustered for optimal query performance.
    * Local SQL queries are provided for data verification and reconciliation at different steps. 

## Dashboard

**Tool:** Google Data Studio

The dashboard includes two tiles:

1.  **Distribution of Course Categories:** A graph showing the distribution of courses across different Formacode categories, providing insights into the areas with trainings and providers. I have come up with a `KPI` here which is `Trainings Provider Ratio`. It is the ratio od total number of trainings and total number of providers. If 10 providers are providing 150 bootcamps for a technology, then BPR = 150/10 = 15. The interpretation is:
    - LOW: might indicate less demand due to lower number of trainings or overcrowdedness due to high number of providers.
    - HIGH: might indicate oversaturation due to too many trainings or less competition due to less number of providers. 
    We suggest to look at the TPR between 5 and 15. 
2.  **Monthly Enrollment Trends:** A line graph illustrating the monthly enrollment trends for selected courses or providers, highlighting temporal patterns and growth.

## Reproducibility

**Prerequisites:**

* GCP account with BigQuery and GCS enabled.
* Kestra instance running.
* dbt Cloud account with BigQuery connection.
* Python 3.x with necessary libraries (specified in `requirements.txt`).
* Service account keys for GCP authentication.

**Steps to Run:**

1.  **Clone the Repository:**
    ```bash
    git clone git@github.com:jugnuarora/france_courses_enrollments.git
    cd france_courses_enrollments
    ```
2. **Setting up the Cloud:**

    __Step 1:__ Create a project on Google Cloud. The name of the project is `france-courses-enrollments`.

    __Step 2:__ Create a service account by clicking on IAM. I have kept the service account name as `france-courses-enrollments`.
    Select roles `Storage Admin`and `BigQuery Admin`.
    Also generate the json key and store it safely for further connection to GCS and bigquery. 

    __Step 3:__ Add a billing account to this project.

    __Step 4:__ Create a cloud storage bucket `jugnu-france-course-enrollments`. Select suitable region. I have selected `europe-west1 (Belgium)`. 

3.  **Configure Kestra:**
    __STEP 1:__ Run `docker compose up`. Access Kestra on `localhost:8080`.

    __STEP 2:__ Import the Kestra workflows 

        . 01_gcp_kv.yaml - Execute it and later go to the namespace, select france_courses_enrollments. Go to kv store and make sure that the values are corresponding to your data lake and warehouse setup.

        . 02_courses_enrollments_pipeline.yaml

        . 03_formacode_pipeline.yaml

    __STEP 3:__ Execute `01_gcp_kv.yaml` to set up the key value pair. Later on you can modify them with the values that corresponds to your set-up by going to namespaces, selecting `france-courses-enrollments` and then selecting `KV Store`. You will need following key value pairs:
        . GCP_CREDS - It has the same content as the json file generated from google cloud.
        . GCP_DATASET - It is the same name as database in Bigquery.
        . GCP_BUCKET_NAME - It is the same name as Bucket in Google Cloud Storage.
        . GCP_PROJECT_ID - It is the Project Id that is automatically generated when creating the new Project on Google Cloud.
        . GCP_LOCATION - I had chosen europe-west1
        . SECRET_PROJECT_ID - Same as GCP_PROJECT_ID but needed for dlt
        . SECRET_CLIENT_EMAIL - Retrieve it from the json file downloaded
        . SECRET_PRIVATE_KEY - Retrieve it from the json file downloaded
        . SECRET_BUCKET_URL - URL for GCS `gs://....`

4.  **Bigquery Set-up:**
    Make sure to have following dataset in bigquery:
        . source_tables
        . courses
        . enrollments

    These are required for the kestra workflow to generate the source tables.

5.  **Configure dbt Cloud:**

    __Step 1:__ Create a new account `france-market-research`.

    __Step 2:__ Create a new connection to `BigQuery` using the json file generated. Please note that the location of the dataset creation is europe-west1.

    __Step 3:__ Create a new project and give details of the repository and the project subfolder as `dbt`. Add repository using GitHub. 

    __Step 4:__ Also, create a development environment. Please note that the location of the dataset creation is europe-west1, Connection is Bigquery. The dataset name is 'dbt_models'.

    __Step 5:__ Create environment variable `DBT_ENVIRONMENT`, where Project default will be `production`and development will be `development`.

    __Step 6:__ Now, you can access the contents of the project repository in Develop Cloud IDE. 

    __Step 7:__ In `stg_courses`there is a variable defined `limit_data`, which is true if the environment variable `DBT_ENVIRONMENT`is `development`. To override it in development, `dbt run --vars '{"limit_data": false}'`. Otherwise, be default in development, it will only give 100 rows.

    __Step 8:__ Set-up the `Production`environment.

    __Step 9:__ Create a new job `Monthly`, which will run in production environment and I have set the Schedule as Cron Schedule for Monthly. See the screenshot.

    __Step 10:__ Create a new CI/CD job, which will run as soon as there is a merge in the main branch of the git hub repo associated. See the screenshot.

5.  **Run Kestra Workflows:**
    * Trigger the Kestra workflow 02_courses_enrollments_pipeline.yaml to start the data ingestion and processing pipelines for courses and enrollments respectively. 

    * Trigger the Kestra workflow 03_formacode_pipeline.yaml to start the data ingestion and processing pipeline for formacode. This will take some time to execute (almost 45-50 mins) because of the translation.

6.  **Verify Data:**
    * run the provided sql queries (local_queries) to verify the data.

7.  **Run dbt Models:**
    * Execute the dbt models in dbt Cloud to transform the data. You can execute in one of the ways listed below:
        - Using dbt command bar and giving:
            `dbt run --vars '{"limit_data": false}'
        
        - Executing the Monthly job now. The monthly job once finished will generate the docs as well.

8.  **Visualize in Dashboard:**
    * You can access the visualization [here](https://lookerstudio.google.com/reporting/71ceb6ee-f472-4892-8e08-6689d9dbd42c).

## Going the Extra Mile (Optional)

* **Tests:** Included dbt tests for data integrity.
* **CI/CD:** CI/CD pipelines set up for dbt Cloud deployment.
* **Documentation:** This README provides detailed instructions and explanations.

## Future Improvements

* Use Terraform for Virtualization
* Explore real-time data ingestion for enrollment data.
* Optimize the Formacode translation job.
