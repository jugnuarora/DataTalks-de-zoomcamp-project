#!/bin/bash
clear

echo "🚀 Starting french Courses Enrollments Project Setup!"

# Add these lines to stop and remove any existing kestra container.
echo "🔹 Stopping and removing any existing Kestra container..."
docker stop kestra 2>/dev/null
docker rm kestra 2>/dev/null

gcloud auth revoke --all

TODAY=$(date +%Y_%m_%d)

# Ensure script runs in its directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit

GCP_LOCATION=europe-west1

#echo "✅ .env file created successfully."

# Create secrets folder
mkdir -p secrets
rm -f secrets/gcp_credentials.json

# Authenticate with Google Cloud
echo "🔹 Authenticating with Google Cloud..."
gcloud auth login || { echo "❌ GCP login failed! Exiting..."; exit 1; }

# Get current user email
USER_EMAIL=$(gcloud config get-value account)
echo "🔹 You are logged in as: $USER_EMAIL"

# Select or create GCP project
echo "🔹 Fetching available GCP projects..."
gcloud projects list --format="table(projectId, name)"

while true; do
  read -p "Enter an existing GCP Project ID (or press Enter to create a new one): " GCP_PROJECT_ID

  if [ -z "$GCP_PROJECT_ID" ]; then
    read -p "Enter a new GCP Project ID: " GCP_PROJECT_ID
    echo "🔹 Creating new project: $GCP_PROJECT_ID..."
    
    gcloud projects create $GCP_PROJECT_ID --name="$GCP_PROJECT_ID" --set-as-default
    PROJECT_EXISTS=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectId)" 2>/dev/null)
    
    if [ -z "$PROJECT_EXISTS" ]; then
      echo "❌ Failed to create project. Please check permissions."
      exit 1
    fi
    echo "✅ Project created successfully!"
    break
  else
    PROJECT_EXISTS=$(gcloud projects describe $GCP_PROJECT_ID --format="value(projectId)" 2>/dev/null)
    if [ -n "$PROJECT_EXISTS" ]; then
      echo "✅ Using existing project: $GCP_PROJECT_ID"
      break
    else
      echo "❌ Project '$GCP_PROJECT_ID' does not exist."
    fi
  fi
done

# Set the project
gcloud config set project $GCP_PROJECT_ID

# Check Billing
echo "🔹 Checking if billing is enabled..."
BILLING_STATUS=$(gcloud billing projects describe $GCP_PROJECT_ID --format="value(billingEnabled)")
if [ "$BILLING_STATUS" != "True" ]; then
   echo "❌ Billing is not enabled for this project!"
  echo "To proceed, you must enable billing manually."

  # Show available billing accounts
  echo "🔹 Available billing accounts:"
  gcloud billing accounts list

  echo "🔹 Follow these steps to enable billing:"
  echo "   1️⃣ Go to the Billing Console: https://console.cloud.google.com/billing"
  echo "   2️⃣ Select or link a billing account to your project: $GCP_PROJECT_ID"
  echo "   3️⃣ Once billing is activated, re-run this script."

  exit 1
fi

echo "✅ Billing is enabled."

# Create Service Account
SERVICE_ACCOUNT_NAME="french-courses-enrollments"
SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT_NAME@$GCP_PROJECT_ID.iam.gserviceaccount.com"
KEY_FILE_PATH="secrets/gcp_credentials.json"

EXISTING_SA=$(gcloud iam service-accounts list --format="value(email)" --filter="email:$SERVICE_ACCOUNT_EMAIL")

if [ -z "$EXISTING_SA" ]; then
    echo "🔹 Creating service account..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME --display-name "Service Account for GCS Access"
fi

# Assign IAM roles (always)
echo "🔹 Assigning/Ensuring IAM roles..."
for role in "roles/storage.admin" "roles/bigquery.admin" "roles/iam.serviceAccountAdmin"; do
    gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" --role="$role"
done


# Generate service account key
echo "🔹 Generating service account key..."
rm -f secrets/gcp_credentials.json

# Attempt to create key, handle max key error
if ! gcloud iam service-accounts keys create secrets/gcp_credentials.json --iam-account=$SERVICE_ACCOUNT_EMAIL; then
    # if the error is a precondition failure, try deleting old keys.
    if [[ "$?" -eq 1 ]]; then
        echo "🔹 Max number of keys reached, deleting old keys..."
        KEY_IDS=$(gcloud iam service-accounts keys list --iam-account=$SERVICE_ACCOUNT_EMAIL --format='value(name)')
        for KEY_ID in $KEY_IDS; do
            gcloud iam service-accounts keys delete $KEY_ID --iam-account=$SERVICE_ACCOUNT_EMAIL -q
        done
        # try to create the key again.
        if ! gcloud iam service-accounts keys create secrets/gcp_credentials.json --iam-account=$SERVICE_ACCOUNT_EMAIL; then
            echo "❌ Failed to create service account key after key deletion."
            exit 1
        fi
    else
        echo "❌ Failed to create service account key. Precondition check failed."
        exit 1
    fi
fi

chmod 644 $KEY_FILE_PATH

# Add a delay here to allow the key to propagate
echo "⏳ Waiting for key propagation (30 seconds)..."
sleep 30

# Validate key file
if [ ! -f "$KEY_FILE_PATH" ]; then
    echo "❌ Failed to create service account key."
    exit 1
fi

# Try to activate service account (Detect JWT Error)
echo "🔹 Activating service account..."
if ! gcloud auth activate-service-account --key-file=$KEY_FILE_PATH 2>&1 | tee activation_log.txt | grep -q "invalid_grant"; then
    echo "✅ Service account activated successfully."
else
    echo "❌ Invalid JWT Signature detected! Recreating key..."
    
    # Delete old key and regenerate
    gcloud iam service-accounts keys delete $(jq -r .private_key_id < $KEY_FILE_PATH) --iam-account=$SERVICE_ACCOUNT_EMAIL -q
    rm -f $KEY_FILE_PATH
    
    gcloud iam service-accounts keys create $KEY_FILE_PATH --iam-account=$SERVICE_ACCOUNT_EMAIL
    chmod 644 $KEY_FILE_PATH

    # Retry activation
    if ! gcloud auth activate-service-account --key-file=$KEY_FILE_PATH; then
        echo "❌ Service account activation failed again. Please check permissions manually."
        exit 1
    fi
fi

echo "✅ GCP authentication complete."

# 7️⃣ Deploy Infrastructure with Terraform
echo "🔹 Deploying infrastructure with Terraform..."
cd terraform
terraform init
terraform apply -auto-approve \
  -var="project_id=$GCP_PROJECT_ID" \
  -var="region=europe-west1" \
  -var="bucket_name=$(whoami)-${GCP_PROJECT_ID}" \
  -var="service_account_email=$SERVICE_ACCOUNT_EMAIL"

if [ $? -ne 0 ]; then
    echo "❌ Terraform deployment failed! Exiting..."
    exit 1
fi

echo "✅ Terraform deployment complete."

# ✅ # Extract Terraform Outputs
GCP_BUCKET_NAME=$(terraform output -raw bucket_name)
GCP_CREDS_JSON=$(terraform output -json GCP_CREDS | jq -r tostring)

cd ..

# Echo the variables before sed commands
echo "GCP_CREDS_JSON: $GCP_CREDS_JSON"
echo "GCP_PROJECT_ID: $GCP_PROJECT_ID"
echo "GCP_LOCATION: $GCP_LOCATION"
echo "GCP_BUCKET_NAME: $GCP_BUCKET_NAME"
echo "GCP_DATASET: $BIGQUERY_SOURCE_DATASET"

# Replace Placeholders in 01_gcp_kv.yaml
echo "🔹 Replacing placeholders in 01_gcp_kv.yaml..."
# Use Perl and sed commands to replace placeholders and save to a new file
#perl -pe "s|gcp_creds_value|$GCP_CREDS_JSON|g" 01_gcp_kv.yaml > new_01_gcp_kv.yaml
sed "s|gcp_project_id_value|$GCP_PROJECT_ID|g" 01_gcp_kv.yaml > new_01_gcp_kv.yaml
sed "s|gcp_location_value|$GCP_LOCATION|g" new_01_gcp_kv.yaml > temp.yaml && mv temp.yaml new_01_gcp_kv.yaml
sed "s|gcp_bucket_name_value|$GCP_BUCKET_NAME|g" new_01_gcp_kv.yaml > temp.yaml && mv temp.yaml new_01_gcp_kv.yaml

# Replace Placeholders in local_queries.sql
echo "🔹 Replacing placeholders in local_queries.sql..."
sed "s|gcp_bucket_name_value|$GCP_BUCKET_NAME|g" local_queries.sql > temp.yaml && mv temp.yaml new_local_queries.sql
sed "s|todays_date|$TODAY|g" new_local_queries.sql > temp.yaml && mv temp.yaml new_local_queries.sql

# 8️⃣ Start Docker & Kestra
echo "🔹 Starting Docker services... THIS WILL TAKE SOMETIME!"
docker-compose build --no-cache
docker-compose up -d


# 9️⃣ Wait for Kestra
echo "⏳ Waiting for Kestra to initialize..."
sleep 120  

# 🔟 Kestra workflow import.
echo "🔹 Importing Kestra workflows..."
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@new_01_gcp_kv.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@02_courses_enrollments_pipeline.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@03_formacode_pipeline.yaml
curl -X POST http://localhost:8080/api/v1/flows/import -F fileUpload=@04_dbt_execution.yaml

# Display URLs
echo "✅ Setup Complete!"
echo "📊 Visit Kestra: http://localhost:8080 and the namespace is france-courses-enrollments. Go to Flows."
read -p "📊 Execute 01_gcp_kv in Kestra and press enter once done..."
echo "📊 Update GCP_CREDS in KV Store of the same namespace. Your key would have been downloaded in secrets folder of this repo. You can copy paste the contents from it."
read -p "Press enter once done..."
read -p "📊 This can be done simultaneously:
    a) Execute 02_courses_enrollments_pipeline with input as courses. 
    b) Execute 02_courses_enrollments_pipeline with input as enrollments. 
    c) Execute 03_formacode_pipeline. It will take ~45-50 mins
    and press enter once done..."
read -p "📊 Verify source_tables in bigquery. It should have courses, enrollments, formacode. You can copy the queries from new_local_queries.sql to local queries in bigquery and run the reconciliation steps mentioned in manual set-up of README. Press enter once done..."
echo "📊 Execute 04_dbt_execution"
read -p "Execute 04_dbt_execution and press enter once done..."
echo "📊 You are ready to visualize."

# Prompt for Terraform Destroy
read -p "Press Enter to destroy the GCS bucket and BigQuery datasets (or Ctrl+C to skip)..."

# Delete the new_01_gcp_kv.yaml file
echo "🔹 Deleting the new_01_gcp_kv.yaml file..."
rm -f new_01_gcp_kv.yaml
echo "✅ new_01_gcp_kv.yaml deleted."

# Delete the new_local_queries.sql file
echo "🔹 Deleting the new_local_queries.sql file..."
rm -f new_local_queries.sql
echo "✅ new_local_queries.sql deleted."

#Docker compose down
echo "🔹 Stoping Docker services... THIS WILL TAKE SOMETIME!"
docker-compose down

# Destroy Terraform Resources
echo "🔹 Destroying Terraform resources..."
cd terraform
terraform destroy -auto-approve \
    -var="project_id=$GCP_PROJECT_ID" \
    -var="region=europe-west1" \
    -var="bucket_name=$GCP_BUCKET_NAME" \
    -var="service_account_email=$SERVICE_ACCOUNT_EMAIL"

if [ $? -ne 0 ]; then
    echo "❌ Terraform destroy failed!"
else
    echo "✅ Terraform resources destroyed successfully."
fi

cd ..