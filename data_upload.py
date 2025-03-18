import dlt
import requests
import io
import pandas as pd

url = "https://opendata.caissedesdepots.fr/api/explore/v2.1/catalog/datasets/moncompteformation_catalogueformation/exports/csv"

@dlt.resource(name="courses")
def fetch_courses_pipeline():
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            buffer = io.BytesIO()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                buffer.write(chunk)
            buffer.seek(0)
            table = pd.read_csv(buffer, sep=";", low_memory=False)
            print(f'Got data from {url} with {len(table)} records')
            if len(table) > 0:
                table['code_region'] = table['code_region'].astype(str)
                table['coderegion_export'] = table['coderegion_export'].astype(str)
                yield table
    except Exception as e:
        print(f"Failed to fetch data from {url}: {e}")

# Define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="moncompteformation_pipeline",
    destination="filesystem",
    dataset_name="courses_data"  # Top-level folder name
)

# Run the pipeline with the new resource, specify table name and destination path
load_info = pipeline.run(
    fetch_courses_pipeline(),
    write_disposition="replace",
    table_name="courses_france"
)
print(load_info)