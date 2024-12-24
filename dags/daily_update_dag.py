from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Function to download daily Excel data
def download_daily_data():
    url = "https://example.com/daily_data.xlsx"  # Replace with actual URL
    file_path = "/tmp/daily_data.xlsx"
    curl_command = f"curl -o {file_path} {url}"
    import os
    os.system(curl_command)
    return file_path

# Function to update records in the database
def update_records(file_path):
    # Load the Excel data
    data = pd.read_excel(file_path)
    
    # Transform the data if needed
    data.columns = data.columns.str.lower()  # Example: Convert column names to lowercase
    
    # Insert/Update in the database
    engine = create_engine("postgresql://username:password@localhost:5432/real_estate")
    for _, row in data.iterrows():
        query = f"""
        INSERT INTO properties (real_estate_id, street_number, street_name, street_type, planning_jurisdiction, physical_zip_code, fire_district, year_built, property_description, last_updated)
        VALUES ('{row.real_estate_id}', '{row.street_number}', '{row.street_name}', '{row.street_type}', '{row.planning_jurisdiction}', '{row.physical_zip_code}', '{row.fire_district}', {row.year_built}, '{row.property_description}', NOW())
        ON CONFLICT (real_estate_id)
        DO UPDATE SET
            street_number = EXCLUDED.street_number,
            street_name = EXCLUDED.street_name,
            street_type = EXCLUDED.street_type,
            planning_jurisdiction = EXCLUDED.planning_jurisdiction,
            physical_zip_code = EXCLUDED.physical_zip_code,
            fire_district = EXCLUDED.fire_district,
            year_built = EXCLUDED.year_built,
            property_description = EXCLUDED.property_description,
            last_updated = NOW();
        """
        engine.execute(query)

# DAG definition
with DAG(
    "daily_update",
    start_date=datetime(2023, 12, 2),
    schedule_interval="0 0 * * *",  # Run daily at midnight
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id="download_daily_data",
        python_callable=download_daily_data
    )

    update_task = PythonOperator(
        task_id="update_records",
        python_callable=update_records,
        op_args=["/tmp/daily_data.xlsx"]
    )

    download_task >> update_task