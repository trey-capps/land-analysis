from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
from airflow import settings
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Function to create the analytics connection
def create_analytics_connection():
    conn = Connection(
        conn_id="analytics_db_conn",
        conn_type="postgres",
        host="postgres-analytics",
        schema="real_estate_analysis",
        login="analytics",
        password="analytics",
        port=5432
    )
    session = settings.Session()
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()
        print("Connection created:", conn.conn_id)
    else:
        print("Connection already exists:", conn.conn_id)

# Function to download the Excel file
def download_initial_data():
    url = "https://services.wake.gov/realdata_extracts/RealEstData12232024.xlsx"
    file_path = "/tmp/initial_data.xlsx"
    curl_command = f"curl -o {file_path} {url}"
    import os
    os.system(curl_command)
    return file_path

# Function to create the database schema
# Function to create the database schema
def create_database_schema():
    postgres_hook = PostgresHook(postgres_conn_id="analytics_db_conn")
    create_tables_sql = """
    -- Table: properties
    CREATE TABLE IF NOT EXISTS properties (
        real_estate_id VARCHAR PRIMARY KEY,
        owner1 VARCHAR,
        owner2 VARCHAR,
        mailing_address1 VARCHAR,
        mailing_address2 VARCHAR,
        mailing_address3 VARCHAR,
        card_number VARCHAR,
        number_of_cards INT,
        street_number VARCHAR,
        street_prefix VARCHAR,
        street_name VARCHAR,
        street_type VARCHAR,
        street_suffix VARCHAR,
        planning_jurisdiction VARCHAR,
        street_misc VARCHAR,
        township VARCHAR,
        fire_district VARCHAR,
        zoning VARCHAR,
        deeded_acreage NUMERIC,
        parcel_identification VARCHAR,
        special_district1 VARCHAR,
        special_district2 VARCHAR,
        special_district3 VARCHAR,
        billing_class VARCHAR,
        property_description TEXT,
        land_classification VARCHAR,
        city VARCHAR,
        grade VARCHAR,
        type_and_use VARCHAR,
        physical_city VARCHAR,
        physical_zip_code VARCHAR,
        created_at TIMESTAMP DEFAULT NOW(),
        last_updated TIMESTAMP DEFAULT NOW()
    );

    -- Table: property_transactions
    CREATE TABLE IF NOT EXISTS property_transactions (
        transaction_id SERIAL PRIMARY KEY,
        real_estate_id VARCHAR NOT NULL,
        land_sale_price NUMERIC,
        land_sale_date DATE,
        total_sale_price NUMERIC,
        total_sale_date DATE,
        deed_book VARCHAR,
        deed_page VARCHAR,
        deed_date DATE,
        assessed_building_value NUMERIC,
        assessed_land_value NUMERIC,
        vcs VARCHAR,
        year_built INT,
        num_of_rooms INT,
        units INT,
        heated_area INT,
        utilities VARCHAR,
        street_pavement VARCHAR,
        topography VARCHAR,
        year_of_addition INT,
        effective_year INT,
        remodeled_year INT,
        special_write_in TEXT,
        design_style VARCHAR,
        foundation_basement VARCHAR,
        foundation_basement_percent NUMERIC,
        exterior_wall VARCHAR,
        common_wall VARCHAR,
        roof VARCHAR,
        roof_floor_system VARCHAR,
        floor_finish VARCHAR,
        interior_finish VARCHAR,
        interior_finish1 VARCHAR,
        interior_finish1_percent NUMERIC,
        interior_finish2 VARCHAR,
        interior_finish2_percent NUMERIC,
        heat VARCHAR,
        heat_percent NUMERIC,
        air VARCHAR,
        air_percent NUMERIC,
        bath INT,
        bath_fixtures INT,
        built_in1_description VARCHAR,
        built_in2_description VARCHAR,
        built_in3_description VARCHAR,
        built_in4_description VARCHAR,
        built_in5_description VARCHAR,
        accrued_assessed_condition_pct NUMERIC,
        land_deferred_code VARCHAR,
        land_deferred_amount NUMERIC,
        historic_deferred_code VARCHAR,
        historic_deferred_amount NUMERIC,
        recycled_units INT,
        disq_and_qual_flag VARCHAR,
        land_disq_and_qual_flag VARCHAR,
        last_updated TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (real_estate_id) REFERENCES properties (real_estate_id)
    );
    """
    postgres_hook.run(create_tables_sql)

# Function to load data into the database
def clean_data(data):
    """
    Cleans and preprocesses the data to match the database schema.
    """
    # Object columns (convert to string and handle NaN)
    object_columns = [
        'owner1', 'owner2', 'mailing_address1', 'mailing_address2', 'mailing_address3',
        'street_prefix', 'street_name', 'street_type', 'street_suffix',
        'planning_jurisdiction', 'street_misc', 'zoning', 'parcel_identification',
        'special_district1', 'property_description', 'land_classification', 'deed_book',
        'deed_page', 'vcs', 'property_index', 'story_height', 'design_style',
        'foundation_basement', 'roof_floor_system', 'heat', 'air', 'built_in1_description',
        'built_in2_description', 'city', 'grade', 'land_deferred_code',
        'disq_and_qual_flag', 'land_disq_and_qual_flag', 'physical_city'
    ]
    for col in object_columns:
        if col in data.columns:
            data[col] = data[col].astype(str).fillna("")

    # Integer columns (coerce to numeric and fill NaN with 0)
    integer_columns = [
        'real_estate_id', 'card_number', 'number_of_cards', 'street_number', 'township',
        'billing_class', 'year_built', 'num_of_rooms', 'units', 'heated_area',
        'year_of_addition', 'effective_year', 'remodeled_year', 'foundation_basement_percent',
        'heat_percent', 'air_percent', 'bath_fixtures', 'assessed_grade_difference',
        'accrued_assessed_condition_pct', 'land_deferred_amount', 'historic_deferred_amount',
        'recycled_units', 'physical_zip_code'
    ]
    for col in integer_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0).astype(int)

    # Float columns (coerce to numeric and fill NaN with 0.0)
    float_columns = [
        'fire_district', 'deeded_acreage', 'special_district2', 'special_district3',
        'street_pavement', 'topography', 'special_write_in', 'common_wall', 'roof',
        'floor_finish', 'interior_finish', 'interior_finish1', 'interior_finish1_percent',
        'interior_finish2', 'interior_finish2_percent', 'type_and_use'
    ]
    for col in float_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0.0).astype(float)

    # Date columns (convert to datetime)
    date_columns = ['land_sale_date', 'total_sale_date', 'deed_date']
    for col in date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], errors='coerce')

    return data

def load_data_to_database(file_path):
    """
    Loads the cleaned data into the database.
    """
    # Load the Excel data
    data = pd.read_excel(file_path)

    # Clean the data
    data = clean_data(data)

    # Prepare connection to the database
    engine = create_engine("postgresql://analytics:analytics@postgres-analytics:5432/real_estate_analysis")

    # Split data for `properties` and `property_transactions`
    properties_columns = [
        'real_estate_id', 'owner1', 'owner2', 'mailing_address1', 'mailing_address2',
        'mailing_address3', 'card_number', 'number_of_cards', 'street_number',
        'street_prefix', 'street_name', 'street_type', 'street_suffix',
        'planning_jurisdiction', 'street_misc', 'township', 'fire_district',
        'zoning', 'deeded_acreage', 'parcel_identification', 'special_district1',
        'special_district2', 'special_district3', 'billing_class',
        'property_description', 'land_classification', 'city', 'grade',
        'physical_city', 'physical_zip_code'
    ]
    transactions_columns = [
        'real_estate_id', 'land_sale_price', 'land_sale_date', 'total_sale_price',
        'total_sale_date', 'deed_book', 'deed_page', 'deed_date',
        'assessed_building_value', 'assessed_land_value', 'year_built', 'num_of_rooms',
        'units', 'heated_area', 'year_of_addition', 'effective_year', 'remodeled_year',
        'foundation_basement_percent', 'heat_percent', 'air_percent', 'bath_fixtures',
        'recycled_units'
    ]

    # Insert data into `properties`
    properties_data = data[properties_columns]
    properties_data.to_sql('properties', engine, if_exists='append', index=False)

    # Insert data into `property_transactions`
    transactions_data = data[transactions_columns]
    transactions_data.to_sql('property_transactions', engine, if_exists='append', index=False)

# DAG definition
with DAG(
    "initial_setup_with_connection",
    start_date=datetime(2024, 12, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_connection_task = PythonOperator(
        task_id="create_analytics_connection",
        python_callable=create_analytics_connection
    )

    download_task = PythonOperator(
        task_id="download_initial_data",
        python_callable=download_initial_data
    )

    create_schema_task = PythonOperator(
        task_id="create_database_schema",
        python_callable=create_database_schema
    )

    load_data_task = PythonOperator(
        task_id="load_initial_data",
        python_callable=load_data_to_database,
        op_args=["/tmp/initial_data.xlsx"]
    )

    create_connection_task >> download_task >> create_schema_task >> load_data_task