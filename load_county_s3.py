import boto3
import json
import argparse
import os

BUCKET_NAME = "land-finder-cig"

#Load S3 credentials
with open("./credentials.json", "r") as cred:
        credentials = json.load(cred)
        ACCESS_ID = credentials["accessKeyID"]
        SECRET_KEY = credentials["secretAccessKey"]

def create_boto_session():
    """Create boto3 session to access AWS services"""
    #Create Session
    try:
        session = boto3.Session(
            aws_access_key_id = ACCESS_ID,
            aws_secret_access_key = SECRET_KEY)
        return session
    except Exception as e:
        print(f"Unable to connect to AWS with boto3, error: {e}")

def connect_to_s3(session):
    """Connect to S3 using boto3"""
    try:
        conn = session.resource("s3")
        return conn
    except Exception as e:
        print(f"Unable to connect to s3, error: {e}")

def upload_to_bucket(conn, file_name):
    """Upload file from 'data' directory to S3"""
    bucket = conn.Bucket(BUCKET_NAME)
    bucket.meta.client.upload_file(
        Filename = "./data/" + file_name,
        Bucket = BUCKET_NAME,
        Key = file_name)
    print(f"Successfully uploaded {file_name} to {BUCKET_NAME}")

def remove_data_from_temp(file_name):
    """Remove the uploaded folder from 'data' directory"""
    print(f"Removing {file_name} from ./data/")
    os.system(f"rm ./data/{file_name}")

def main(params):
     #Get user params
    file_name = params.file

    #Load Data to s3
    session = create_boto_session()
    conn = connect_to_s3(session)
    upload_to_bucket(conn, file_name)
    remove_data_from_temp(file_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Load county data to S3")
    parser.add_argument('--file', required=True, help="file name to be loaded")
    
    params = parser.parse_args()
    main(params)