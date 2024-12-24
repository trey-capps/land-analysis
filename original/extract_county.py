import os
from zipfile import ZipFile
from datetime import date
import argparse

def get_data(data_url):
    """Get the dataset from url"""
    os.system(f"wget {data_url}")
    print(f"Extracted data from: {data_url}")

def remove_dir(dir_name):
    """Remove parent directory"""
    print("Removing parent directory")
    os.system(f"rm ./{dir_name}")

def unzip(zip_file, extract_file):
    """Unzip specified file (extract_file) from the zip_file"""
    #Extract file from zip
    with ZipFile(zip_file, 'r') as zip:
        print(f"Extracting {extract_file}")
        zip.extract(extract_file)

    #Remove the unzipped folder with unecessary data
    remove_dir(zip_file)
    print('Unzipped File and removed zipped folder')

def rename(file_name, cnty_abrev):
    """Rename dataset and move to data/directory"""
    split_path = file_name.split(".")[1]
    export_name = f"{date.today()}_{cnty_abrev}_raw.{split_path}"
    os.system(f"mv ./{file_name} ./data/{export_name}")
    print(f"Renamed {file_name} to {export_name}, currently location: ./data/{export_name}")

def main(params):
    """main function to extract dataset"""
    #Get user params
    url = params.url
    file_name = params.file #always
    parent = params.parent_dir #sometimes needed
    cnty = params.cnty_id

    #Extract Data
    get_data(url)
    if parent:
        unzip(parent, file_name)
    rename(file_name, cnty)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from county website")
    parser.add_argument('--url', required=True, help='url of dataset')
    parser.add_argument('--file', required=True, help='file name to be extracted')
    parser.add_argument('--cnty_id', required=True, help='two letter abreviation for county')
    parser.add_argument('--parent_dir', required=False, help='(Optional) if file is in parent directory')
    
    params = parser.parse_args()
    main(params)