import os
from zipfile import ZipFile
from datetime import date


def scrape_HC():
    """
    This function is used to scrape property data from HC
    """
    #Get data from website and save to current dir
    data_url = "https://gis.harnett.org/DataDownloads/Shapefile/TaxParcels.zip"
    os.system(f"wget {data_url}")
    print(f"Extracted data from: {data_url}")
    
    #Extract file from zip
    zip_name = "TaxParcels.zip"
    file_name = "TaxParcels.dbf"
    
    with ZipFile(zip_name, 'r') as zip:
        print(f"Extracting: {file_name}")
        zip.extract(file_name)
    
    #Remove original zip file from dir
    print("Removing zip file from dir")
    os.system(f"rm ./{zip_name}")
    
    #Rename dataset to specify day of collect
    export_name = f"{date.today()}_HC_raw.dbf"
    os.system(f"mv ./{file_name} ./data/{export_name}")
    
    print("Scrape Complete")

def scrape_JC():
    """
    This function is used to scrape property data from JC
    """
    #Get data from website and save to current dir
    data_url = "https://www.johnstonnc.com/files/gis/files/Tax_Assessment_Attributes.zip"
    os.system(f"wget {data_url}")
    print(f"Extracted data from: {data_url}")
    
    #Extract file from zip
    zip_name = "Tax_Assessment_Attributes.zip"
    file_name = "taxatt.xlsx"
    
    with ZipFile(zip_name, 'r') as zip:
        print(f"Extracting {file_name}")
        zip.extract(file_name)
        
    #Remove original zip file from dir
    print("Removing zip file from dir")
    os.system(f"mv ./{zip_name}")
    
    #Rename dataset to specify day of collect
    export_name = f"{date.today()}_JC_raw.xlsx"
    os.system(f"mv ./{file_name} ./data/{export_name}")
    
    print("Scrape Complete")

def scrape_WC():
    """
    This function is used to scrape property data from WC
    """
    #Get data from website and save to current dir
    data_url = "https://services.wakegov.com/realdata_extracts/RealEstData06232022.txt"
    os.system(f"wget {data_url}")
    print(f"Extracted data from: {data_url}")

    file_name = data_url.rsplit('/', 1)[1]
    
    #Rename dataset to specify day of collect
    export_name = f"{date.today()}_HC_raw.txt"
    os.system(f"mv ./{file_name} ./data/{export_name}")
    
    print("Scrape Complete")

if __name__ == "__main__":
    print("Scraping HC ...")
    scrape_HC()

    print("Scraping JC ...")
    scrape_JC()

    print("Scraping WC ...")
    scrape_WC()