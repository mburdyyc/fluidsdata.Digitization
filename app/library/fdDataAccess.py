#
# Python classes for processing PVT report into digitized records
#

#region Imports

import fitz                                             # https://pymupdf.readthedocs.io/en/latest/installation.html
import os                                               # https://docs.python.org/3/library/os.html
import pandas as pd                                     # https://pandas.pydata.org/docs/getting_started/index.html
import re                                               # https://docs.python.org/3/library/re.html
import streamlit as st                                  # https://docs.streamlit.io/library/get-started/installation
from datetime import datetime
from datetime import date                               # https://docs.python.org/3/library/datetime.html
from st_aggrid import AgGrid, GridOptionsBuilder        # https://github.com/PablocFonseca/streamlit-aggrid
from PIL import Image 
#import uuid    
from fuzzywuzzy import fuzz, process
from openpyxl import load_workbook
#import cv2  
#import pytesseract
#import matplotlib.pyplot as plt
import pandas as pd
import requests
import json
from st_aggrid import *
from azure.storage.filedatalake import DataLakeFileClient
from pdf2image import convert_from_bytes
import json
import base64
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pyodbc
from pyinstrument import Profiler 
from sqlalchemy import create_engine
import urllib
import zipfile
import tempfile
from azure.storage.blob import BlobClient
from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient,
    BlobSasPermissions,
    UserDelegationKey,
    generate_container_sas,
    generate_blob_sas
)
from datetime import timezone, timedelta
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os, io
import tempfile
from library import fdCommon
#endregion



    


#
# Class for CRUD operations with Azure backend
# ToDo: move to api
# ToDo: add error handling
class DataAccess():
    
    # create and cache sql alchemy connection
    # ToDo: remove streamlit dependency
    # ToDo: remove password auth
    file_system_name="fluidsdata"
    connection_string = "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
    # Set Azure Blob Storage credentials: to do: move to secret store
    container_name="fluidsdata"
    #AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
 
    @st.cache_resource(ttl=3600)    
    def get_SQLAlchemy_engine(_self):
        
        server = 'fluidsdatadev.database.windows.net'
        database = 'fluidsdata_dev'
        #authentication = 'Active Directory Password'  # AD authentication method
        driver = '{ODBC Driver 18 for SQL Server}'
        username = 'medwards'
        password = 'fd0Eppword!'

        conn = f"""Driver={driver};Server=tcp:{server},1433;Database={database};
                Uid={username};Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"""

        params = urllib.parse.quote_plus(conn)
        conn_str = 'mssql+pyodbc:///?autocommit=true&odbc_connect={}'.format(params)
        engine = create_engine(conn_str, echo=True)

        return engine #connection

    # get datframe from sql query
    def df_from_azure_sql(_self, query):
        
        print('SQL Query:', query)
        
        connection = _self.get_SQLAlchemy_engine()
        # Fetch data into a pandas DataFrame
        df = pd.read_sql(query, connection)
        # Define a regular expression pattern to match 'nan' exactly
        pattern = r'^nan$'

        # Use the pattern to find exact matches in the dataframe and replace them with np.nan
        df = df.replace(re.compile(pattern), np.nan)

        return df

    # create pyodbc connection
    # ToDo: deprecate pyodbc
    @st.cache_resource(ttl=3600)
    def get_pyodbc_connection(_self):
        server = 'fluidsdatadev.database.windows.net'
        database = 'fluidsdata_dev'
        #authentication = 'ActiveDirectoryPassword'  # AD authentication method
        driver = '{ODBC Driver 18 for SQL Server}'
        username = 'medwards'
        password = 'fd0Eppword!'

        # Create the connection string
        connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
        # Connect to the Azure SQL database
        connection = pyodbc.connect(connection_string)
        return connection
    
    # update Azure sql
    # ToDo: batch update is inefficient
    def update_azure_sql(_self, query, data='',many=False):
        print(['sql query=', query])
        connection = _self.get_pyodbc_connection()
        with connection.cursor() as cursor:
            if many == True:
                cursor.executemany(query, data)
            elif data != '':
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            cursor.commit()
        print('end query')

    # get images from PDF files in Azure storage
    def get_pdf_images_from_azure(_self, path, file_name):
        file_path = path + file_name
        service_client = DataLakeFileClient.from_connection_string(_self.connection_string, file_system_name=_self.file_system_name,file_path=file_path )
        #with open(file_name, 'wb') as  file: 
        data = service_client.download_file()
        downloaded_bytes = data.readall()    
        doc = fitz.open(stream=downloaded_bytes, filetype="pdf")
        return doc
    
        # get images from PDF files in Azure storage
    def get_pdf_file_from_azure(_self, path, file_name):
        file_path = path + file_name
        service_client = DataLakeFileClient.from_connection_string(_self.connection_string, file_system_name=_self.file_system_name,file_path=file_path )
        #with open(file_name, 'wb') as  file: 
        data = service_client.download_file()
        downloaded_bytes = data.readall()    
        
        return downloaded_bytes

    # get images from locally stored PDF files
    def get_pdf_images_from_local(_self, path, file_name):
        file_path = path + file_name 
        doc = fitz.open(file_path)
        return doc

    # get images from PDF files locally or in Azure
    def get_pdf_images(_self, path, file_name, source='local'):
        if source == 'local':
            doc = _self.get_pdf_images_from_local(path, file_name)
        else:
            doc = _self.get_pdf_images_from_azure(path, file_name)
        return doc
    
        # get images from PDF files in Azure storage
    def get_image_from_azure(_self, path, file_name, size):
        file_path = path + file_name
        service_client = DataLakeFileClient.from_connection_string(_self.connection_string, file_system_name=_self.file_system_name,file_path=file_path )
        # with service_client.open(file_name, "rb") as file:
        #     image_data = file.read()
        # return image_data
        #with open(file_name, 'wb') as  file: 
        data = service_client.download_file()
        image_data = data.readall()
            #downloaded_bytes = data.readall()    
        img = image_data#Image.frombytes("RGBA",data=downloaded_bytes,size=size)
        return img
    # get dataframe from csv file stored in Azure
    def get_csv_from_azure(_self, path, file_name):
        try:
            df = pd.read_csv(
                f"abfs://{_self.file_system_name}/{path}{file_name}",
                storage_options={
                    "connection_string": "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
                }, keep_default_na=False)
        except Exception as ex:
            print(f'get_csv_from_azure EXCEPTION: {ex}')
            df = pd.DataFrame()
        return df

    # get dataframe from csv file stored locally
    def get_csv_from_local(_self, path,filename):
        df = pd.read_csv(path + filename, keep_default_na=False)
        return df

    # write dataframe from csv file in Azure storage
    def write_csv_to_azure(_self, df, path, file_name):
        print('start csv write',path, file_name)
        df.to_csv(f"abfs://{_self.file_system_name}/{path}{file_name}",
            storage_options={
                "connection_string": "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
            },index=False)
        print('end csv write')
        return df

    # write dataframe from csv file in local storage
    def write_csv_to_local(_self, df, path, file_name):
        df.to_csv(path + file_name,index=False)
        return df

    # write dataframe from csv file in local or Azure storage
    def write_csv(_self, df, path, file_name,source = 'cloud'):
        if source == 'local':
            df = _self.write_csv_to_local(df, path, file_name)
        else:
            df = _self.write_csv_to_azure(df, path, file_name)
        return df

    # rename file in Azure storage
    def rename_azure_file(_self,path, old_filename, new_filename, new_path=''):
        
        connection_string = "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"

        container_name = _self.file_system_name
        blob_name = path + old_filename
        if new_path != '':
            new_blob_name = new_path + new_filename
        else:
            new_blob_name = path + new_filename
        print(f'renaming {blob_name} to {new_blob_name}')
        blob_service_client = BlobServiceClient.from_connection_string(_self.connection_string)
        blob_client = blob_service_client.get_blob_client(container_name, blob_name)
        new_blob_client = blob_service_client.get_blob_client(container_name, new_blob_name)

        # Copy the blob to the new name
        new_blob_client.start_copy_from_url(blob_client.url)

        # Delete the original blob
        blob_client.delete_blob()
        print("The blob is Renamed successfully:",{new_blob_name})

    # get dataframe from Excel file in Azure storage
    def get_excel_from_azure(_self, path, file_name):
        #debug(['excel read=',file_name])
        df = pd.read_excel(f"abfs://fluidsdata/DATA_DIGITIZATION_WORKFLOW/%s" % file_name,
            storage_options={
                "connection_string": "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
            })
        return df

    # get dataframe from Excel file in Azure storage
    def get_excel_from_local(_self, path, file_name):
        df = pd.read_excel(path + '/' +file_name, sheet_name="Sheet1")
        return df

    # get json data from file in Azure storage
    def get_json_from_azure(_self, path, file_name):
        file_path = path + file_name
        service_client = DataLakeFileClient.from_connection_string(_self.connection_string, file_system_name=_self.file_system_name,file_path=file_path )
        #with open(file_name, 'wb') as  file: 
        data = service_client.download_file()
        downloaded_bytes = data.readall()
        data = json.loads( downloaded_bytes)   
        #images = convert_from_bytes(downloaded_bytes)
        return data
    
        # get json data from file in Azure storage

    def write_json_to_azure(_self, path, file_name, json_str):
        file_path = path + file_name
        service_client = DataLakeFileClient.from_connection_string(_self.connection_string, file_system_name=_self.file_system_name,file_path=file_path )
            # Get a reference to the file system
        
        #file_system_client = service_client  .get_file_client(_self.file_system_name)

        # Create or get a reference to the file in Data Lake Storage Gen2
        #file_client = file_system_client.get_file_client(file_path)
        json_str = str.encode(json_str)
        # Upload the JSON data to the file
        with service_client.create_file() as file:
            file.append_data(json_str, offset=0, length=len(json_str))
            file.flush_data(len(json_str))     
    # def write_json_to_azure(_self, path, file_name, json_str):
    #     file_path = path + file_name
    #     service_client = DataLakeFileClient.from_connection_string(_self.connection_string, file_system_name=_self.file_system_name, file_path=path )
    #     directory_client = service_client.get_directory_client(path)
    #     file_client = directory_client.create_file(file_name)
    #     file_client.append_data(json_str, offset=0, length=len(json_str))
    #     file_client.flush_data(len(json_str))  
            


    # get json data from file in local storage
    def get_json_from_local(_self, path, file_name):
        file_path = path + file_name
        with open(file_path, 'r') as openfile:
            data = json.load(openfile)
        return data
    def zip_files(_self,zip_file_name, files_to_zip):
        with zipfile.ZipFile(zip_file_name, 'w') as zipf:
            for file in files_to_zip:
                zipf.write(file, os.path.basename(file))
    def uploadToBlobStorage(_self,path,filename, blobpath,data):
        container_name = _self.file_system_name
        local_file_path = path + filename
        
        blob_service_client = BlobServiceClient.from_connection_string(_self.connection_string)
        # Create a ContainerClient
        container_client = blob_service_client.get_container_client(container_name)
        
        # Upload the file
        # with open(local_file_path, "rb") as data:
        #     container_client.upload_blob(name=blobpath, data=data,overwrite=True)
        # files_to_zip = [local_file_path]
        # _self.zip_files('example.zip', files_to_zip)
        # filename = 'example.zip'
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blobpath+filename)
        with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding="utf-8") as temp_file:
            # Write the JSON string to the temporary file
            json.dump(data, temp_file, indent=4)
        #with open(temp_file,'rb') as file_data:
        #with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
        blob_client.upload_blob(json.dumps(data, temp_file, indent=4))   #(file_data,overwrite=True)
        print(f'Uploaded {filename}.')

def get_samples(dig, source=''):
    headers = {
        'Authorization': f'Bearer {dig.auth.token}',
        'Content-Type': 'application/json',
        'X-TenantID': dig.auth.tenant_id
        }
    url = 'https://fluids-data-api.azurewebsites.net/public/api/v1/samples'
    if source != '':
        url = f'{url}?source="{source}"'
    response = requests.get(url, headers=headers)
    if response.status_code > 299:
        if 'invalid token' in response.text:
            dig.auth.logged_in = False
            st.rerun()
        #with TABLE_SELECTOR:
        st.warning(f'API error {response.status_code}:{response.text} ')
    return response.text        

def upload_json_to_BlobStorage(filename, blobpath,data_dict):
    file_system_name="fluidsdata"
    connection_string = "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
            
    container_name = file_system_name
    #local_file_path = path + filename
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Create a ContainerClient
    container_client = blob_service_client.get_container_client(container_name)
    
    # Upload the file
    # with open(local_file_path, "rb") as data:
    #     container_client.upload_blob(name=blobpath, data=data,overwrite=True)
    # files_to_zip = [local_file_path]
    # _self.zip_files('example.zip', files_to_zip)
    # filename = 'example.zip'
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blobpath+filename)
    #with tempfile.NamedTemporaryFile(mode='w', delete=False, encoding="utf-8") as temp_file:
        # Write the JSON string to the temporary file
        #json.dump(data, temp_file, indent=4)
    #with open(temp_file,'rb') as file_data:
    #with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_file:
    blob_client.upload_blob(json.dumps(data_dict, indent=4),overwrite=True)   #(file_data,overwrite=True)
    print(f'Uploaded {filename}.')

    # Nanonets integration
def create_service_sas_blob(blob_client: BlobClient, account_key: str):
    # Create a SAS token that's valid for one day, as an example
    start_time = datetime.now(timezone.utc)
    expiry_time = start_time + timedelta(days=1)

    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time
    )

    return sas_token

# load previously saved data for a given report table
# ToDo: either replace with a generic dataframe loader or make more specific to loading saved dataframe
def load_saved_df(da, path,filename,source = 'local'):
    if source == 'local':
        df = da.get_csv_from_local(path, filename)
    else:
        df = da.get_csv_from_azure(path, filename)    
    return df

# load application configuration
def get_app_config(da, source='local'):
# Read configuration from json file
    if source == 'local':
        # ToDo: standardize local path for when VM deployed
        config = da.get_json_from_local("/Users/matthewburd/Python/DATA_DIGITIZATION_WORKFLOW/","config.json")
    else:
        config = da.get_json_from_azure("DATA_DIGITIZATION_WORKFLOW/","config.json")
    return config

def upload_to_azure_storage(path, filename, data, da):
        # Create a temporary directory to store PDF images
    TEMP_DIR = tempfile.mkdtemp()
        # Initialize Azure Blob Storage client
    blob_service_client = BlobServiceClient.from_connection_string(da.connection_string)
    container_client = blob_service_client.get_container_client(da.container_name)
    # Save the uploaded file to a temporary directory
    temp_file_path = os.path.join(TEMP_DIR, filename)
    with open(temp_file_path, "wb") as f:
        f.write(data)
    blob_client = container_client.get_blob_client(path + filename)
    with open(temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

def delete_test(url, auth):
    headers = {
        'Authorization': f'Bearer {auth.token}',
        'Content-Type': 'application/json',
        'X-TenantID': auth.tenant_id
        }
    response = requests.delete(url, headers=headers)
    print('create', url, response.status_code, response.text)
    return response.status_code, response.text




def post_test(test_json, url, auth):
    headers = {
        'Authorization': f'Bearer {auth.token}',
        'Content-Type': 'application/json',
        'X-TenantID': auth.tenant_id
        }
    data = {}
    data['Data'] = test_json
    body = json.dumps(data, indent=4, default=fdCommon.np_encoder)
    #body = json.loads(body)
    response = requests.post(url, headers=headers, data=body,)
    print('create', url, response.status_code, response.text)
    print(body)
    return response.status_code, response.text

def update_test(test_json, sample_id, test_id, endpoint, auth):
    delete_url = f'https://fluids-data-api.azurewebsites.net/public/api/v1/samples/{sample_id}/{endpoint}/{test_id}'
    response_code, response_text = delete_test(delete_url, auth)
    
    if response_code < 300:
        response_code, response_text = create_test(test_json, sample_id, endpoint, auth)
    return response_code, response_text

def create_test(test_json, sample_id, endpoint, auth):
    post_url = f'https://fluids-data-api.azurewebsites.net/public/api/v1/samples/{sample_id}/{endpoint}'
    response_code, response_text = post_test(test_json, post_url, auth)
    return response_code, response_text




def save_json(table_json, table_type, token, tenant_id, ID=''):
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'X-TenantID': tenant_id
            }
        #body = json.loads(table_json)
        print('saving',table_json)
        
        
        if table_type == 'Sample':
            url = 'https://fluids-data-api.azurewebsites.net/public/api/v1/samples'
        elif table_type == 'ConstantCompositionExpansionTest':
            url = 'https://fluids-data-api.azurewebsites.net/public/api/v1/samples'
        if ID == '':
            response = requests.post(url, headers=headers, data=table_json)
        else:
            url = url + f'/{ID}'
            response = requests.put(url, headers=headers, data=table_json)
        return response.status_code, response.text

def delete_sample(ID,token, tenant_id):
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'X-TenantID': tenant_id
            }
        #body = table_json
        
        url = f'https://fluids-data-api.azurewebsites.net/public/api/v1/samples/{ID}'
        
        response = requests.delete(url, headers=headers)
        return response.status_code, response.text
 
def list_blobs_in_path(path, da):
            # Initialize Azure Blob Storage client
    blob_service_client = BlobServiceClient.from_connection_string(da.connection_string)
    container_client = blob_service_client.get_container_client(da.container_name)
    # List all files in Azure Blob Storage container
    file_list = [blob.name for blob in container_client.list_blobs(name_starts_with=path)]
    file_list2 = []
    for f in file_list:
        file_list2.append(f.removeprefix(path))
    return file_list2

def get_asset_info(company, da):   

    path = f'DATA_DIGITIZATION_WORKFLOW/COMPANIES/{company.upper()}/CONFIG/'
    filename = 'assets.csv'
    df = da.get_csv_from_azure(path, filename)
    return df