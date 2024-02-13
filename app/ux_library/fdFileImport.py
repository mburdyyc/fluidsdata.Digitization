import tempfile
import streamlit as st
from library import fdDataAccess
from library import fdReport
from library import fdCommon
from library import fdMapping
import random
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
import pandas as pd

def upload_files(dig, uploaded_files):  
    try:
        i = 0
        file_list = []
        for uploaded_file in uploaded_files:
            if uploaded_file:
                    fdDataAccess.upload_to_azure_storage(dig.uploaded_pdf_path, uploaded_file.name, uploaded_file.read(), dig.da)
                    print(f'Uploaded {uploaded_file.name} to Azure')
                    i = i+1
                    file_list.append(uploaded_file.name)
        return 'Success', None, file_list
    except Exception as e:
        print(f"Unexpected {e=}, {type(e)=}")
        return 'Error', e, file_list
    
def process_uploaded_pdf_file(dig,filename):

    pdfpath = dig.uploaded_pdf_path + filename
    data = dig.da.get_pdf_file_from_azure(dig.uploaded_pdf_path, filename)

    file_system_name="fluidsdata"
    connection_string = "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
    
    blob_client = BlobClient.from_connection_string(conn_str=connection_string, container_name=file_system_name, blob_name=pdfpath)
    sas_token = fdDataAccess.create_service_sas_blob(blob_client=blob_client, account_key='3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==')
    sas_url = f"{blob_client.url}?{sas_token}"
    
    url = 'https://app.nanonets.com/api/v2/OCR/Model/7a98fa65-db31-4b4e-b632-41b742bed9ac/LabelUrls/?async=false'

    headers = {
        'accept': 'application/x-www-form-urlencoded'
    }
    data = {'urls' : [sas_url]}
    response = requests.request('POST', url, headers=headers, auth=requests.auth.HTTPBasicAuth('22737e63-b835-11ed-98d9-2a5198997ee4', ''), data=data)
    if response.status_code == 200:
        file_system_name="fluidsdata"
        json_path = dig.json_input_path + filename.split('.')[0] + '.json'
        blob = BlobClient.from_connection_string(conn_str=connection_string, container_name=file_system_name, blob_name=json_path)
        data = response.text        
        blob.upload_blob(data,overwrite=True)
        return response.status_code, 'Success'
    else:
        return response.status_code, response.text

def do_file_import_flow(dig, cfg):
    # Create a temporary directory to store PDF images
    TEMP_DIR = tempfile.mkdtemp()
    # Initialize Azure Blob Storage client
    blob_service_client = BlobServiceClient.from_connection_string(dig.da.connection_string)
    container_client = blob_service_client.get_container_client(dig.da.container_name)


    # Upload PDF files
    if 'uploader_key' not in st.session_state:
        st.session_state['uploader_key'] ='0'
    st.warning('File upload is currently not working on cloud version\nMove the file you want to process from the processed list to the uploaded list')
    uploaded_files = st.file_uploader("Upload one or more PDF files", accept_multiple_files=True,key=st.session_state.uploader_key,type=['pdf'])

    if uploaded_files:
        status, error, uploaded_file_list = upload_files(dig, uploaded_files)
        if len(uploaded_file_list) > 0:
            st.session_state.uploader_key = str(random.random()) # generate a new key each time to clear the widget's selection list
        if status == 'Error':
            st.warning(f'Error loading files: {type(error)}')
        


    # Display a table of uploaded files
    st.subheader("Uploaded Files")

    # Create a DataFrame with a column of select boxes
    uploaded_files_list = fdDataAccess.list_blobs_in_path(dig.uploaded_pdf_path, dig.da)
    uploaded_files_df = pd.DataFrame({'Select File': False, 'File Name': uploaded_files_list})

    UPLOADED = st.container(border=True)
    with UPLOADED:
        UPLOADED_LIST, UPLOADED_COMMANDS = st.columns([1,1])
    with UPLOADED_LIST:
        st.data_editor(uploaded_files_df, key='uploaded_files2')
    with UPLOADED_COMMANDS:
        row_nums = []
        for key in st.session_state.uploaded_files2['edited_rows']:
            if st.session_state.uploaded_files2['edited_rows'][key]['Select File'] == True:
                row_nums.append(key)
        skip_ocr = st.checkbox('Skip OCR step', value=True)
        if st.button('Process File(s)'):
            if len(row_nums) > 0:
                with st.spinner('Processing uploaded files'):
                    for r in row_nums:
                        pdf_filename = uploaded_files_df.iloc[r]['File Name']
                        if skip_ocr == False:
                            st.write(f'OCR processing on {pdf_filename}')
                            status_code, response_text = process_uploaded_pdf_file(dig, pdf_filename)
                        if skip_ocr or (status_code == 200):
                            try:
                                st.write(f'creating report object')
                                report_name = pdf_filename.split('.')[0]
                                company = dig.auth.tenant_name.upper()
                                dig.report_obj = fdReport.ReportData(company, report_name, 'pdf', f'{report_name}.pdf')  
                                json_filename = f'{report_name}.json'
                                
                                st.write(f'loading OCR output')
                                dig.report_obj.load_raw_json(json_filename)
                                
                                st.write(f'processing OCR output')
                                dig.report_obj.process_raw_json()
                                num_tables = len(dig.report_obj.tables)
                                
                                st.write(f'identified {num_tables} tables')
                                progress_text='predicting table types'
                                my_bar = st.progress(0, text=progress_text)
                                table_num = 1
                                for t in dig.report_obj.tables:
                                    t.table_data_raw = t.table_data_raw.drop(columns=fdCommon.find_empty_columns(t.table_data_raw),axis=1)
                                    t.table_data_edited = t.table_data_edited.drop(columns=fdCommon.find_empty_columns(t.table_data_edited),axis=1)
                                    t.predicted_table_type, predicted_row_nums, t.predicted_transposed, t.predicted_header_rows = fdMapping.predict_table_type(t.table_data_edited, t.table_type,dig, cfg)
                                    my_bar.progress(table_num/num_tables, text=progress_text)
                                    table_num = table_num + 1
                                
                                st.write('saving report object')
                                fdReport.save(dig.report_obj, dig)
                                dig.da.rename_azure_file(dig.uploaded_pdf_path, pdf_filename,pdf_filename, dig.processed_pdf_path)
                                
                                st.success('Done!')
                                st.rerun()
                            except Exception as e:
                                st.error(f'file processing failed: {str(e)}')
                        else:
                            st.error(f'OCR conversion failed: {response_text}')
            else:
                st.info('Select one or more files')

    processed_files_list = fdDataAccess.list_blobs_in_path(dig.processed_pdf_path,dig.da)
    dig.file_list = processed_files_list
    processed_files_df = pd.DataFrame({'Select File': False, 'File Name': processed_files_list})
    PROCESSED = st.container(border=True)
    with PROCESSED:
        PROCESSED_LIST, PROCESSED_COMMANDS = st.columns([1,1])
    with PROCESSED_LIST:
        st.subheader("Processed Files")
        processed_files2 = st.data_editor(processed_files_df, key='processed_files2')
    with PROCESSED_COMMANDS:
        row_nums = []
        for key in st.session_state.processed_files2['edited_rows']:
            if st.session_state.processed_files2['edited_rows'][key]['Select File'] == True:
                row_nums.append(key)
        if st.button('Move to Uploaded List for re-processing'):
            for r in row_nums:
                pdf_filename = processed_files_df.iloc[r]['File Name']
                dig.da.rename_azure_file(dig.processed_pdf_path, pdf_filename,pdf_filename, dig.uploaded_pdf_path)
                st.rerun()

