import pandas as pd
import numpy as np
import json

from datetime import datetime
 
import uuid
from library import fdCommon
from library import fdMapping
from library import fdDataAccess
from library import fdNormalization
from library import fdReport

# this class holds all information about a table extracted from the original report
# includes: raw, edited, and mapped versions of the data, the table column mappings that were used, etc.
class Table:
    def __init__(self, report, page, report_table_number, page_table_number, table_labels=[], table_notes=[],
                 table_type='Unknown', table_status='New', id = None,
                 table_data_raw=None, table_data_edited=None, table_data_mapped=None, header_data_mapped=None,header_data_normalized=None, 
                 table_data_normalized=None, table_data_saved=None, column_mapping=[], copied_from=None, split_from=None, split_to=None, edit_log = []):
        
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        self.report = report
        self.page = page
        self.report_table_number = report_table_number
        self.page_table_number = page_table_number
        self.table_labels = table_labels
        self.table_notes = table_notes
        self.table_type = table_type
        self.table_status = table_status
        self.table_data_raw = table_data_raw
        self.table_data_edited = table_data_edited
        self.table_data_mapped = table_data_mapped
        self.header_data_mapped = header_data_mapped
        self.table_data_normalized = table_data_normalized
        self.header_data_normalized = header_data_normalized
        try:
            if self.table_data_normalized.empty: #normalized should not be a dataframe
                self.table_data_normalized = None
        except:
            pass
        self.table_data_saved = table_data_saved
        self.table_json = {}
        self.column_mapping = column_mapping
        self.table_column_mappings = []
        self.table_header_mapping = None
        self.copied_from = copied_from
        self.split_from = split_from
        self.split_to = split_to
        self.edit_log = edit_log
        self.predicted_table_type = ''
        self.header = ''
        
        self.predicted_header = ''
        
        self.transposed = False
        self.predicted_transposed = False
        self.validation_errors = []
        
        
        

    # def display_table_info(self):
    #     print(f"Report: {self.report}")
    #     print(f"Page: {self.page}")
    #     print(f"ID: {self.id}")
    #     print(f"Report Table Number: {self.report_table_number}")
    #     print(f"Page Table Number: {self.page_table_number}")

    #     print(f"Table Labels: {self.table_labels}")
    #     print(f"Table Notes: {self.table_notes}")
    #     print(f"Table Type: {self.table_type}")
    #     print(f"Table Status: {self.table_status}")
    #     # Additional print statements to display DataFrame info
    #     print(f"Table Data Raw: {self.table_data_raw}")
    #     print(f"Table Data Edited: {self.table_data_edited}")
    #     print(f"Table Data Mapped: {self.table_data_mapped}")
    #     print(f"Table Data Mapped: {self.header_data_mapped}")
    #     print(f"Table Data Normalized: {self.table_data_normalized}")
    #     print(f"Header Data Normalized: {self.header_data_normalized}")
    #     print(f"Table Data Saved: {self.table_data_saved}")
    #     print(f"Column Mapping: {self.column_mapping}")
    #     print(f"Mapped Columns: {self.table_column_mapping}")
    #     print(f"Mapped Headers: {self.table_header_mapping}")
    #     print(f"Copied from: {self.copied_from}")
    #     print(f"Split from: {self.split_from}")
    #     print(f"Split to: {self.split_to}")
    #     print(f"Edit Log: {self.edit_log}")
    #     print(f"Predicted Table Type: {self.predicted_table_type}")
    #     print(f"Header: {self.header}")
        
    #     print(f"Predicted Header: {self.header}")
    #     print(f"Validation Errors: {self.validation_errors}")
       



        
# this class holds all information about a report
# includes: all tables, all extracted text, report meta data
class ReportData:
    def __init__(self, company, report_name, source, filename_pdf, fluid_type='Unknown',
                 asset=None, field=None, reservoir=None, well=None, 
                 lab=None, report_data=None, extracted_text=None,num_pages=None, num_tables=None, filename_json=None, 
                 status=None, raw_json=None, json_processed=None, 
                 samples=None, updated=None):
        
        self.company = company
        self.report_name = report_name
        self.source = source
        self.filename_pdf = filename_pdf
        self.fluid_type = fluid_type
        self.asset = asset
        self.field = field
        self.reservoir = reservoir
        self.well = well
        self.lab = lab
        self.fluid_type
        self.report_data = report_data
        self.extracted_text = extracted_text
        self.num_pages = num_pages
        self.num_tables = num_tables
        self.filename_json = filename_json
        self.status = status
        self.raw_json = raw_json
        self.json_processed = json_processed
        self.samples = pd.DataFrame()
        self.tables = []
        self.tests = {}
        self.updated = updated if updated else datetime.now()

def display_report_info(report):
    print(f"Company: {report.company}")
    print(f"Report Name: {report.report_name}")
    print(f"Source: {report.source}")
    print(f"PDF Filename: {report.filename_pdf}")
    print(f"Fluid Type: {report.fluid_type}")
    print(f"Asset: {report.asset}")
    print(f"Field: {report.field}")
    print(f"Reservoir: {report.reservoir}")
    print(f"Well: {report.well}")
    print(f"Lab: {report.lab}")
    print(f"Fluid Type: {report.fluid_type}")
    print(f"Report Data: {report.report_data}")
    print(f"Extracted Text: {report.extracted_text}")
    print(f"Number of Pages: {report.num_pages}")
    print(f"Number of Tables: {report.num_tables}")
    print(f"JSON Filename: {report.filename_json}")
    print(f"Status: {report.status}")
    print(f"Raw JSON: {report.raw_json}")
    print(f"Processed JSON: {report.json_processed}")
    print(f"Samples: {report.samples}")
    print(f"Tables: {report.tables}")
    print(f"Updated: {report.updated}")

def load_report_object(selected_file, dig, cfg):
    report_name = selected_file.split('.')[0]
    company = dig.auth.tenant_name.upper()
    dig.report_obj = ReportData(company, report_name, 'pdf', f'{report_name}.pdf')  
    
    restored = restore(dig.report_obj, dig, cfg)
    if not restored:
        filename = f'{report_name}.json'
        # if company == 'ADNOC':
        #     process_raw_nanonets(dig.report_obj,dig)
        # else:
        load_raw_json(dig.report_obj, filename, dig)
        process_raw_json(dig.report_obj)
        fdReport.save(dig.report_obj, dig)

    for t in dig.report_obj.tables:
        t.table_data_raw = t.table_data_raw.drop(columns=fdCommon.find_empty_columns(t.table_data_raw),axis=1)
        t.table_data_edited = t.table_data_edited.drop(columns=fdCommon.find_empty_columns(t.table_data_edited),axis=1)


def load_raw_json(report, filename, dig):
    report.filename_json = filename

    path=f'DATA_DIGITIZATION_WORKFLOW/COMPANIES/{report.company.upper()}/REPORT_FILES_JSON/'
    try:
        report.raw_json = dig.da.get_json_from_azure(path,filename)
        print(f"File '{filename}' loaded into 'raw_json' successfully.")
    except FileNotFoundError:
        print(f"File '{path}{filename}' not found.")
    

def get_raw_json(report):
    json_obj = report.raw_json#json.loads(report.raw_json)
    return json_obj

def process_raw_json(report):
    raw_json_obj = report.raw_json#report.get_raw_json()
    if raw_json_obj is None:
        print('No raw json to process')
        return
    result_list = raw_json_obj.get('result',[])
    if len(result_list) == 0:
        print('No returns to process')
        return
    page = -1
    table_num = 0
    page_table_num = 0
    tables = []
    extracted_text = []
    for r in result_list:
        prediction_list = r.get('prediction',[])
        if len(prediction_list) == 0:
            print('No predictions to process')
            print(r)
        else:
            table_labels = []
            table_notes = []
            for p in prediction_list:
                #extracted_text.append((p['page_no'],p['ocr_text'],p['xmin'],p['ymin'],p['xmax'],p['ymax']))
                if p['label'] in ['TableName','Tabular_Data_Label']:
                    table_labels.append(p['ocr_text'])
                    extracted_text.append((p['page_no'],p['ocr_text'],p['xmin'],p['ymin'],p['xmax'],p['ymax']))
                if p['label'] == 'TableNotes':
                    table_notes.append(p['ocr_text'])
                    extracted_text.append((p['page_no'],p['ocr_text'],p['xmin'],p['ymin'],p['xmax'],p['ymax']))
                if p['type'] == 'table':
                    # Extracting the 'cells' data from the JSON
                    cells_data = p.get('cells', [])

                    # Create a dictionary to hold the data for each cell
                    data = {'row': [], 'col': [], 'text': []}

                    # Iterate through the 'cells' data to populate the dictionary
                    for cell in cells_data:
                        extracted_text.append((p['page_no'],cell['text'],cell['xmin'],cell['ymin'],cell['xmax'],cell['ymax']))
                        data['row'].append(cell['row'])
                        data['col'].append('col'+ str(cell['col']))
                        data['text'].append(cell['text'])

                    # Create a DataFrame
                    df = pd.DataFrame(data)

                    # Pivot the DataFrame to arrange values in rows and columns
                    df_pivot = df.pivot_table(index='row', columns='col', values='text', aggfunc='first')
                    
                    if page != p['page_no']:
                        page_table_num = 0
                        page = p['page_no']
                    print('create table',table_num,p['id'])
                    table_obj = Table(report.report_name,page,table_num,page_table_num, table_data_raw=df_pivot,table_data_edited=df_pivot,table_labels=table_labels,table_notes=table_notes)
                    tables.append(table_obj)
                    table_num += 1
                    page_table_num += 1
    
    report.num_pages = page+1
    report.num_tables = table_num
    report.tables = tables
    #dig.report_obj = dig.report_obj
    patterns = {
        r'\u00b0C|\u00b0\s*C': 'degC',
        r'\u00b0F|\u00b0\s*F': 'degF',
        r'\u00b0K|\u00b0\s*K': 'degK'
    }

    # report.extracted_text['pattern_exists'] = report.extracted_text['text'].str.contains(pattern)
    # print(report.extracted_text[report.extracted_text['pattern_exists']])
    
    report.extracted_text = pd.DataFrame(extracted_text,columns=['page','text','x1', 'y1', 'x2', 'y2'])
    report.extracted_text['text'] =  report.extracted_text['text'].replace(patterns, regex=True)
    #dig.report_obj.extracted_text = report.extracted_text

def extract_text_from_raw_json(report):
    raw_json_obj = report.raw_json#report.get_raw_json()
    if raw_json_obj is None:
        print('No raw json to process')
        return
    result_list = raw_json_obj.get('result',[])
    if len(result_list) == 0:
        print('No returns to process')
        return
    extracted_text = []
    for r in result_list:
        prediction_list = r.get('prediction',[])
        if len(prediction_list) == 0:
            print('No predictions to process')
            print(r)
        else:
            table_labels = []
            table_notes = []
            for p in prediction_list:
                extracted_text.append((p['page_no'],p['ocr_text']))
                if p['label'] in ['TableName','Tabular_Data_Label']:
                    table_labels.append(p['ocr_text'])
                    extracted_text.append((p['page_no'],p['ocr_text']))
                if p['label'] == 'TableNotes':
                    table_notes.append(p['ocr_text'])
                    extracted_text.append((p['page_no'],p['ocr_text']))
                if p['type'] == 'table':
                    # Extracting the 'cells' data from the JSON
                    cells_data = p.get('cells', [])

                    
                    # Iterate through the 'cells' data to populate the dictionary
                    for cell in cells_data:
                        extracted_text.append((p['page_no'],cell['text']))
                        

    report.extracted_text = pd.DataFrame(extracted_text,columns=['page','text','x1', 'y1', 'x2', 'y2'])
    #dig.report_obj.extracted_text = report.extracted_text

# load raw text extracted from PVT report. 
# ToDo: Currently text extraction is a done with a separate batch utility. Add extraction capability to this app, e.g. if data hasn't already been extracted

def load_extracted_file_text_df(path,filename,dig,source = 'cloud'):
    print(f'extracting data for {filename}')
    df = pd.DataFrame()
    try:
        if source == 'local':
            df = dig.da.get_csv_from_local(path, filename)
        else:
            df = dig.da.get_csv_from_azure(path, filename)
        if df is None:
            df = pd.DataFrame(columns=(['page','text','x1','y1','x2','y2']))
        dig.report_obj.extracted_text = df
        print(f"data length {len(dig.report_obj.extracted_text.index)}")
    except Exception as ex:
        print(f'load_extracted_file_text_df EXCEPTION: {ex}')
    return df
def get_report_data_from_excel(filename, dig,page=-1):   
    query = f"select * from dbo.nanonets_data where original_filename = '{filename}'"
    if page > -1:
        query = query + f' and page = {page}'
    query = query
    print('query=',query)
    df = dig.da.df_from_azure_sql(query)
    return df
def process_raw_nanonets(report, dig):
    #filepath = f'DATA_DIGITIZATION_WORKFLOW/COMPANIES/{report.company.upper()}/REPORT_FILES_EXCEL/'
    filename = report.filename_pdf
    raw_excel = get_report_data_from_excel(filename, dig)
    
    if (raw_excel is None) or (raw_excel.empty):
        print('No raw excel data to process')
        return
    
    
    page = -1
    table_num = 0
    page_table_num = 0
    tables = []
    extracted_text = pd.DataFrame()
    # Grouping the data by 'page' and iterating through the groups
    first_row = raw_excel.iloc[0]
    report.asset = first_row['AssetName']
    report.field = first_row['FieldName']
    report.reservoir = first_row['ReservoirName']
    report.well = first_row['WellName']
    report.lab = first_row['LaboratoryName']
    
    for page, page_data in raw_excel.groupby('page'):
        # For each page, iterate through unique table numbers
        for page_table_number in page_data['table_number'].unique():
            # Create a subset DataFrame for each table
            table_df = page_data[page_data['table_number'] == max(page_table_number,0)]
            table_df = table_df.drop(['original_filename', 'page', 'LaboratoryName','ReportUID', 'AssetName', 'FieldName', 'ReservoirName', 'WellName', 'ContainerID', 'nanonets_data_pk_id','table_number'], axis=1)
            
            table_df = table_df.reset_index(drop=True)
            print(f'creating table {table_num} on page {page}')

            tables.append(Table(report.report_name,int(page),table_num,int(page_table_number)-1, table_data_raw=table_df, table_data_edited=table_df))
            table_num += 1
            #page_table_num += 1
    
        report.num_pages = int(page)+1
    report.num_tables = table_num
    report.tables = tables
    tenant = dig.dig.auth.tenant_name.upper()
    extracted_text_file_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/EXTRACTED_PDF_TEXT/"
    report.extracted_text = load_extracted_file_text_df(extracted_text_file_path, filename.split('.')[0] + '.csv', dig, source='')
    print('report object created')


def get_page_table(report,page, table_no):
    page_table = None
    for table in report.tables:
        #print(table)
        if ((table.page == page) & (table.page_table_number == table_no)):
            page_table = table
            break
    if page_table is not None:
        return table
    else:
        return None

def get_page_tables(report,page):
    tables = [table for table in report.tables if (table.page == page)]
    return tables

def get_report_dict(report):
        
        tables = []
        table_column_mappings = []
        for t in report.tables:
            for c in t.table_column_mappings:
                table_column_mappings_dict = {
                    "original_column": c.original_column,
                    "edited_column": c.edited_column,
                    "predicted_column": c.predicted_column,
                    "mapped_column": c.mapped_column,
                    "original_uom": c.original_uom,
                    "predicted_uom":c.predicted_uom,
                    "mapped_uom": c.mapped_uom,
                    "std_uom": c.std_uom,
                    "has_uom": c.has_uom,
                    "uom_dimension": c.uom_dimension    
                }
                table_column_mappings.append(table_column_mappings_dict)
            table_dict = {
                "report": t.report,
                "page": t.page,
                "report_table_number": t.report_table_number,
                "page_table_number": t.page_table_number,
                "table_labels": t.table_labels,
                "table_notes": t.table_notes,
                "table_type": t.table_type,
                "table_status": t.table_status,
                "table_data_raw": json.loads(t.table_data_raw.to_json(orient='records')) if t.table_data_raw is not None else None,
                "table_data_edited": json.loads(t.table_data_edited.to_json(orient='records')) if t.table_data_edited is not None else None,
                "table_data_mapped": json.loads(t.table_data_mapped.to_json(orient='records')) if t.table_data_mapped is not None else None,
                "header_data_mapped": json.loads(t.header_data_mapped.to_json(orient='records')) if t.header_data_mapped is not None else None,
                "table_data_normalized": json.loads(t.table_data_normalized.to_json(orient='records')) if t.table_data_normalized is not None else None,
                "header_data_normalized": json.loads(t.header_data_normalized.to_json(orient='records')) if t.header_data_normalized is not None else None,
                "table_data_saved": json.loads(t.table_data_saved.to_json(orient='records')) if t.table_data_saved is not None else None,
                "table_json": t.table_json,
                "table_column_mappings": table_column_mappings,
                "table_header_mapping": json.loads(t.table_header_mapping.to_json(orient='records')) if t.table_header_mapping is not None else None,
                #"column_mapping": json.loads(t.column_mapping.to_json(orient='records')) if t.column_mapping is not None else None,
                "copied_from": t.copied_from,
                "split_from": t.split_from,
                "split_to": t.split_to,
                "edit_log": t.edit_log,
                "predicted_table_type": t.predicted_table_type,
                "header": t.header,
                "predicted_header": t.predicted_header,
                "transposed": t.transposed,
                "predicted_transposed": t.predicted_transposed,
                "validation_errors": t.validation_errors
            }
            print(t.table_data_raw)
            tables.append(table_dict)
        report_dict = {
            "company": report.company,
            "report_name": report.report_name,
            "source": report.source,
            "filename_pdf": report.filename_pdf,
            "fluid_type": report.fluid_type,
            "asset": report.asset,
            "field": report.field,
            "reservoir": report.reservoir,
            "well": report.well,
            "lab": report.lab,
            "fluid_type": report.fluid_type,
            "report_data": report.report_data,
            "num_pages": report.num_pages,
            "num_tables": report.num_tables,
            "filename_json": report.filename_json,
            "status": report.status,
            #"raw_json": report.raw_json,
            "json_processed": report.json_processed,
            "samples": json.loads(report.samples.to_json(orient='records')) if report.samples is not None else None,
            "tables": tables,
            "extracted_text":json.loads(report.extracted_text.to_json(orient='records')) if report.extracted_text is not None else None,
            "updated": str(report.updated)
        }
        return report_dict

def get_report_samples(report,dig, cfg):
    response = fdDataAccess.get_samples(dig,source=report)
    existing_samples = json.loads(response)
    legacy_samples = False
    if len(existing_samples['data']['samples']) > 0:
        dig.report_obj.samples = pd.DataFrame.from_dict(existing_samples['data']['samples'])
        samples_df = dig.report_obj.samples#pd.DataFrame.from_dict(dig.report_obj.samples)
        samples_df.columns = [col[0].upper() + col[1:] for col in samples_df.columns]
    # elif (dig.report_obj.samples is not None) and (len(dig.report_obj.samples) > 0):
    #     samples_df = pd.DataFrame.from_dict(dig.report_obj.samples)
    else:
        legacy_samples = False
        query = f"""
        Select * from dbo.pvt_table_status
        where report = '{report+'.pdf'}' and
        Tablename = 'Sample' """
        df = dig.da.df_from_azure_sql(query)
        
        samples_df = pd.DataFrame(columns=['SampleID', 'FluidSampleID', 'FluidSampleContainerID'])
        
        for r, row in df.iterrows():
            filename = row['Report'] + '_' + str(row['Page'])+ '_' + str(row['TableNumber']) + '_' + row['Tablename'] + '.csv'
            path = dig.csv_output_path
            sample_df = dig.da.get_csv_from_azure(path, filename)

            if len(samples_df.index) == 0:
                samples_df = sample_df
            else:
                samples_df = pd.concat([samples_df,sample_df])
    
        samples_df = samples_df.replace(np.nan,None)
        samples_df = samples_df.reset_index(drop=True)
        sample_index = 1

        for i in range(len(samples_df.index)):
            if samples_df.iloc[i]['FluidSampleID'] == '':
                samples_df.at[i,'FluidSampleID'] = sample_index
                sample_index = sample_index + 1
            if samples_df.iloc[i]['FluidSampleContainerID'] == '':
                samples_df.at[i,'FluidSampleContainerID'] = 'unknown'
        samples_df['FluidSampleID'] = samples_df['FluidSampleID'].map(str)
        samples_df['FluidSampleContainerID'] = samples_df['FluidSampleContainerID'].map(str)
        samples_df['SampleID'] = samples_df['FluidSampleID'].map(str) + ':' + samples_df['FluidSampleContainerID'].map(str)

        try:
            filename = report + '.pdf_Manual_Samples.csv'
            manual_samples_df = dig.da.get_csv_from_azure(dig.csv_output_path, filename)
            samples_df = pd.concat([manual_samples_df])
        except:
            print('No manual samples')
        if samples_df.empty == False:
            if 'FluidSampleID' not in samples_df.columns:
                samples_df['FluidSampleID'] = range(len(samples_df.index))
                samples_df['FluidSampleID'] = samples_df['FluidSampleID'].map(str)
            if 'FluidSampleContainerID' not in samples_df.columns:
                samples_df['FluidSampleContainerID'] = 'unknown'
            samples_df = samples_df.sort_values(by='FluidSampleID')
            samples_df = samples_df.reset_index(drop=True)
            if 'ID' in samples_df.columns:
                # Move the specified column to the end
                samples_df = samples_df[[col for col in samples_df.columns if col != 'ID'] + ['ID']]
            dig.report_obj.samples = samples_df

            if samples_df.empty == False:
                samples_df = fdNormalization.normalize_column_df(samples_df, 'Sample', cfg)
                fdDataAccess.save_sample(samples_df)
            response = fdDataAccess.get_samples(dig, source=report)
            existing_samples = json.loads(response)
        
            if len(existing_samples['data']['samples']) > 0:
                dig.report_obj.samples = pd.DataFrame.from_dict(existing_samples['data']['samples'])
                samples_df = dig.report_obj.samples#pd.DataFrame.from_dict(dig.report_obj.samples)
                samples_df.columns = [col[0].upper() + col[1:] for col in samples_df.columns]



    if samples_df.empty == False:
        # Group by the common column ('ID' in this case) and concatenate values in other columns
        #samples_df = samples_df.groupby(['FluidSampleID','FluidSampleContainerID'], as_index=False).agg(lambda x: choose_first_non_none(x))
        # samples_df = samples_df.groupby(', as_index=False).agg(lambda x: choose_first_non_none(x))
        #re-order columns
        #samples_df.insert(0,'SampleID',samples_df.pop('SampleID'))
        samples_df.insert(1,'FluidSampleID',samples_df.pop('FluidSampleID'))
        samples_df.insert(2,'FluidSampleContainerID',samples_df.pop('FluidSampleContainerID'))

        samples_df = samples_df.reset_index(drop = True)
        for s, sample in samples_df.iterrows():
            if sample['FluidSampleContainerID'] == '':
                samples_df.at[s,'FluidSampleContainerID'] = 'Unknown'
            if sample['FluidSampleID'] == '':
                samples_df.at[s,'FluidSampleID'] = str(s+1)#workaround for manually added samples:
        #filename = report + '_Manual_Samples.csv'
        if samples_df.empty == False:
            samples_df = samples_df.sort_values(by='FluidSampleID')
            #samples_df = samples_df[[col for col in samples_df.columns if col != 'ID'] + ['ID']]
            samples_df = samples_df.reset_index(drop=True)
        
    dig.report_obj.samples = samples_df#.to_dict(orient='records')
    return samples_df    

def restore(report_obj,dig, cfg):
    tenant = dig.auth.tenant_name.upper()
    object_store_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/OBJECT_STORE/"
    filename = report_obj.report_name + '_backup.json'# Opening JSON file
    filepath = object_store_path + filename
    try:
        # returns JSON object as a dictionary
        data = dig.da.get_json_from_azure(object_store_path,filename)
        # f = open(filepath)

        # data = json.load(f)
    except FileNotFoundError:
        print(f"backup file {filepath} not found")
        return False
    except Exception as e:
        print(f"restore from {filepath} failed")
        print(e)
        return False

    
    table_data = data.get("tables",[])
    tables = []
    for t in table_data:
        report = t["report"]
        page = t["page"]
        try:
            id = t['id']
        except:
            id = str(uuid.uuid4())
        report_table_number = t["report_table_number"]
        page_table_number = t["page_table_number"]

        table = Table(report, page, report_table_number, page_table_number, id)#,table_type, table_status,table_data_raw, table_data_edited, table_data_mapped,table_data_normalized, table_data_saved, column_mapping)
        table.table_type = t["table_type"]
        table.table_status = t["table_status"]
        table.table_data_raw = pd.DataFrame(t["table_data_raw"])
        table.table_data_edited = pd.DataFrame(t["table_data_edited"])
        table.table_data_mapped = pd.DataFrame(t["table_data_mapped"])
        table.header_data_mapped = pd.DataFrame(fdCommon.try_get(t,"header_data_mapped",pd.DataFrame()))
        
        table.table_data_normalized = fdCommon.try_get(t,"table_data_normalized",pd.DataFrame())
        if not isinstance(table.table_data_normalized,pd.DataFrame): #was previously a string, protect against old saved values
            table.table_data_normalized = pd.DataFrame()
        table.header_data_normalized = fdCommon.try_get(t,"header_data_normalized",pd.DataFrame())
        if not isinstance(table.header_data_normalized,pd.DataFrame): #was previously a string, protect against old saved values
            table.header_data_normalized = pd.DataFrame()
        table.table_json = fdCommon.try_get(t,["table_json"],{})
        table.table_data_saved = pd.DataFrame(t["table_data_saved"])
        try:
            table.column_mapping = t["column_mapping"]
        except:
            table.column_mapping = []
        table_column_mappings_data = fdCommon.try_get(t,"table_column_mappings",[])
        table.table_column_mappings = []
        for c in table.table_column_mappings:
            table_column_mapping = fdMapping.TableColumnMapping(
                original_column = c['original_column'],
                edited_column = c['edited_column'],
                predicted_column = c['predicted_column'],
                mapped_column = c['mapped_column'],
                original_uom = c['original_uom'],
                predicted_uom = c['predicted_uom'],
                mapped_uom = c['mapped_uom'],
                std_uom = c['std_uom'],
                has_uom = c['has_uom'],
                uom_dimension = c['uom_dimension']
            )
            table.table_column_mappings.append(table_column_mapping)

        table.table_header_mapping = fdCommon.try_get(t,"table_header_mapping",pd.DataFrame())
        if not isinstance(table.table_header_mapping,pd.DataFrame): #was previously a string, protect against old saved values
            table.table_header_mapping = pd.DataFrame(table.table_header_mapping)
        try:
            table.copied_from = t["copied_from"]
            table.split_from = t["split_from"]
            table.split_to = t["split_to"]
            table.edit_log = t["edit_log"]
        except:
            table.copied_from = None
            table.split_from = None
            table.split_to = None
            table.edit_log = []
        try:
            table.table_labels = t['table_tabels']
            table.table_notes = t['table_notes']
        except:
            table.table_labels = None
            table.table_notes = None
        try:
            table.predicted_table_type = t['predicted_table_type']
        except:
            table.predicted_table_type = ''
        table.header = ''
        table.header = fdCommon.try_get(data, 'header_rows', table.header) #handle legacy property name
        table.header = fdCommon.try_get(data, 'header', table.header) #new property name
        table.predicted_header = ''
        table.predicted_header = fdCommon.try_get(data, 'predicted_header_rows', table.predicted_header) #handle legacy property name
        table.predicted_header = fdCommon.try_get(data, 'predicted_header', table.predicted_header) #new property name
        table.transposed = fdCommon.try_get(data, 'transposed', False)
        table.predicted_transposed = fdCommon.try_get(data,'predicted_transposed', False)
        table.validation_errors = fdCommon.try_get(data, 'validation_errors',{})
        tables.append(table)

    report_obj.company = data["company"]
    report_obj.report_name = data["report_name"]
    report_obj.source = data["source"]
    report_obj.filename_pdf = data["filename_pdf"]
    report_obj.fluid_type = fdCommon.try_get(data,"fluid_type","Unknown")
    report_obj.asset = data["asset"]
    report_obj.field = data["field"]
    report_obj.reservoir = data["reservoir"]
    report_obj.well = data["well"]
    report_obj.lab = data["lab"]
    report_obj.fluid_type = data["fluid_type"]
    report_obj.report_data = data["report_data"]
    report_obj.num_pages = data["num_pages"]
    report_obj.num_tables = data["num_tables"]
    report_obj.filename_json = data["filename_json"]
    report_obj.status = data["status"]
    #report_obj.raw_json = raw_json
    report_obj.json_processed = data["json_processed"]
    report_obj.samples = get_report_samples(report_obj.report_name, dig, cfg)

    report_obj.tables = tables
    report_obj.extracted_text = fdCommon.try_get(data,'extracted_text',pd.DataFrame())
    try:
        if isinstance(report_obj.extracted_text,list):
            report_obj.extracted_text = pd.DataFrame(report_obj.extracted_text)
    except:
        report_obj.extracted_text = pd.DataFrame()
    try:
        if report_obj.extracted_text.empty:
            tenant = dig.auth.tenant_name.upper()
            extracted_text_file_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/EXTRACTED_PDF_TEXT/"
            load_extracted_file_text_df(extracted_text_file_path,report_obj.filename_pdf.split('.')[0]+'.csv')
    except:
        report_obj.extracted_text = pd.DataFrame() 
    
    # Define the regex patterns to replace temperature units using superscript degree symbol

    patterns = {
        r'\u00b0C|\u00b0\s*C': 'degC',
        r'\u00b0F|\u00b0\s*F': 'degF',
        r'\u00b0K|\u00b0\s*K': 'degK'
    }
    # Check if the pattern exists in each row of the 'temperature' column
    # report_obj.extracted_text['pattern_exists'] = report_obj.extracted_text['text'].str.contains(pattern)
    # print(report_obj.extracted_text[report_obj.extracted_text['pattern_exists']])
    if report_obj.extracted_text.empty == False:
        report_obj.extracted_text['text'] =  report_obj.extracted_text['text'].replace(patterns, regex=True)


    report_obj.updated = data["updated"]  
    print('restore complete')  
    #report_obj.display_report_info()   
    return True

def save(report_obj, dig):
    
    filename = report_obj.report_name + '_backup.json'
    report_dict = get_report_dict(report_obj)
    fdDataAccess.upload_json_to_BlobStorage(filename,dig.object_store_path,report_dict)