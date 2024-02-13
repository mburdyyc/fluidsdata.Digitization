import pandas as pd
import re
from library import fdCommon
#
# Manage application configuration
#

class TableTypeColumn:
    def __init__(self, table_type, subtable, field, display_name, short_form, symbol, field_type, data_type, has_uom,
                 required_column, required_value, sort_order, min_value, max_value, default_value):
        self.tabletype = table_type
        self.subtable = subtable
        self.field = field
        self.display_name = display_name
        self.short_form = short_form
        self.symbol = symbol
        self.field_type = field_type
        self.data_type = data_type
        self.has_uom = has_uom
        self.required_column = required_column
        self.required_value = required_value
        self.sort_order = sort_order
        self.min_value = min_value
        self.max_value = max_value
        self.default_value = default_value

class TableType:
    def __init__(self, table_name, display_name, header_table, child_table, terms, type, charts):
        self.table_name = table_name
        self.display_name = display_name
        self.header_table = header_table
        self.child_table = child_table
        self.terms = terms
        self.type = type
        self.charts = charts
        self.columns = {}

    def add_table_column(self, table_column):
        self.table_columns.append(table_column)

# class Table:
#     def __init__(self, report, page, report_table_number, page_table_number,
#                  table_type='unknown', table_status='new', num_pages=0, num_tables=0,
#                  table_data_raw=None, table_data_edited=None, table_data_mapped=None,
#                  table_data_normalized=None, table_data_saved=None):
        
#         self.report = report
#         self.page = page
#         self.report_table_number = report_table_number
#         self.page_table_number = page_table_number
#         self.table_type = table_type
#         self.table_status = table_status
#         self.num_pages = num_pages
#         self.num_tables = num_tables
#         self.table_data_raw = table_data_raw
#         self.table_data_edited = table_data_edited
#         self.table_data_mapped = table_data_mapped
#         self.table_data_normalized = table_data_normalized
#         self.table_data_saved = table_data_saved

#     def display_table_info(self):
#         print(f"Report: {self.report}")
#         print(f"Page: {self.page}")
#         print(f"Report Table Number: {self.report_table_number}")
#         print(f"Page Table Number: {self.page_table_number}")
#         print(f"Table Type: {self.table_type}")
#         print(f"Table Status: {self.table_status}")
#         print(f"Number of Pages: {self.num_pages}")
#         print(f"Number of Tables: {self.num_tables}")
#         # Additional print statements to display DataFrame info
#         print(f"Table Data Raw: {self.table_data_raw}")
#         print(f"Table Data Edited: {self.table_data_edited}")
#         print(f"Table Data Mapped: {self.table_data_mapped}")
#         print(f"Table Data Normalized: {self.table_data_normalized}")
#         print(f"Table Data Saved: {self.table_data_saved}")

# class ReportData:
#     def __init__(self, company, report_name, source, filename_pdf, 
#                  asset=None, field=None, reservoir=None, well=None, 
#                  lab=None, report_data=None, filename_json=None, 
#                  status=None, raw_json=None, json_processed=None, 
#                  samples=None, updated=None):
        
#         self.company = company
#         self.report_name = report_name
#         self.source = source
#         self.filename_pdf = filename_pdf
#         self.asset = asset
#         self.field = field
#         self.reservoir = reservoir
#         self.well = well
#         self.lab = lab
#         self.report_data = report_data
#         self.filename_json = filename_json
#         self.status = status
#         self.raw_json = raw_json
#         self.json_processed = json_processed
#         self.samples = samples
#         self.updated = updated if updated else datetime.now()

#     def display_report_info(self):
#         print(f"Company: {self.company}")
#         print(f"Report Name: {self.report_name}")
#         print(f"Source: {self.source}")
#         print(f"PDF Filename: {self.filename_pdf}")
#         print(f"Asset: {self.asset}")
#         print(f"Field: {self.field}")
#         print(f"Reservoir: {self.reservoir}")
#         print(f"Well: {self.well}")
#         print(f"Lab: {self.lab}")
#         print(f"Report Data: {self.report_data}")
#         print(f"JSON Filename: {self.filename_json}")
#         print(f"Status: {self.status}")
#         print(f"Raw JSON: {self.raw_json}")
#         print(f"Processed JSON: {self.json_processed}")
#         print(f"Samples: {self.samples}")
#         print(f"Updated: {self.updated}")

#     def load_raw_json(self, filename):
#         self.filename_json = filename
#         path='DATA_DIGITIZATION_WORKFLOW/REPORT_FILES_JSON/'
#         try:
#             self.raw_json = st.session_state.DataAccess.get_json_from_azure(path,filename)
#             print(f"File '{filename}' loaded into 'raw_json' successfully.")
#         except FileNotFoundError:
#             print(f"File '{path}{filename}' not found.")
        

#     def get_raw_json(self):
#         json_obj = self.raw_json#json.loads(self.raw_json)
#         return json_obj
    
#     def process_raw_json(self):
#         raw_json_obj = self.raw_json#self.get_raw_json()
#         if raw_json_obj is None:
#             print('No raw json to process')
#             return
        
#         prediction_list = raw_json_obj.get('prediction',[])
#         if len(prediction_list) == 0:
#             print('No predictions to process')
#             return
        
#         for p in prediction_list:
#             for item in p:
#                 if p['type'] == 'table':
#                     print(p)
#                 else:
#                     print('not table')

class Configuration():
    def __init__(_self, config_path, source, da):
        _self.config_path = config_path
        _self.source = source
        _self.da = da
        _self.tables = {}
        _self.table_columns_df = None
        _self.tables_df = None
        _self.column_mapping_df = None
        _self.table_mapping_df = None
        _self.table_filter_list = None
        _self.uom_df = None
        _self.uom_mapping_df = None
        _self.component_mapping_df = None
        _self.component_df = None
        _self.enumeration_mapping_df = None
        _self.enumeration_df = None
        _self.asset_info = None
        _self.validation_rules_df = None

# Get configuration from storage
# ToDo: currently from csv files, update to database via api
def load_configuration(cfg):
    config_path = cfg.config_path
    source = cfg.source
    da = cfg.da 

    #ToDo: currently dependent on Streamlit caching. Need to determine approach for backend processing

    # define standard data objects and fields for report and test data
    cfg.table_columns_df = load_table_columns_df(cfg,da,config_path,"/pvt_table_columns.csv", source)
    cfg.table_columns_df['SortOrder'] = cfg.table_columns_df['SortOrder'].replace('',999)
    cfg.table_columns_df['SortOrder'] = cfg.table_columns_df['SortOrder'].astype(int)
    cfg.tables_df =load_tables_df(cfg, da,config_path,"/pvt_tables.csv", source)

    for index, row in cfg.tables_df.iterrows():
        table_obj = TableType(
            row['Table'], row['DisplayName'], row['HeaderTable'], row['ChildTable'], 
            row['Terms'], row['Type'], row['Charts']
        )
        cfg.tables[row['Table']] = table_obj

    for index, row in cfg.table_columns_df.iterrows():
        column_obj = TableTypeColumn(
            row['Table'], row['Subtable'], row['Field'], row['DisplayName'], row['ShortForm'],
            row['Symbol'], row['FieldType'], row['DataType'], row['HasUOM'], row['RequiredColumn'],
            row['RequiredValue'], row['SortOrder'], row['MinValue'], row['MaxValue'], row['DefaultValue']
        )
        table_name = row['Table']
        if table_name in cfg.tables:
            table = cfg.tables[table_name]
            table.columns[row['Field']] = column_obj
        else:
            print(f"Table '{table_name}' not found for column '{row['Field']}'.")

    # all unique mapping observed from previously processed PVT reports to standard data objects and fields
    cfg.column_mapping_df =load_mapping_df(cfg, da,config_path,"/pvt_column_mapping.csv", source) # maps report columns to standard fields

    cfg.table_mapping_df =load_table_mapping_df(cfg, da,config_path,"/pvt_table_mapping.csv", source) # maps unique report table headers to standard table types
    cfg.table_filter_list = cfg.tables_df['Table'].to_list() # create unique list of standard tables
    
    # units of measure mapping
    cfg.uom_df=load_uom_df(cfg,da,config_path,"/pvt_uom.csv", source) # standard UOMs
    cfg.uom_mapping_df =load_uom_mapping_df(cfg,da,config_path,"/pvt_uom_mapping.csv", source) # maps report units of measure to standard

    # update uom mapping with any new uoms found in reports
    distinct_report_uoms = cfg.column_mapping_df['UOM'].unique() # unique UOMs in mapped columns
    for uom in distinct_report_uoms:
        # Check if the UOM does not appear in uom_mapping_df['Report_Uom']
        if (uom.strip() != '') & (uom.strip() not in cfg.uom_mapping_df['ReportUOM'].values):
            # Add it as a new row to uom_mapping_df
            new_row = pd.DataFrame({'ReportUOM': [uom.strip()], 'UOM': [''], 'Multiplier': [1]})
            cfg.uom_mapping_df = pd.concat([cfg.uom_mapping_df, new_row], ignore_index=True)
    
    # component name mapping
    cfg.component_mapping_df =load_component_mapping_df(cfg,da,config_path,"/pvt_component_mapping.csv", source) # maps report component names to standard names
    cfg.component_df =load_component_df(cfg,da,config_path,"/pvt_components.csv", source) # standard component names
    # update component mapping with any new component names found in reports
    distinct_report_components = cfg.column_mapping_df['Field'].apply(fdCommon.extract_text_inside_parentheses).unique()
    
    for component in distinct_report_components:
        # Check if the component does not appear in component_mapping_df['Report_component']
        if component not in cfg.component_mapping_df['ReportComponent'].values:
            # Add it as a new row to component_mapping_df
            new_row = pd.DataFrame({'ReportComponent': [component], 'Component': ['']})
            cfg.component_mapping_df = pd.concat([cfg.component_mapping_df, new_row], ignore_index=True)
        cfg.component_mapping_df = cfg.component_mapping_df.drop_duplicates()

    # enumeration mapping
    cfg.enumeration_mapping_df =load_enumeration_mapping_df(cfg,da,config_path,"/pvt_enumeration_mapping.csv", source) # maps report component names to standard names
    cfg.enumeration_df =load_enumeration_df(cfg,da,config_path,"/pvt_enumerations.csv", source) # standard component names
    # get customer assets, wells, etc. 
    # ToDo: make customer-specific
    cfg.asset_info = get_asset_info(cfg,da)

    # validation rile configuration
    cfg.validation_rules_df =load_validation_rules_df(cfg,da,config_path,"/pvt_validation_rules.csv", source)

# load data methods
# ToDo: remove local option
# ToDo: add error handling
# ToDo: move to api
def get_enum_value(cfg, enumeration_set, value):
    enum_value = None
    enum_mapping = cfg.enumeration_mapping_df[(cfg.enumeration_mapping_df['EnumerationSet']==enumeration_set) & (cfg.enumeration_mapping_df['MappedValue']==value)]
    if enum_mapping.empty == False:
        enum_value = enum_mapping.iloc[0]['Member']
    return enum_value
def get_enumeration_list(cfg, table, column, value=None):
    enum_list = None
    
    table_column_config = get_table_type_column_config(cfg,table, column)
    if table_column_config is not None:
        col_type = table_column_config.data_type
        if col_type.startswith('enum:'):
            enum_set = col_type.split(':')[1]
            enum_list = cfg.enumeration_df[cfg.enumeration_df['EnumerationSet']==enum_set]['Member'].tolist()
    return enum_list
def get_asset_info(cfg,_da):   
    query = f"select distinct AssetName, FieldName, ReservoirName, WellName from dbo.pvt_reports"
    df = _da.df_from_azure_sql(query)
    return df

def load_table_columns_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df
    
def load_tables_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df


def load_mapping_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df


def load_table_mapping_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_table_status_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.df_from_azure_sql("SELECT s.[Report],s.[Page],s.[Tablename],s.[Status],s.[Updated],r.[AssetName],r.[FieldName],r.[ReservoirName],r.[WellName], r.[LabName],r.[ReportDate],r.[Source],s.[TableKey],s.[TableNumber] FROM [dbo].[pvt_table_status] s, [dbo].[pvt_reports] r where s.Report = r.filename")

    return df

def load_uom_mapping_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_uom_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_component_mapping_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_component_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_enumeration_mapping_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_enumeration_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def load_validation_rules_df(cfg,_da,path,filename,source = 'local'):
    if source == 'local':
        df = _da.get_csv_from_local(path, filename)
    else:
        df = _da.get_csv_from_azure(path, filename)
    return df

def get_table_type_column_config(cfg, table, column) -> TableTypeColumn:
    column_config = None
    if table in cfg.tables.keys():
        if column in cfg.tables[table].columns.keys():
            column_config = cfg.tables[table].columns[column]
    return column_config



# Function to extract text inside brackets
# UOMs may be recorded as uom[<UOM>]. This function extracts the UOM
def extract_text_inside_brackets(cfg,text):
    pattern = r'\[(.*?)\]'
    matches = re.findall(pattern, text)
    return matches[0] if matches else None

def is_comp(component, cfg):
    if (component is None) or (component == '') or (component == 'nan'):
        return False
    component = component.strip()
    
    component = fdCommon.convert_subscript_to_normal(component)
    
    cfg.component_mapping_df['ReportComponent'] = cfg.component_mapping_df['ReportComponent'].str.strip()
    # Check if the specific value appears in the ReportComponent column
    if component in cfg.component_mapping_df['ReportComponent'].values:
        return True
    else:
        return False

# Function to get the expected datatype for a column
def get_column_config(column, tablename, cfg):
    # check for indexed parameters
    column_config_name = column

    # parantheses are used to disambiguate the same field used for multiple components
    split = column_config_name.split('(')

    if len(split) > 1:
        column_config_name = split[0] + '()'
    else:
        # brackets are used to disambiguate the same field with multiple UOM
        split = column_config_name.split(' [')

        if len(split) > 1:
            column_config_name = split[0]

    # Filter the table_columns_df based on tablename and column
    filtered_row = cfg.table_columns_df[(cfg.table_columns_df['Table'] == tablename) & (cfg.table_columns_df['Field'] == column_config_name)]

    if filtered_row.empty:
        # If the specified column and tablename combination is not found in table_columns_df
        return None
    else:
        return filtered_row

    # Get the expected data type from the filtered row
    expected_data_type = filtered_row.iloc[0]['DataType']

def get_column_uom_list(uom_df, uom_dimensions, mapped_field):
    uom_list = ['Select UOM']
    if uom_dimensions[mapped_field] != '':
        u = uom_df[uom_df['dimension']==uom_dimensions[mapped_field]]['symbol'].tolist()
    else:
        u = uom_df['symbol'].tolist()
    u.sort()
    uom_list = uom_list + u
    return uom_list
