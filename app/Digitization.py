import sys                                            
sys.path.append("..")
import pandas as pd                                                                                
import streamlit as st    
from datetime import datetime
from datetime import date                                     
from openpyxl import load_workbook
import pandas as pd
from pdf2image import convert_from_bytes
from pyinstrument import Profiler 
from sqlalchemy import create_engine
import json
import requests
from streamlit.web.server.websocket_headers import _get_websocket_headers
from library import fdDigitizationSession
from library import fdAuthorization
from ux_library import fdReportFilters
from ux_library import fdNavigation
from library import fdMapping
from library import fdCommon
from library import fdReport
from library import fdConfiguration
from library import fdDataAccess
from library import fdValidation
from library import fdNormalization
from ux_library import fdUIFunctions
from library import fdTestData
from ux_library import fdFileImport
from azure.storage.blob import BlobClient
from azure.storage.blob import ContainerClient
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient,
    generate_container_sas,
    generate_blob_sas
)
import streamlit_antd_components as sac
import random
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import tempfile


#
# This is a Streamlit app to convert tabular data extracted from PVT reports into standardized sample records
# ToDo: modularize, extract functions to API to support backend batch processing
# ToDo: error handling
# ToDo: logging
#

#
# INITIALIZE CONFIGURATION PARAMETERS
#
st.set_page_config(
    page_title="FLUIDSDATA.COM",
    page_icon="💧",                                     
    layout="wide",
    initial_sidebar_state="expanded",)
#EXP_DEBUG = st.expander("Debug") # Region for debug messages


#initialize profiler
if 'profiler' not in st.session_state:
    st.session_state['profiler']=Profiler()
if not st.session_state.profiler.is_running: 
    st.session_state.profiler.reset()
    st.session_state.profiler.start()


#   xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#
#   MAIN CODE 
#
#   xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

print('***** Main App *****')
# params = st.query_params()
# print('params',params)
####### Initialization ########


if 'digitization_session' not in st.session_state:
    st.session_state['digitization_session'] = fdDigitizationSession.DigitizationSession('')
dig = st.session_state.digitization_session
# ToDo: make source a deployent-time option to support VM or local deployment
source = 'cloud'#st.sidebar.radio("select file source",['cloud','local'])
# initialize data access
if 'DataAccess' not in st.session_state:
        dig.da = fdDataAccess.DataAccess()

# get data paths from app configuration
if dig.config is None:
    dig.config = fdDataAccess.get_app_config(dig.da, source)

image_path = dig.config['image_path']

temp_pdf_path = dig.config['temp_pdf_path']
dig.config_path = dig.config['config_path']

# load digitization configuration
# ToDo: caching strategy for cloud
if 'configuration' not in st.session_state:
        st.session_state['configuration'] = fdConfiguration.Configuration(dig.config_path, source, dig.da)

cfg = st.session_state['configuration'] 


dig.auth = fdAuthorization.do_authorization_flow()
if dig.auth.logged_in == False:
    if 'logo' not in st.session_state:
        size = [1278,338]
        st.session_state['logo'] = dig.da.get_image_from_azure(image_path + '/', 'FluidsDataDarkMode.png',size)
    st.image(st.session_state['logo'])
    st.stop()

today = date.today()                                    
d3 = today.strftime("%m/%d/%y")
st.sidebar.title("fluidsdata Digitization Tool")# - V2.2 (" + d3 + ")")
#fluiddata_image = get_pdf_images('DATA_DIGITIZATION_WORKFLOW/','FluidsDataDarkMode.png')[0]    
#st.sidebar.image(fluiddata_image, width=300)
with st.sidebar:
    view = sac.menu([
        sac.MenuItem('Import Files', icon=None,),
        sac.MenuItem('Report Info', icon=None,),
        sac.MenuItem('Manage Samples', icon=None),
        sac.MenuItem('Map Report Data', icon=None),
        sac.MenuItem('Report Tables', icon=None)
    ],)

tenant = dig.auth.tenant_name.upper()
dig.uploaded_pdf_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/UPLOADED_REPORT_FILES_PDF/"
dig.processing_pdf_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/PROCESSING_REPORT_FILES_PDF/"
dig.processed_pdf_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/REPORT_FILES_PDF/"
dig.json_input_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/REPORT_FILES_JSON/"
dig.csv_output_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/OUTPUT_CSV/"
dig.json_output_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/OUTPUT_JSON/"
dig.object_store_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/OBJECT_STORE/"
dig.extracted_text_file_path = f"DATA_DIGITIZATION_WORKFLOW/COMPANIES/{tenant}/EXTRACTED_PDF_TEXT/"


#
# INITIALIZE SESSION STATE
#



if ('init' not in st.session_state):
    st.session_state['init'] = True
    
    
    fdConfiguration.load_configuration(cfg)
    cfg.table_status_df = fdConfiguration.load_table_status_df(cfg,dig.da,dig.csv_output_path,"/table_status.csv", source)
    
    st.session_state['img'] = None
    
    cfg.tables_df = cfg.tables_df.sort_values(by='Order')
    cfg.tables_df = cfg.tables_df.reset_index(drop=True)
tables_dropdown_list = ['Select Table Type'] + cfg.tables_df['Table'].to_list()


# filter reports. Currently only available for ADNOC data. Need to have meta data for other client's reports (which is currently only available once the report has been digitized)
# if dig.auth.tenant_name == 'ADNOC':
#     filtered_table_status_df = cfg.table_status_df.copy()
#     filtered_table_status_df = fdReportFilters.do_report_filter_flow(filtered_table_status_df)
#     file_list =  sorted(filtered_table_status_df['Report'].unique()) 
# else: 
file_system_name="fluidsdata"
connection_string = "DefaultEndpointsProtocol=https;AccountName=fluidsdatafiles;AccountKey=3hBNsR8DPO/BfYlNucd+QZG4Gj1KuKih/YRdb+5tA0DxkvLPXvVbNVzjMsB5FgjwADy4nppXbKyV+AStdXzAvA==;EndpointSuffix=core.windows.net"
container = ContainerClient.from_connection_string(connection_string, container_name=file_system_name)
file_list = dig.file_list 

if view == 'Import Files':
    fdFileImport.do_file_import_flow(dig, cfg)
    selected_file = None

else:
    #create a dictionary to get a numbered list of files     
    dictionary = dict(enumerate(file_list))         
    dict_list = list(dictionary.items())            

    if len(dict_list) > 0:
        selected_tuple = st.sidebar.selectbox('SELECT A FILE TO REVIEW', dict_list)
        selected_file = selected_tuple[1]  
    else:
        selected_file = None             

    st.sidebar.write('SELECTED FILE:  ', selected_file)

    
if selected_file is not None:

    if dig.selected_file != selected_file:
        fdMapping.file_selected(dig, cfg, selected_file)
        
    if view == 'Report Info':

        REPORT_COL1, REPORT_COL2 = st.columns([1,1])   

        with REPORT_COL1: # form for displaying and entering asset, well, and other common report data.
            st.title('Report Info')
            form_asset = st.text_input('asset',value=dig.report_obj.asset)
            form_field = st.text_input('field',value=dig.report_obj.field)
            form_reservoir = st.text_input('reservoir',value=dig.report_obj.reservoir)
            form_well = st.text_input('well',value=dig.report_obj.well)
            form_lab = st.text_input('lab',value=dig.report_obj.lab) 
            fluid_type_index = 0
            # Convert values of the DataFrame column to lowercase and store them in a list
            text_list = [text.lower() for text in dig.report_obj.extracted_text['text']]
            fluid_type_list = ['Unknown','Dry Gas', 'Wet Gas', 'Gas Condensate', 'Volatile Oil', 'Black Oil', 'Heavy Oil']
            if (any('condensate' in text for text in text_list)):
                fluid_type_index = fluid_type_list.index('Gas Condensate')
            form_type = st.selectbox('Fluid Type', fluid_type_list, index=fluid_type_index)       
            submitted = st.button("Save")   
            if submitted:
                dig.report_obj.asset = form_asset
                dig.report_obj.field = form_field
                dig.report_obj.reservoir = form_reservoir
                dig.report_obj.well = form_well
                dig.report_obj.lab = form_lab
                dig.report_obj.fluid_type = form_type
                fdReport.save(dig.report_obj, dig)
        with REPORT_COL2: # scrollable view of the report file
            st.session_state['pdf_bytes'] = dig.da.get_pdf_file_from_azure(dig.processed_pdf_path,dig.report_obj.filename_pdf)
            import base64
            b64 = base64.b64encode(st.session_state['pdf_bytes']).decode()
            pdf_display = f'<iframe src="data:application/pdf;base64,{b64}" width="700" height="1000" style="border:0;"></iframe>'
            st.markdown(pdf_display, unsafe_allow_html=True)
    
    if view == 'Map Report Data':

        PAGE_HEADER_COL1, PAGE_HEADER_COL2 = st.columns([1,1])

        with PAGE_HEADER_COL1:
            # split the screen to display the report pdf if checkbox selected
            st.checkbox('View Report File',value=True,key='view_report_file')
            
        if st.session_state.view_report_file:    
            IMAGE_COL, DATA_COL = st.columns([3,5])
        else:
            DATA_COL = st.container()
        
        with DATA_COL: 
            TABLE_SELECTOR = st.container()
            TYPE_SELECTOR, PREDICT_TABLE = st.columns([4,1]) 

            with DATA_COL:
                MAP_HEADER_CONT = st.container()
                DATA_COL1 = st.container()

            selected_page_index = fdNavigation.do_page_navigation_flow(dig, dig.selected_page_index)
            
            if dig.selected_page_index != selected_page_index:
                ### new page selected ###
                st.session_state['img'] = fdMapping.page_selected(dig, selected_page_index)
                dig.selected_table_number = -1 # force table initialization
                dig.selected_page_index = selected_page_index
                dig.page_tables = fdReport.get_page_tables(dig.report_obj, selected_page_index)
            
            
            if len(dig.page_tables) == 0: 
                dig.selected_table_obj = None
                dig.tab = 'None'
            else:
                if len(dig.page_tables) > 1: #show table selection slider if more than one table on the page
                    option_labels = {}
                    options = []
                    i = 0
                    for t in dig.page_tables:
                        options.append(i)
                        option_labels[i] = f'Table {i+1} ({t.table_type})'
                        i = i+1
                    with TABLE_SELECTOR:
                        selected_table_number = st.radio("Select Table",options=options,format_func=lambda x: option_labels.get(x), horizontal=True,) + 1
                else:
                    selected_table_number = 1
                
                with TABLE_SELECTOR:

                    STEP, PREV_STEP, NEXT_STEP, PREDICT, RESET = st.columns([2,2,2,2,2])
                    tabs = ['Edit','Map Columns','Map Header','Review','Saved','Normalized','Save Test']
                    st.session_state['tabs'] = tabs
                    with PREV_STEP:
                        idx = tabs.index(dig.tab)
                        if idx > 0:
                            st.button(f'<{tabs[idx-1]}',args=[idx],on_click=fdUIFunctions.ux_decrement_tab,key='prev_page',use_container_width=True)
                    with NEXT_STEP:
                        if idx < len(tabs)-1:
                            st.button(f'{tabs[idx+1]}>',args=[idx],on_click=fdUIFunctions.ux_increment_tab, key='next_page',use_container_width=True)
                    with STEP:
                        st.subheader(f'{dig.tab}')
                
                if (dig.selected_table_number != selected_table_number):
                    ### new table selected ###
                    dig.selected_table_obj = dig.page_tables[selected_table_number-1]
                    fdMapping.table_selected(dig, cfg, dig.selected_table_obj) 
                    dig.selected_table_number = selected_table_number 
                    dig.tab = 'Edit'
                    st.rerun()

            if dig.selected_page_index > -1:
                if st.session_state.view_report_file == True:
                    with IMAGE_COL:
                        st.image(st.session_state['img'])
                        
            
            if (dig.selected_table_obj is not None) and (dig.selected_table_obj.table_data_edited.empty == False): 
                if dig.tab in ('Edit','Map Columns'): 

                    if dig.selected_table_obj.table_type not in ('','Unknown', 'Select Table Type'):
                        selected_table_type_index = cfg.tables_df.index[cfg.tables_df['Table']==dig.selected_table_obj.table_type].tolist()[0]+1 # +1 to skip 'select table'
                    else:
                        selected_table_type_index = 0
                    
                    #st.write(dig.selected_table_obj.predicted_table_type)

                    
                    with TYPE_SELECTOR:
                        selected_table_type = st.selectbox('SELECT A TABLE TYPE', tables_dropdown_list, index = selected_table_type_index, key = 'ppp', format_func=fdUIFunctions.ux_mark_inferred_type_option,label_visibility="collapsed")
                        # One-time actions when new table type is selected
                        if selected_table_type not in ['Select Table Type','',dig.selected_table_type]:
                            fdMapping.table_type_changed(dig, cfg, selected_table_type)
                    
                    with PREDICT:
                        #force a re-prediction
                        if st.button('✨ Predict'):
                            dig.selected_table_obj.table_type = 'Unknown'
                            dig.selected_table_obj.table_column_mappings = []
                            dig.selected_table_obj.table_header_mapping = pd.DataFrame()
                            fdMapping.table_selected(dig, cfg,dig.selected_table_obj)


#============================================================================================================================================================
#
# Edit tab - allows user to reshape the data table as needed prior to mapping. User actions are tracked and can be performed automatically for similar tables
#
#============================================================================================================================================================
                
                
                if dig.tab == 'Edit':
                       
                    DATA_COL1, DATA_COL2 = st.columns([3,1])

                    if 'Selected' not in dig.selected_table_obj.table_data_edited.columns:
                        dig.selected_table_obj.table_data_edited.insert(0, column='Selected', value=False)

                    with DATA_COL1:
                        if dig.selected_table_obj.header != '':
                            if dig.selected_table_obj.header == dig.selected_table_obj.predicted_header:
                                    st.caption('✨ Column headers set automatically based on prediction. Reset Table to set them manually')
                            if dig.selected_table_obj.transposed and (dig.selected_table_obj.transposed == dig.selected_table_obj.predicted_transposed):
                                    st.caption('✨ Column headers transposed automatically based on prediction. Reset Table to set them manually')
                        st.data_editor(dig.selected_table_obj.table_data_edited,use_container_width=True,hide_index=False, height=1200, key='edited_data_editor', on_change=fdUIFunctions.ux_edited_data_editor_on_change)# create and populate common report fields such as asset, etc.
                                
                    with DATA_COL2: # table editing options
                        
                        # get selected rows
                        row_nums = []
                        for i, row in dig.selected_table_obj.table_data_edited.iterrows():
                            if row['Selected'] == True:
                                row_nums.append(i)    
                        
                        if (dig.selected_table_obj.header == '') and (dig.selected_table_obj.transposed == False):
                            split_disabled = False
                        else:
                            split_disabled = True
                        # Split table
                        if st.button('Split Table', key='selected_split', disabled=split_disabled):
                            if len(row_nums) > 0:
                                #split_cols = dig.selected_table_obj.table_data_edited.columns
                                selected_table_number, dig.selected_table_obj = fdMapping.split_table(dig, dig.selected_table_obj,row_nums[0])
                                dig.selected_table_number = selected_table_number + 1
                                dig.page_tables = fdReport.get_page_tables(dig.report_obj, selected_page_index)
                                st.rerun()
                            else:
                                st.write('Select one or more rows to split')
                        
                        # Unsplit table
                        if dig.selected_table_obj.split_from is not None:
                            if st.button('UnSplit Table', key='selected_unsplit'):

                                if selected_table_number > 1:
                                    fdMapping.unsplit_table(dig, dig.selected_table_obj)
                                    dig.page_tables = fdReport.get_page_tables(dig.report_obj, selected_page_index)
                                    st.rerun()
                                else:
                                    st.write("Can't unsplit first table")  

                        # Transpose table
                        unpivoted = dig.selected_table_obj.table_data_edited
                        st.button('Transpose', key='selected_transpose', on_click=fdMapping.transpose_df, disabled = dig.selected_table_obj.transposed)
                        
                        # Set header
                        if st.button('Set Header Row(s)', key='selected_set_header'):
                            if len(row_nums) > 0:
                                dig.selected_table_obj.table_data_edited = fdMapping.set_table_column_headings(dig, cfg, dig.selected_table_obj.table_data_edited, row_nums)
                                st.rerun()
                            else:
                                st.write('Select one or more rows to create header')

                        # Delete rows
                        if st.button('Delete Row', key='selected_delete'):
                            if len(row_nums) > 0:
                                for r in reversed(range(dig.selected_table_obj.table_data_edited.index[0],dig.selected_table_obj.table_data_edited.index[0]+len(dig.selected_table_obj.table_data_edited.index))):
                                    for rn in row_nums:
                                        st.write(r,rn)

                                        if r == rn:
                                            st.write('delete')
                                            dig.selected_table_obj.table_data_edited = dig.selected_table_obj.table_data_edited.drop([r])

                                fdMapping.log_edit(dig.auth, dig.selected_table_obj,'Deleted row(s)', row_nums)
                                st.rerun()
                            else:
                                st.write('Select one or more rows to delete')

                        def ux_copy_table():
                            selected_table_number, dig.selected_table_obj = fdMapping.copy_table(dig, dig.selected_table_obj)
                            dig.selected_table_number = selected_table_number + 1
                            dig.page_tables = dig.report_obj.get_page_tables(selected_page_index)
                        # Copy table
                        st.button('Copy Table', key='selected_copy', on_click=ux_copy_table)

                        def ux_delete_copied_table():
                            fdMapping.delete_copied_table(dig, dig.selected_table_obj)
                            dig.selected_table_number =  -1 #force re-init
                            dig.selected_table_obj = None
                            dig.page_tables = fdReport.get_page_tables(dig.report_obj, selected_page_index)
                            fdMapping.log_edit(dig.auth, dig.selected_table_obj,'Delete copied table')

                        if dig.selected_table_obj.copied_from is not None:
                            st.button('Delete Copied Table', key='delete_copy', on_click=ux_delete_copied_table)
                                
                        # Reset table to original data
                        def ux_reset_table():
                            dig.selected_table_obj.table_data_edited = dig.selected_table_obj.table_data_raw
                            dig.selected_table_obj.header = ''
                            dig.selected_table_obj.transposed = False
                            fdMapping.log_edit(dig.auth, dig.selected_table_obj,'Reset table')
                        
                        st.button("Reset Table", on_click=ux_reset_table)

                    with st.expander('Edit Log'):
                        if dig.selected_table_obj is not None:
                            st.write(dig.selected_table_obj.edit_log)          

#============================================================================================================================================================
#
# Map Columns tab - allows user to the report table columns to standard columns. User actions are tracked and can be performed automatically for similar tables
#
#============================================================================================================================================================


                if dig.tab == 'Map Columns':
                    
                    if 'Selected' in dig.selected_table_obj.table_data_edited.columns:
                        dig.selected_table_obj.table_data_edited = dig.selected_table_obj.table_data_edited.drop(columns=['Selected'], axis=1)

                    
                    if dig.selected_table_obj.table_type != 'Select Table Type':
                    
                        fdMapping.map_table_columns(dig, cfg)
            
                        #column_mappings = st.session_state.column_mappings


                        # with DATA_COL:
                            # if st.checkbox('Only show typical columns in drop down lists',key='show_typical_columns',value=True) == True:
                            #     show_required_columns = ['Y','N']
                            # else:
                            #     show_required_columns = ['Y']
                        with DATA_COL:
                            REPORT_COL, STANDARD_COL, PREDICTED_UOM, REPORT_UOM, STANDARD_UOM = st.columns([8,8,1,8,8])
                        with REPORT_COL:
                            st.markdown('**Report Column**')
                        with STANDARD_COL:
                            st.markdown('**Standard Column**')
                        with REPORT_UOM:
                            st.markdown('**Report UOM**')
                        with STANDARD_UOM:
                            st.markdown('**Standard UOM**')
                        comp_mapping = ''
                        uom_mapping = ''

                        c = 0
                        for col in dig.selected_table_obj.table_column_mappings:
                            if col.original_column != 'Default':
                                with st.container(border=False):
                                    REPORT_COL, STANDARD_COL, PREDICTED_UOM, REPORT_UOM, STANDARD_UOM = st.columns([8,8,1,8,8])
                                    col_key = str(c)

                                    with REPORT_COL:
                                        col.edited_column = st.text_input('edited column name', value=col.edited_column, label_visibility='collapsed', key='ecol'+col_key)

                                    with STANDARD_COL: # create column mapping dropdown for each column in table
                                        def ux_update_mapped_column(idx,key):
                                            dig.selected_table_obj.table_column_mappings[idx].mapped_column = st.session_state[key]

                                        if (col.mapped_column is not None) and (col.mapped_column not in ['', 'No Mapping']): 
                                            col.mapped_column = col.mapped_column.split(' [')[0]
                                            idx = dig.columns_dropdown_list.index(col.mapped_column)
                                            
                                        elif fdConfiguration.is_comp(col.original_column, cfg) and (comp_mapping != ''):
                                            idx = dig.columns_dropdown_list.index(comp_mapping)
                                            col.predicted_column = comp_mapping
                                            col.mapped_column = comp_mapping

                                        else:
                                            idx = 0

                                        key = 'mcl'+col_key
                                        st.selectbox('Map %s' %col.original_column, dig.columns_dropdown_list ,index=idx,key=key, 
                                                    args=[c,key],  on_change=ux_update_mapped_column,
                                                    format_func=lambda option: '✨' + option if option == col.predicted_column else option, label_visibility='collapsed')
                                        
                                        # if the column represent a component, store it's mapping to apply to other components
                                        if fdConfiguration.is_comp(col.original_column, cfg) and (comp_mapping == ''):
                                            comp_mapping = col.mapped_column

                                    with REPORT_UOM:
                                        def update_original_uom(key, index):
                                            original_uom = st.session_state[key]
                                            if original_uom not in ['Select UOM', 'n/a', '']:
                                                col.original_uom = original_uom
                                            else:
                                                col.original_uom = ''
                                    
                                        if (col.mapped_column is not None) and (col.mapped_column not in ['', 'No Mapping']):
                                            # create and populate uom text entry fields if applicable
                                            if dig.has_uoms[col.mapped_column] == 'Y':
                                                if fdConfiguration.is_comp(col.original_column, cfg) and (uom_mapping != ''):                                                       
                                                    col.predicted_uom = uom_mapping
                                                if col.predicted_uom == '': #if there is no predicted UOM, present an entry box with placeholder text
                                                    col.original_uom = st.text_input('UOM',placeholder='enter uoms',value=None,key='ouom'+col_key,args=('ouom'+col_key, c),label_visibility="collapsed", on_change=update_original_uom)
                                                else:
                                                    col.original_uom = st.text_input('UOM',value=col.predicted_uom ,key='ouom'+col_key,args=('ouom'+col_key, c),label_visibility="collapsed", on_change=update_original_uom)
                                                # if the column represent a component, store it's mapping to apply to other components
                                                if fdConfiguration.is_comp(col.original_column, cfg) and (uom_mapping == ''):
                                                    uom_mapping = col.original_uom  
                                                         
                                    with PREDICTED_UOM:
                                        if (col.predicted_uom not in ['','n/a']) and (col.predicted_uom == col.original_uom):
                                            st.write('✨')

                                    with STANDARD_UOM:
                                        if (col.mapped_column is not None) and (col.mapped_column not in ['', 'No Mapping']) and (col.original_uom != ''):
                                            
                                            std_uom = fdNormalization.get_standard_uom(col.original_uom, cfg.uom_mapping_df)
                                            uom_list = fdConfiguration.get_column_uom_list(cfg.uom_df, dig.uom_dimensions,col.mapped_column)
                                            uom_idx = fdCommon.get_index_in_list(std_uom, uom_list)
                                            mapped_uom = st.selectbox('Map %s' %col.original_uom, uom_list ,index=uom_idx,key='suom'+col_key, format_func=lambda option: '✨' + option if option == col.std_uom else option, label_visibility='collapsed')
                                            if mapped_uom not in ['Select UOM', 'n/a']:
                                                col.mapped_uom = mapped_uom
                                            else:
                                                col.mapped_uom = ''

                            c = c+1
                        
                        # update table to correct data types based on mapped column type, if possible, but don't overwrite invalid data
                        dig.selected_table_obj.table_data_edited = fdMapping.try_normalize_df_data_data_types(dig.selected_table_obj.table_data_edited, dig.selected_table_obj.table_type, cfg, dig.selected_table_obj.table_column_mappings)
                        
                        if st.button('Save Mapping '):
                            fdMapping.save_column_mapping(dig.selected_table_obj, dig.da, cfg.column_mapping_df, dig.config_path)
                    
                    with DATA_COL1:
                        # set the column headings in the streamlit table, without changign data frame column names
                        column_cfg = {}
                        if dig.selected_table_obj.header != '':
                            for col in dig.selected_table_obj.table_column_mappings:
                                if col.mapped_column not in ['', 'No Mapping']:
                                    if  col.mapped_column == col.predicted_column:
                                        column_cfg[col.original_column] = st.column_config.Column('✨' + col.mapped_column )
                                    else:
                                        column_cfg[col.original_column] = st.column_config.Column(col.mapped_column )

                                    
                        st.data_editor(dig.selected_table_obj.table_data_edited,use_container_width=True,hide_index=False, height=300,column_config=column_cfg, key='edited_data_editor', on_change=fdUIFunctions.ux_edited_data_editor_on_change)# create and populate common report fields such as asset, etc.


#============================================================================================================================================================
#
# Map Header tab - allows user to enter data for the overall test. Fields are automatically populated where possible
#
#============================================================================================================================================================

                if dig.selected_table_obj.table_type != '':
                    if dig.tab == 'Map Header':        

                        dig.selected_table_obj.table_header_mapping = fdMapping.map_header_columns(dig.selected_table_obj.table_header_mapping, dig.selected_table_obj, dig.header_columns_config_df, dig, cfg)
                        mapped_headers = dig.selected_table_obj.table_header_mapping
                        
                        # create text inputs for headers, with UOMs if relevant
                        idx = 0

                        with MAP_HEADER_CONT: 
                            st.subheader('Header Data')
                            
                            HDR_NAME, PREDICTED_HEADER_ICON,MAPPED_HEADER_VALUE, PREDICTED_HEADER_UOM_ICON, ORIG_HEADER_UOM, MAPPED_HEADER_UOM = st.columns([10,1,10,1,7,7])
                            with HDR_NAME:
                                st.markdown('**Standard Property**')
                            with MAPPED_HEADER_VALUE:
                                st.markdown('**Report Value**')
                            with ORIG_HEADER_UOM:
                                st.markdown('**Report UOM**')
                            with MAPPED_HEADER_UOM:
                                st.markdown('**Standard UOM**')
                            if len(mapped_headers.index) == 0:
                                st.info('No header data for for this table')
                        
                            uom_dimensions =  dict(zip(dig.header_columns_config_df['Field'].values,dig.header_columns_config_df['UOMDimension'].values))
                            
                            for r, row in dig.selected_table_obj.table_header_mapping.iterrows():
                                hdr_key = str(r)
                                with st.container(border=False):
                                    HDR_NAME, PREDICTED_HEADER_ICON,MAPPED_HEADER_VALUE, PREDICTED_HEADER_UOM_ICON, ORIG_HEADER_UOM, MAPPED_HEADER_UOM = st.columns([10,1,10,1,7,7])
                                    with HDR_NAME:
                                        col_cfg = fdConfiguration.get_table_type_column_config(cfg, dig.selected_table_obj.table_type,row['field_name'])
                                        if col_cfg.required_column == 'Y':
                                            st.write(f"{row['field_name']}*")
                                        else:
                                            st.write(row['field_name'])

                                    with MAPPED_HEADER_VALUE:
                                        if row['field_name'] == 'SampleID':
                                            sample_ids, sample_labels = fdMapping.get_sample_list(dig)
                                            sample_label_dict = dict(zip(sample_ids, sample_labels))
                                            idx = fdCommon.get_index_in_list(row['value'],sample_ids)

                                            mapped_headers.at[r,'value'] = st.selectbox(row['field_name'], sample_ids,index = idx, key='mkey'+hdr_key, format_func=lambda x: sample_label_dict[x],label_visibility="collapsed")

                                        elif row['value'] != '':
                                            mapped_headers.at[r,'value'] = st.text_input(row['field_name'],value = row['value'], key='mkey'+hdr_key,label_visibility="collapsed")
                                        else:
                                            mapped_headers.at[r,'value'] = st.text_input(row['field_name'],placeholder='enter value', key='mkey'+hdr_key,label_visibility="collapsed")
                                    with PREDICTED_HEADER_ICON:
                                        if (row['predicted_value'] is not None) and (row['value'] == row['predicted_value']):
                                            st.write('✨')
                                    with ORIG_HEADER_UOM:        
                                        if row['has_uom'] == 'Y':

                                            if row['mapped_uom'] != '':
                                                mapped_headers.at[r,'uom'] = st.text_input('UOM',value = row['uom'], key='muom'+hdr_key,label_visibility="collapsed")
                                            else:
                                                mapped_headers.at[r,'uom'] = st.text_input('UOM',placeholder='enter uom', key='muom'+hdr_key,label_visibility="collapsed")
                                        else:
                                            mapped_headers.at[r,'uom'] = None    
                                        
                                    with PREDICTED_HEADER_UOM_ICON:    
                                        if (row['predicted_uom'] is not None) and (row['uom'] == row['predicted_uom']):
                                                st.write('✨')
                                    with MAPPED_HEADER_UOM:
                                        if (row['uom'] is not None) and (row['uom'] != ''):
                                            uom_list = fdConfiguration.get_column_uom_list(cfg.uom_df, dig.uom_dimensions,row['field_name'])
                                            
                                            if row['std_uom'] in uom_list:
                                                uom_list.insert(2,uom_list.pop(uom_list.index(row['std_uom'])))
                                            
                                            idx = fdCommon.get_index_in_list(row['std_uom'], uom_list)
                                        
                                            mapped_uom = st.selectbox('Map %s' %row['uom'], uom_list ,index=idx,key='skey'+hdr_key, format_func=lambda option: '✨' + option if option == row['std_uom'] else option, label_visibility='collapsed')
                                            if mapped_uom not in ['Select UOM']:
                                                mapped_headers.at[r,'mapped_uom'] = mapped_uom
                                            else:
                                                mapped_headers.at[r,'mapped_uom'] = ''
                                        else:
                                            mapped_headers.at[r,'mapped_uom'] = ''
                        # Create output dataframe
                        dig.selected_table_obj.header_data_mapped  = pd.DataFrame()
                        # Add values and units of measure as new columns

                        for r, row in mapped_headers.iterrows():
                            dig.selected_table_obj.header_data_mapped[row['field_name']] = [row['value']]

                            if row['mapped_uom'] != 'n/a':
                                dig.selected_table_obj.header_data_mapped[row['field_name'] + '_UOM'] = row['mapped_uom']

                        
                    elif dig.tab == 'Saved':
                        st.write('status:', dig.selected_table_obj.table_status)
                        #output_df = st.session_state['output_df']
                        if dig.selected_table_obj.table_status in ('Saved','Accepted'):
                            
                            with DATA_COL1:
                                # Load previously saved datan for display
                                key = selected_file + '_' + str(selected_page_index) + '_' + str(selected_table_number) + '_' + dig.selected_table_obj.table_type
                                key = key.replace('\n',' ')
                                filename = key + '.csv'
                                try:
                                    st.write('file',filename)
                                    saved_df = fdDataAccess.load_saved_df(dig.da, dig.csv_output_path,filename, source)
                                    
                                # handle legacy naming convention
                                except:
                                    key = selected_file + '_' + str(selected_page_index) + '_' + dig.selected_table_obj.table_type
                                    key = key.replace('\n',' ')
                                    filename = key + '.csv'
                                    saved_df = fdDataAccess.load_saved_df(dig.da,dig.csv_output_path,filename, source)
                                saved_df = saved_df.astype('str')
                                st.dataframe(saved_df,use_container_width=True,)
                                #st.write('Table Status:', dig.selected_table_obj.table_status)
                                if dig.selected_table_obj.table_status != 'Accepted':
                                    # Accept saved data
                                    if st.button('Accept Saved Data'):
                                        dig.selected_table_obj.table_status = fdMapping.update_file_and_status(dig.da,selected_file,selected_page_index, selected_table_number, dig.selected_table_obj.table_type, 'Accept')
                                        st.write('Accepted')
                                        st.rerun()
                                    # Reject saved data
                                    if st.button('Reject Saved Data'):
                                        dig.selected_table_obj.table_status = fdMapping.update_file_and_status(dig, cfg,dig.da,selected_file,selected_page_index, selected_table_number, dig.selected_table_obj.table_type, 'Rejected')
                                        st.write('Rejected')
                                        dig.selected_table_obj.table_status = 'Rejected'
                                        st.rerun()
                                else:
                                    # set status from Accepted back to Saved
                                    if st.button('Un-Accept Data'):
                                        dig.selected_table_obj.table_status = fdMapping.update_file_and_status(dig, cfg,dig.da,selected_file,selected_page_index, selected_table_number, dig.selected_table_obj.table_type, 'Saved')
                                        st.write('Changed to Saved')
                                        dig.selected_table_obj.table_status = 'Saved'
                                        st.rerun()

#============================================================================================================================================================
#
# Review tab - allows user to review the final data and save a copy
#
#============================================================================================================================================================

                    elif dig.tab == 'Review':    
                        
                        validation, output_df = fdMapping.update_table_data_mapped(dig.selected_table_obj, dig.selected_table_obj.table_column_mappings, dig.table_columns_config_df, dig, cfg)
                        fdMapping.normalize_table_data(dig.selected_table_obj, cfg)
                        with DATA_COL1:
                            # Display final mapped data, with validation status
                            VAL_EXP2 = st.expander(':bangbang: :red[Validation errors]')
                            # check for mandatory columns and add to dataset 
                            with st.container(border=True):
                                HEADER_COLS = st.columns([1,1])
                            c = 0
                            for col in dig.selected_table_obj.header_data_normalized.columns:
                                hc = c % 2
                                #header_data_mapped has only non-empty or mandatory columns, so check if column exists there
                                if col in dig.selected_table_obj.table_header_mapping['field_name'].tolist():
                                    with HEADER_COLS[hc]:
                                        if f'{col}_UOM' in dig.selected_table_obj.header_data_normalized.columns:
                                            uom = dig.selected_table_obj.header_data_mapped.iloc[0][f'{col}_UOM']
                                        else:
                                            uom = ''
                                        if col == 'SampleID':
                                            sample_ids, sample_labels = fdMapping.get_sample_list(dig)
                                            sample_label_dict = dict(zip(sample_ids, sample_labels))
                                            value = sample_label_dict[dig.selected_table_obj.header_data_normalized.iloc[0][col]]
                                        else:
                                            value = dig.selected_table_obj.header_data_normalized.iloc[0][col]
                                        if isinstance(value, str):  #replace characters that don't work with markdown
                                            value = value.replace(':', '&#58;')
                                        st.markdown(f"**{col}:** {value} {uom}")
                                        c = c + 1
                            
                            column_config = {}
                            for col in dig.selected_table_obj.table_data_normalized.columns:
                                enum_list = fdConfiguration.get_enumeration_list(cfg, dig.selected_table_obj.table_type, col)
                                if enum_list is not None:
                                    column_config[col] = st.column_config.SelectboxColumn(col,options=enum_list)
                                # c_cfg = st.column_config.Column(col.original_column)
                                # if  col.mapped_column == col.predicted_column:
                                #     column_cfg[col.original_column] = st.column_config.Column('✨' + col.mapped_column )
                                # elif col.mapped_column != 'No Mapping':
                                #     column_cfg[col.original_column] = st.column_config.Column(col.mapped_column )
                                
                            st.data_editor(dig.selected_table_obj.table_data_normalized,column_config=column_config,use_container_width=True,hide_index=False,key='mapped_data_editor')#, on_change='ux_mapped_data_editor_on_change')
                        
                        with VAL_EXP2:
                            
                            if len(validation) > 0:
                                st.write(validation)

                            if len(dig.selected_table_obj.validation_errors)>0:
                                st.write('stvalid',dig.selected_table_obj.validation_errors)

                        st.write('status:', dig.selected_table_obj.table_status)   
                        if dig.selected_table_obj.table_status != 'Accepted': 

                            # Save data
                            if st.button('Save Data'):
                                def save_data(table, status):
                                    with st.spinner('Save in progress'):
                                        st.write('Updating file status')
                                        dig.selected_table_obj.table_status = fdMapping.update_file_and_status(dig, cfg,dig.da,selected_file,selected_page_index, dig.selected_table_obj.page_table_number, dig.selected_table_obj.table_type, 'Saved')
                                        st.write('Saving column mapping')
                                        fdMapping.save_column_mapping(dig.selected_table_obj, dig.da, cfg.column_mapping_df, dig.config_path)
                                        st.write('Saving report backup')
                                        fdReport.save(dig.report_obj, dig)
                                        st.write('Saved')
                                st.rerun() #refresh the screen

                                                            # Save and publish data
                            if (len(validation) == 0) and (dig.selected_table_obj.table_status == 'Saved'): #data is valid
                                if st.button('Publish Data'):
                                    with st.spinner('Publish in progress'):
                                        table = dig.selected_table_obj

                                        st.write('Normalizing data')
                                        fdMapping.normalize_table_data(table, cfg)
                                        st.write('Converting to JSON')
                                        table_json = fdNormalization.table_data_to_json(table, cfg.tables_df)
                                        #dig.selected_table_obj.table_data_normalized = table_json

                                        if table.table_type == 'Sample':
                                            st.write('Saving sample(s)')
                                            response_code, response_text = fdTestData.save_sample(dig.report_obj,table.table_data_normalized, dig.auth)
                                            st.info(f'Add Sample: {response_text}')
                                            dig.report_obj.samples = fdReport.get_report_samples(dig,dig.report_obj.report_name, cfg)

                                        filename = f'{dig.selected_file}_{table.page}_{table.page_table_number}_{table.table_type}.json'
                                        st.write('saving json file')

                                        fdDataAccess.upload_json_to_BlobStorage(filename,dig.json_output_path,table_json)
                                        
                                        table.table_status = 'Published'
                                        st.write('Saving report backup')
                                        fdReport.save(dig.report_obj, dig)
                                        st.write('Saved')
                                        st.rerun()
                    

#============================================================================================================================================================
#
# Save Test tab - allows user to create or update samples and tests based on the digitized tables. Everything up to this point has been dealing with individual
#                   table in the report. Here, the tables are added into the sample they belong to and stored to the backend
#
#============================================================================================================================================================

                    elif dig.tab == 'Save Test':
                        response = fdDataAccess.get_samples(dig, source=dig.report_obj.report_name)
                        existing_samples = json.loads(response)
                        test_list = []
                        header_table_name = cfg.tables_df[(cfg.tables_df['Table']==dig.selected_table_obj.table_type)]['HeaderTable'].values[0]
                        endpoint = cfg.tables_df[(cfg.tables_df['Table']==dig.selected_table_obj.table_type)]['EndPoint'].values[0]
                        for sample in existing_samples['data']['samples']:
                            if endpoint in sample.keys():
                                if isinstance(sample[endpoint],list): #sample can have multiple instances of the test type
                                    for tests in sample[endpoint]:
                                        test_list.append(tests)
                                else:
                                    test_list.append(sample[endpoint])
                        test_type = dig.selected_table_obj.table_type
                        dig.report_obj.tests[test_type] = test_list


                        test_labels = ['Select Test']
                        
                        table_json = dig.selected_table_obj.table_json
                        predicted_test_idx = 0
                        num_tests = 1
                        test_json_list = []
                        report_sample_id = fdCommon.try_get(table_json[header_table_name],'SampleID','') 
                        report_saturation_pressure = fdCommon.try_get(table_json,'SaturationPressure','')
                        report_test_temperature = fdCommon.try_get(table_json,'TestTemperature','') 

                        for test in test_list:
                            table_matches_test = True
                            # Convert all dictionary keys to lower case
                            
                            lower_case_keys = {k.lower(): v for k, v in test.items()}
                            test_label = f'{header_table_name}# {num_tests}: '
                            if ('sampleid' in lower_case_keys):
                                sample_id =  lower_case_keys['sampleid']
                                sample_ids, sample_labels = fdMapping.get_sample_list(dig)
                                sample_label_dict = dict(zip(sample_ids, sample_labels))
                                test_label = test_label + f'Sample ID: {sample_label_dict[sample_id]}'
                                if report_sample_id != sample_id:
                                    table_matches_test = False
                            if ('saturationpressure' in lower_case_keys):
                                saturation_pressure =  lower_case_keys['saturationpressure']
                                saturation_pressure_uom =  lower_case_keys['saturationpressure_uom']
                                test_label = ', '.join([test_label, f'Saturation Pressure: {str(saturation_pressure)} {saturation_pressure_uom}'])
                                
                                if report_saturation_pressure != saturation_pressure:
                                    table_matches_test = False
                            if ('testtemperature' in lower_case_keys):
                                test_temperature =  lower_case_keys['testtemperature']
                                test_temperature_uom =  lower_case_keys['testtemperature_uom']
                                 
                                if report_test_temperature != test_temperature:
                                    table_matches_test = False
                                test_label = ', '.join([test_label, f'Test Temperature {str(test_temperature)} {test_temperature_uom}'])
                            if table_matches_test == True:
                                test_label = '✨' + test_label
                                predicted_test_idx = num_tests
                            test_labels.append(test_label)
                            num_tests = num_tests+1

                        
                        
                        selected_test_label = st.selectbox('Select Test', test_labels, index=predicted_test_idx,key='select_test')

                        # update the root of the json object to match the api endpoint for the selected table type
                        endpoint_table_json = table_json.copy()
                        endpoint_table_json[endpoint] = endpoint_table_json.pop(header_table_name)
                        if selected_test_label == 'Select Test':
                            test_number = num_tests
                            table_json['TestNumber'] = test_number
                            test_id = ''

                            if st.button('create test'):
                                response_code, response_text = fdDataAccess.create_test(endpoint_table_json,report_sample_id,endpoint, dig.auth)
                                if response_code < 300:
                                    st.rerun()
                                else:
                                    if response_text is not None:
                                        st.error(response_text)
                                    else:
                                        st.error(response_code)

                        if selected_test_label not in ['Select Test']:
                            test_number = test_labels.index(selected_test_label) 
                            table_json['TestNumber'] = test_number
                            selected_test = fdCommon.try_get(test_list,test_number-1,None)
                            test_id = fdCommon.try_get(selected_test,'ID','')
                            if st.button('update test'):
                                response_code, response_text = fdDataAccess.update_test(table_json,report_sample_id, test_id,endpoint, dig.auth)
                                if response_code < 300:
                                    st.rerun()
                                else:
                                    st.error(response_text)
                    
#============================================================================================================================================================
#
# Normalized tab - allows user view the json output, same as stored to api
#
#============================================================================================================================================================


                    elif dig.tab == 'Normalized':
                        
                            
                    #if st.button('Save JSON'):
                        header_table_name = cfg.tables_df[(cfg.tables_df['Table']==dig.selected_table_obj.table_type)]['HeaderTable'].values[0]
                        column_table_name = cfg.tables_df[(cfg.tables_df['Table']==dig.selected_table_obj.table_type)]['ChildTable'].values[0]
                        
                        fdMapping.normalize_table_data(dig.selected_table_obj, cfg)
                        table_json = fdNormalization.table_data_to_json(dig.selected_table_obj, cfg.tables_df)
                        dig.selected_table_obj.table_json = table_json
                        json_output_str = json.dumps(table_json, indent=4, default=fdCommon.np_encoder)
                        with DATA_COL:
                            st.json(json_output_str)   

#============================================================================================================================================================
#
# Manage Samples tab - allows user to the view, modify, add and delete samples, including recombing existing samples or adding manually if there are no
#                      samples in the report data
#
#============================================================================================================================================================



    if view == 'Manage Samples':
            
        if st.button('Refresh Sample List'):
            dig.report_obj.samples = fdReport.get_report_samples(dig,dig.report_obj.report_name, cfg)
            st.rerun()
        report_samples = dig.report_obj.samples
        if 'Selected' not in report_samples.columns:
            report_samples.insert(0, column='Selected', value=False) 

        # if 'RecombinedSamples' not in report_samples.columns:
        #     report_samples['RecombinedSamples'] = ''
        report_samples_table = st.data_editor(report_samples,hide_index=True)

        # Select rows
        row_nums = []
        for i, row in report_samples_table.iterrows():
            if row['Selected'] == True:
                row_nums.append(i)    
        report_samples = report_samples.drop('Selected', axis=1)
        
            # Create recombined_sample_fraction_df for each selected row
        recombined_sample_fraction_df = pd.DataFrame(columns=[
            'RecombinedSampleID', 'FluidSampleID', 'VolumeFraction',
            'VolumeFraction_UOM', 'MassFraction', 'MassFraction_UOM',
            'MoleFraction', 'MoleFraction_UOM', 'Remark'
        ])

        if st.button('Combine Samples'):
            if len(row_nums) > 1:
                # Concatenate 'SampleID' values using '_' as delimiter
                report_samples = report_samples.astype({"FluidSampleID": str, "FluidSampleContainerID": str})
                
                recombined_fluid_sample_id = ','.join(report_samples.iloc[row_nums]['FluidSampleID'])
                recombined_fluid_sample_container_id = ','.join(report_samples.iloc[row_nums]['FluidSampleContainerID'])
                # Set 'SampleKind' column to 'recombined'
                sample_kind = 'recombined'

                # Determine values for other columns based on specified conditions
                other_columns_values = {}
                for column in report_samples.columns:
                    if column not in ['FluidSampleID', 'FluidSampleContainerID','SampleKind']:
                        values = report_samples.loc[row_nums, column]
                        # Use set to find unique values, considering dictionaries as strings
                        unique_values = set(json.dumps(value, sort_keys=True, default=fdCommon.np_encoder) if isinstance(value, dict) else value for value in values)

                        if len(unique_values) == 1:
                            # If values are identical, use the value
                            other_columns_values[column] = values.iloc[0]
                        else:
                            # If values are different, set to None
                            other_columns_values[column] = None

                # Check if the new SampleID already exists in the DataFrame
                if recombined_fluid_sample_id not in report_samples['FluidSampleID'].values:
                    # Create a new DataFrame row
                    new_row = pd.DataFrame({
                        'FluidSampleID': [recombined_fluid_sample_id],
                        'FluidSampleContainerID': [recombined_fluid_sample_container_id],
                        'SampleKind': [sample_kind],
                        'Remark':['Combined by user']
                    })

                    # Assign values for other columns
                    for column, value in other_columns_values.items():
                        new_row[column] = [value]


                    recombined_sample_fractions = []
                    # Create or update recombined_sample_fraction_df DataFrame
                    for idx in row_nums:
                        fluid_sample_id = report_samples.loc[idx, 'FluidSampleID']
                        sample_fraction = {
                            'RecombinedFluidSampleID': [recombined_fluid_sample_id],
                            'FluidSampleID': [fluid_sample_id],
                            'VolumeFraction': [None],
                            'VolumeFraction_UOM': [None],
                            'MassFraction': [None],
                            'MassFraction_UOM': [None],
                            'MoleFraction': [None],
                            'MoleFraction_UOM': [None],
                            'Remark': ['Combined by user']
                        }
                        recombined_sample_fractions.append(sample_fraction)
                        recombined_sample_fraction_df = pd.concat([recombined_sample_fraction_df, pd.DataFrame(sample_fraction)], ignore_index=True)

                    response_code, response_text = fdTestData.save_sample(dig.report_obj,new_row, dig.auth)
                    st.info(f'Add Sample: {response_text}')
                    dig.report_obj.samples = fdReport.get_report_samples(dig,dig.report_obj.report_name, cfg)
                    report_samples = pd.concat([report_samples,new_row])

                    st.rerun()
                else:
                    st.warning(f"SampleID '{recombined_fluid_sample_id}' already exists in the table")


            else:
                st.warning('Select two or more rows to combine')

                
        if st.button('Delete Sample', args=[row_nums]):
            with st.spinner('Deleting sample(s)'):
                try:
                    if len(row_nums) > 0:
                        report_samples = dig.report_obj.samples
                        for idx in reversed(row_nums):
                            if 'ID' in report_samples:
                                ID = report_samples.iloc[idx]['ID']
                                response_status, response_text = fdDataAccess.delete_sample(ID, dig.auth.token, dig.auth.tenant_id, dig.auth)
                                if response_status > 299:
                                    st.error(f'Delete failed: {response_text}')
                                    st.stop()
                        dig.report_obj.samples = fdReport.get_report_samples(dig,dig.report_obj.report_name, cfg)
                        st.rerun()
                except Exception as e:
                    st.error(f'Delete failed: {e}')
                    print(f'Delete failed: {e}')
                    st.stop()


        if st.button('Save Samples', args=[report_samples_table]):
            with st.spinner('Updating sample(s)'):
                try:
                    for r, row in report_samples.iterrows():
                        row_df = pd.DataFrame(report_samples.iloc[r]).transpose()
                        if row_df.empty == False:
                            row_df = fdMapping.normalize_column_df(row_df, 'Sample', cfg)
                            response_code, response_text = fdTestData.save_sample(dig.report_obj,new_row, dig.auth)
                            st.info(f'Add Sample: {response_text}')
                    dig.report_obj.samples = fdReport.get_report_samples(dig,dig.report_obj.report_name, cfg)
                    st.rerun()
                except Exception as e:
                    st.error(f'Update failed: {e}')
                    print(f'Update failed: {e}')

        if len(dig.report_obj.samples.index) == 0:
            if st.button('Add Sample'):
                new_row = pd.DataFrame({
                            'FluidSampleID': '1',
                            'FluidSampleContainerID': 'Unknown',
                            'SampleKind': '',
                            'Remark':'Added by user'
                        },index=[0]
                        )
                
                response_code, response_text = fdTestData.save_sample(dig.report_obj,new_row, dig.auth)
                st.info(f'Add Sample: {response_text}')
                dig.report_obj.samples = fdReport.get_report_samples(dig,dig.report_obj.report_name, cfg)
                st.rerun()

    if view == 'Report Tables':
        report_tables_list = []
        for t in dig.report_obj.tables:

            report_tables_list.append({'Page':t.page, 
                                        'Table Number':t.page_table_number, 
                                        'Predicted Table Type':t.predicted_table_type,
                                        'Table Type':t.table_type,
                                        'Status':t.table_status,
                                        'Predicted Header':t.predicted_header,
                                        'Predicted Transpose':t.predicted_transposed,
                                        })
        report_tables_df = pd.DataFrame.from_dict(report_tables_list)
        st.dataframe(report_tables_df, height=1200, use_container_width=True)
try:    
    # End profiling
    if st.session_state.profiler.is_running: 
        st.session_state.profiler.stop()
        st.session_state.profiler.print()
except:
    print('profiler stop failed')     