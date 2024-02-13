
import pandas as pd                                     
import re                                               
import streamlit as st                                  
import pandas as pd
from pyinstrument import Profiler 
from sqlalchemy import create_engine
import sys
sys.path.append("..")
from fluidsdata_common import DigitizationClasses

#
# This is a Streamlit app to display and update app configuration
# ToDo: error handling
# ToDo: logging

@st.cache_data()
def get_config(_da,source='local'):
# Read configuration from json file
    if source == 'local':
        config = _da.get_json_from_local("/Users/matthewburd/Python/DATA_DIGITIZATION_WORKFLOW/","config.json")
    else:
        config = _da.get_json_from_azure("DATA_DIGITIZATION_WORKFLOW/","config.json")
    return config
   

if 'profiler' not in st.session_state:
    st.session_state['profiler']=Profiler()
if not st.session_state.profiler.is_running: 
    st.session_state.profiler.reset()
    st.session_state.profiler.start()

st.set_page_config(
    page_title="FLUIDSDATA.COM",
    page_icon="💧",                                     
    layout="wide",
    initial_sidebar_state="expanded",)
print('***** Manage Configuration *****')
if 'DataAccess' not in st.session_state:
        st.session_state['DataAccess'] = DigitizationClasses.DataAccess()
da = st.session_state['DataAccess']
source = 'cloud'
config = get_config(da,source)
config_path = config['config_path']
output_path = config['output_path']
if 'configuration' not in st.session_state:
        st.session_state['configuration'] = DigitizationClasses.Configuration(config_path,source,st.session_state.DataAccess)
        st.session_state.configuration.load_configuration()
        st.session_state['table_status_df'] = st.session_state.configuration.load_table_status_df(da,output_path,"/table_status.csv", source)
cfg = st.session_state.configuration




if st.sidebar.button("Reload Configuration"):
    cfg.load_configuration()
    st.experimental_rerun()

#
# Configuration pages
#
CNF_TAB1,CNF_TAB2,CNF_TAB3,CNF_TAB4,CNF_TAB5,CNF_TAB6,CNF_TAB7,CNF_TAB8,CNF_TAB9, CNF_TAB10 = st.tabs(['Tables', 'Table Columns','Table Mapping', 'Column Mapping', 'Table Status', 'UOMs', 'UOM Mapping','Components','ComponentMapping','Validation Rules'])
with CNF_TAB1:
    tables_editor = st.data_editor(st.session_state['tables_df'].sort_values(by=['Table']),num_rows='dynamic',use_container_width=True,key='cnf001')
    if st.button('Save Table Configuration'):
        st.session_state['tables_df'] = tables_editor
        st.session_state.DataAccess.write_csv(st.session_state['tables_df'],config_path,"/pvt_tables.csv", source)
        cfg.load_configuration()
with CNF_TAB2:
    table_columns_editor = st.data_editor(st.session_state['table_columns_df'].sort_values(by=['Table']),num_rows='dynamic',use_container_width=True)
    if st.button('Save Table Column Configuration'):
        st.session_state['table_columns_df'] = table_columns_editor
        st.session_state.DataAccess.write_csv(st.session_state['table_columns_df'],config_path,"/pvt_table_columns.csv", source)
        cfg.load_configuration()
with CNF_TAB3:
    table_mapping_editor = st.data_editor(st.session_state['table_mapping_df'].sort_values(by=['Table']),num_rows='dynamic',use_container_width=True)
    if st.button('Save Table Mapping Configuration'):
        st.session_state['table_mapping_df'] = table_mapping_editor
        st.session_state.DataAccess.write_csv(st.session_state['table_mapping_df'],config_path,"/pvt_table_mapping.csv", source)
        cfg.load_configuration()
with CNF_TAB4:
    column_mapping_df = st.session_state['column_mapping_df'].copy()
    column_mapping_df.insert(0, column='Selected', value=False)
    column_mapping_editor = st.data_editor(column_mapping_df.sort_values(by=['Table']),num_rows='dynamic',use_container_width=True)
    if st.button('Save Column Mapping Configuration'):
        st.session_state['column_mapping_df'] = column_mapping_editor.drop(columns=['Selected'])
        st.session_state.DataAccess.write_csv(st.session_state['column_mapping_df'],config_path,"/pvt_column_mapping.csv", source)
        cfg.load_configuration()
with CNF_TAB5:
    # allow filtering by report name
    # ToDo" extend to other relevant fields in every page
    report_list = ['select report']
    report_list  = report_list + (st.session_state.table_status_df['Report'].unique().tolist())
    selected_report = st.selectbox('Report filter',report_list)
    if selected_report != 'select report':
        filtered_table_status_df = st.session_state.table_status_df[st.session_state.table_status_df['Report']==selected_report]
    else:
        filtered_table_status_df = st.session_state.table_status_df    
    table_status_editor = st.data_editor(filtered_table_status_df.sort_values(by=['Report','Page','TableNumber']),use_container_width=True)#,num_rows='dynamic')
    if st.button('Save Table Status'):
        st.session_state['table_status_df'] = table_status_editor
        st.session_state.DataAccess.write_csv(st.session_state['table_status_df'],output_path,"/table_status.csv", source)
        cfg.load_configuration()
        


with CNF_TAB6:
    # Allow the user to sort the DataFrame by any column
    sort_by_column = st.selectbox('Sort DataFrame by column:', st.session_state['uom_df'].columns)
    col_cfg = {'PVT_applicable':st.column_config.CheckboxColumn(default=False)}
    uom_editor = st.data_editor(st.session_state['uom_df'].sort_values(by=[sort_by_column],key=lambda x: x.str.lower()),use_container_width=True,num_rows='dynamic', hide_index=True, column_config=col_cfg)
    if st.button('Save UOMs'):
        st.session_state['uom_df'] = uom_editor
        st.session_state.DataAccess.write_csv(st.session_state['uom_df'],config_path,"/pvt_uom.csv", source)
        cfg.load_configuration()
        
with CNF_TAB7:
    
    st.info('Map unit of measure strings discovered in reports to the corresponding standard unit of measure names. Use the UOM Browser to search for standard UOMs then type or copy the UOM name into the UOM column.')
    COL1, COL2 = st.columns([3,2])
    uom_df = st.session_state['uom_df'].sort_values(by=[sort_by_column],key=lambda x: x.str.lower())
    with COL2:
        st.subheader('UOM Browser')
        uom_list = ['']
        uom_list = uom_list + st.session_state['uom_df']['symbol']
        uom_list = uom_list.sort_values()
        uom_type_list = st.session_state['uom_df']['dimension'].unique()
        uom_type_list.sort()
        uom_type = st.multiselect('select UOM type(s)',uom_type_list)
        uom_search = st.text_input('search for UOM')
        if len(uom_type) > 0:
            uom_df = uom_df[uom_df['dimension'].isin(uom_type)]
        if uom_search != '':
            uom_df = uom_df[(uom_df['name'].str.contains(uom_search)) | (uom_df['symbol'].str.contains(uom_search))]
        col_cfg = {'category':None,'isSI':None,'baseUnit':None,'conversionRef':None,'isExact':None,'A':None,'B':None,'C':None,'D':None,'underlyingDef':None,'description':None}
        #col_cfg = {'id':None,'dimensional_class':None,'PVT_applicable':None, 'aliases':None}
        st.data_editor(uom_df, hide_index = True, disabled=True, column_config=col_cfg)
        filter_count = len(uom_df.index)
        total_count = len(st.session_state['uom_df'].index)
        st.write(f'Showing {filter_count} of {total_count} records')
    with COL1:
        uom_col_cfg = st.column_config.SelectboxColumn('UOM',options=uom_list)
        #mult_col_cfg = st.column_config.Column('Multiplier',width=20)
        uom_mapping_editor = st.data_editor(st.session_state['uom_mapping_df'].sort_values(by=['ReportUOM'],key=lambda x: x.str.lower()),use_container_width=True, hide_index=True, column_config={'UOM':uom_col_cfg},height=500)

        if st.button('Save UOM mapping'):
            st.session_state['uom_mapping_df'] = uom_mapping_editor
            st.session_state.DataAccess.write_csv(st.session_state['uom_mapping_df'],config_path,"/pvt_uom_mapping.csv", source)
            cfg.load_configuration()
        if st.button('find source'):
            uom_source = st.session_state.column_mapping_df[st.session_state.column_mapping_df['UOM']=='??']
            st.write(uom_source)


with CNF_TAB8:
    
    component_editor = st.data_editor(st.session_state['component_df'].sort_values(by=['name']),use_container_width=True,num_rows='dynamic', hide_index=True)
    if st.button('Save components'):
        st.session_state['component_df'] = component_editor
        st.session_state.DataAccess.write_csv(st.session_state['component_df'],config_path,"/pvt_components.csv", source)
        cfg.load_configuration()

with CNF_TAB9:
    component_list = [' '] 
    component_list = component_list + st.session_state['component_df']['name']
    col_cfg = st.column_config.SelectboxColumn('Component',options=component_list)

    component_mapping_editor = st.data_editor(st.session_state['component_mapping_df'].sort_values(by=['ReportComponent'],key=lambda x: x.str.lower()),use_container_width=True, hide_index=True, column_config={'Component':col_cfg})
    if st.button('Save component mapping'):
        st.session_state['component_mapping_df'] = component_mapping_editor
        st.session_state.DataAccess.write_csv(st.session_state['component_mapping_df'],config_path,"/pvt_component_mapping.csv", source)
        cfg.load_configuration()

with CNF_TAB10:
    
    validation_rules_editor = st.data_editor(st.session_state['validation_rules_df'].sort_values(by=['Name']),use_container_width=True,num_rows='dynamic', hide_index=True)
    if st.button('Save validation_ruless'):
        st.session_state['validation_rules_df'] = validation_rules_editor
        st.session_state.DataAccess.write_csv(st.session_state['validation_rules_df'],config_path,"/pvt_validation_rules_df.csv", source)
        cfg.load_configuration()

try:    
    if st.session_state.profiler.is_running: 
        st.session_state.profiler.stop()
        st.session_state.profiler.print()
except:
        print('profiler stop failed')     