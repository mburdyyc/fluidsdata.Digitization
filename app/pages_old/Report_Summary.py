                                         
import streamlit as st                                  
from datetime import date                               
from pyinstrument import Profiler 
import DigitizationClasses

#
# This is a Streamlit app to display status and statistics of digitized reports
# ToDo: make customer-specific
# ToDo: error handling
# ToDo: logging
@st.cache_data()
def get_config(source='local'):
# Read configuration from json file
    if source == 'local':
        config = st.session_state.DataAccess.get_json_from_local("/Users/matthewburd/Python/DATA_DIGITIZATION_WORKFLOW/","config.json")
    else:
        config = st.session_state.DataAccess.get_json_from_azure("DATA_DIGITIZATION_WORKFLOW/","config.json")
    return config

#
# INITIALIZE CONFIGURATION PARAMETERS
#
st.set_page_config(
    page_title="FLUIDSDATA.COM",
    page_icon="💧",                                     
    layout="wide",
    initial_sidebar_state="expanded",)

if 'profiler' not in st.session_state:
    st.session_state['profiler']=Profiler()
if not st.session_state.profiler.is_running: 
    st.session_state.profiler.reset()
    st.session_state.profiler.start()
print('***** Main App *****')
params = st.experimental_get_query_params()
print('params',params)
if 'DataAccess' not in st.session_state:
        st.session_state['DataAccess'] = DigitizationClasses.DataAccess()
da = st.session_state['DataAccess']

source = 'cloud'
config = get_config(source)
output_path = config['output_path']
config_path = config['config_path']
if 'configuration' not in st.session_state:
        st.session_state['configuration'] = DigitizationClasses.Configuration(config_path, source,st.session_state.DataAccess)

cfg = st.session_state['configuration'] 

#
#   CONFIGURE PAGE SELECTION SIDEBAR
#
today = date.today()                                    
d3 = today.strftime("%m/%d/%y")
st.sidebar.title("FluidsData PVT Data Digitization Tool - V2.1 (" + d3 + ")")

#region FILTERS
#  define report filters
FILTER_EXP = st.sidebar.expander("Filters")
with FILTER_EXP:
    filtered_table_status_df = st.session_state['table_status_df'].copy()
    source_filter_list = filtered_table_status_df['Source'].unique()
    source_filter_list = [str(x) for x in source_filter_list]
    source_filter_list.sort()
    
    source_filter = st.multiselect("Select Source(s)",source_filter_list, default=None)

    if len(source_filter) > 0:
        filtered_table_status_df = filtered_table_status_df[filtered_table_status_df['Source'].isin(source_filter)] 

    lab_filter_list = filtered_table_status_df['LabName'].unique()
    lab_filter_list = [str(x) for x in lab_filter_list]
    lab_filter_list.sort()
    
    lab_filter = st.multiselect("Select Lab(s)",lab_filter_list, default=None)

    if len(lab_filter) > 0:
        filtered_table_status_df = filtered_table_status_df[filtered_table_status_df['LabName'].isin(lab_filter)] 

    asset_filter_list = filtered_table_status_df['AssetName'].unique()
    asset_filter_list = [str(x) for x in asset_filter_list]
    asset_filter_list.sort()
    asset_filter = st.multiselect("Select Asset(s)",asset_filter_list, default=None)

    if len(asset_filter) > 0:
        filtered_table_status_df = filtered_table_status_df[filtered_table_status_df['AssetName'].isin(asset_filter)]    
    
    field_filter_list = filtered_table_status_df['FieldName'].unique().tolist()
    field_filter_list = [str(x) for x in field_filter_list]
    field_filter_list.sort()
    field_filter = st.multiselect("Select Field(s)",field_filter_list)
    
    if len(field_filter) > 0:
        filtered_table_status_df = filtered_table_status_df[filtered_table_status_df['FieldName'].isin(field_filter)]    
    
    reservoir_filter_list = filtered_table_status_df['ReservoirName'].unique().tolist()
    reservoir_filter_list = [str(x) for x in reservoir_filter_list]

    reservoir_filter_list.sort()
    reservoir_filter = st.multiselect("Select Reservoir(s)",reservoir_filter_list)

    if len(reservoir_filter) > 0:
        filtered_table_status_df = filtered_table_status_df[filtered_table_status_df['ReservoirName'].isin(reservoir_filter)]    
    
    well_filter_list = filtered_table_status_df['WellName'].unique().tolist()
    well_filter_list = [str(x) for x in well_filter_list]
    well_filter_list.sort()
    well_filter = st.multiselect("Select Well(s)",well_filter_list)

    if len(well_filter) > 0:
        filtered_table_status_df = filtered_table_status_df[filtered_table_status_df['WellName'].isin(well_filter)]    

# display count of active filters
filter_count = len(asset_filter) + len(field_filter) + len(reservoir_filter) + len(well_filter) + len(source_filter) + len(lab_filter)
if filter_count > 0:
    st.sidebar.write('%s filter(s) applied' % filter_count)    
#endregion    

total_record_count = len(st.session_state['table_status_df'].index)

filtered_record_count = len(filtered_table_status_df)
report_status_df = filtered_table_status_df.copy()
report_status_df = report_status_df[report_status_df['Tablename']!='Unknown']
report_status_df = report_status_df.drop(columns=['Updated','ReportDate','Source','TableKey','TableNumber','LabName'])
report_status_df = report_status_df.sort_values(by=['AssetName','FieldName','ReservoirName','WellName','Report','Page'])
filtered_table_count = len(report_status_df.index)

st.dataframe(report_status_df,column_order=('AssetName','FieldName','ReservoirName','WellName','Report','Page','Tablename','Status'))
st.write(f'Total Pages (filtered): {filtered_record_count}')
st.write(f'Total Tables (filtered): {filtered_table_count}')
xaxis = st.selectbox('select column to plot',('AssetName','FieldName','ReservoirName','WellName','Tablename'))
st.bar_chart(report_status_df[xaxis].value_counts())

try:    

    if st.session_state.profiler.is_running: 
        st.session_state.profiler.stop()
        st.session_state.profiler.print()
except:
    print('profiler stop failed')     