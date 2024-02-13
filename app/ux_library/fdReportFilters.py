import streamlit as st

def do_report_filter_flow(report_list_df):
    #  define report filters
    auth = st.session_state['auth']
    cfg = st.session_state['configuration']
    FILTER_EXP = st.sidebar.expander("Filters")
    with FILTER_EXP:

        # filter by source batch 
        source_filter_list = report_list_df['Source'].unique()
        source_filter_list = [str(x) for x in source_filter_list]
        source_filter_list.sort()
        source_filter = st.multiselect("Select Source(s)",source_filter_list, default=None)

        if len(source_filter) > 0:
            report_list_df = report_list_df[report_list_df['Source'].isin(source_filter)] 

        # filter by lab name
        lab_filter_list = report_list_df['LabName'].unique()
        lab_filter_list = [str(x) for x in lab_filter_list]
        lab_filter_list.sort()
        lab_filter = st.multiselect("Select Lab(s)",lab_filter_list, default=None)

        if len(lab_filter) > 0:
            report_list_df = report_list_df[report_list_df['LabName'].isin(lab_filter)] 

        # filter by asset
        asset_filter_list = report_list_df['AssetName'].unique()
        asset_filter_list = [str(x) for x in asset_filter_list]
        asset_filter_list.sort()
        asset_filter = st.multiselect("Select Asset(s)",asset_filter_list, default=None)

        if len(asset_filter) > 0:
            report_list_df = report_list_df[report_list_df['AssetName'].isin(asset_filter)]    
        
        # filter by field name
        field_filter_list = report_list_df['FieldName'].unique().tolist()
        field_filter_list = [str(x) for x in field_filter_list]
        field_filter_list.sort()
        field_filter = st.multiselect("Select Field(s)",field_filter_list)
        
        if len(field_filter) > 0:
            report_list_df = report_list_df[report_list_df['FieldName'].isin(field_filter)]    
            
        # filter by reservoir name
        reservoir_filter_list = report_list_df['ReservoirName'].unique().tolist()
        reservoir_filter_list = [str(x) for x in reservoir_filter_list]
        reservoir_filter_list.sort()
        reservoir_filter = st.multiselect("Select Reservoir(s)",reservoir_filter_list)

        if len(reservoir_filter) > 0:
            report_list_df = report_list_df[report_list_df['ReservoirName'].isin(reservoir_filter)]    
        
        # filter by well name
        well_filter_list = report_list_df['WellName'].unique().tolist()
        well_filter_list = [str(x) for x in well_filter_list]
        well_filter_list.sort()
        well_filter = st.multiselect("Select Well(s)",well_filter_list)

        if len(well_filter) > 0:
            report_list_df = report_list_df[report_list_df['WellName'].isin(well_filter)]    

        # # filter by table type 
        # table_type_filter = st.multiselect("Select Table Types(s)",tables_dropdown_list)

        # if len(table_type_filter) > 0:
        #     report_list_df = report_list_df[report_list_df['Tablename'].isin(table_type_filter)]  

    # display count of active filters
    filter_count = len(asset_filter) + len(field_filter) + len(reservoir_filter) + len(well_filter) + len(source_filter) + len(lab_filter)# + len(table_type_filter)
    if filter_count > 0:
        st.sidebar.write('%s filter(s) applied' % filter_count)  
    return report_list_df
    