import pandas as pd
class DigitizationSession:
    def __init__(self, report):
        self.report = report
        self.config = None
        self.pdf = None
        
        # st.session_state['tables'] = pd.DataFrame(columns=['page','table','df'])
        # st.session_state['tables_dict'] = {}
        # st.session_state['row_cols'] = ''
        # st.session_state['new_col_dict'] = {}
        # st.session_state['report_fields'] = {}
        
        self.file_list = []
        self.extracted_page_text_df = None
        self.selected_file = ''
        self.selected_page_index = -1
        self.selected_table_number = 1
        self.uom_dimensions = {}
        self.has_uoms = {}
        self.uom_df = pd.DataFrame()
        self.selected_table_obj = None
        self.report_obj = None
        self.columns_dropdown_list = []
        #st.session_state['selected_table_status_df'] = None

        self.header_df = pd.DataFrame()
        
        self.columns_df = pd.DataFrame()
        
        self.columns_dropdown_list = []
        self.has_uoms =  {}
        self.uom_dimensions =  {}

        #st.session_state['header'] = []
        self.selected_table_type = 'Unknown'
        self.tab = 'Edit'
        #st.session_state['show_all_columns'] = False
        self.page_tables = []
        self.comp_mapping = ''
        self.da = None
        self.auth = None
        self.config_path = None
        self.uploaded_pdf_path = None
        self.processing_pdf_path = None
        self.processed_pdf_path = None
        self.json_input_path = None
        self.csv_output_path = None
        self.json_output_path = None
        self.object_store_path = None
        self.extracted_text_file_path = None