import streamlit as st
import pandas as pd

def do_page_navigation_flow(dig, selected_page_index):
                if 'page_selector' not in st.session_state:
                    st.session_state.page_selector = 0
            
                st.sidebar.write('Navigate by:')
                PAGE_NAVIGATION, MAPPED_TABLES_NAVIGATION, SEARCH_NAVIGATION = st.sidebar.tabs(['Pages', 'Mapped Tables', 'Search'])
                with PAGE_NAVIGATION:
                    PAGE_CONT = st.container()
                with MAPPED_TABLES_NAVIGATION:
                    mapped_tables = []
                    for t in dig.report_obj.tables:
                        if t.table_type not in ('Select Table Type', 'Unknown'):
                            mapped_tables.append((t.page,t.table_type))
                    mapped_tables =  pd.DataFrame(mapped_tables, columns=['Page', 'TableName'])                   
                    for r, row in mapped_tables.iterrows():
                        with st.container():
                            MT_COLS = st.columns([4,1])
                            with MT_COLS[0]:
                                st.write(row['TableName'])
                            with MT_COLS[1]:
                                if st.button(f"**{row['Page']}**", key = f'mapped{r}'):
                                    go_page = row['Page']
                                    st.session_state.page_selector = int(go_page)

                with SEARCH_NAVIGATION:
                    search = st.text_input("Enter text to search for")
                    if search != '':
                        # if dig.report_obj.extracted_text is None:
                        #     dig.report_obj.extracted_text = load_extracted_file_text_df(extracted_text_file_path, dig.report_obj.report_name  + '.csv', source)

                        if dig.report_obj.extracted_text.empty == False:
                            search_results_df = dig.report_obj.extracted_text[dig.report_obj.extracted_text['text'].str.contains(search.lower(), case=False)]
                            
                            for r, row in search_results_df.iterrows():
                                with st.container():
                                    RESULT_COL = st.columns([4,1])
                                    with RESULT_COL[0]:
                                        text = row['text']
                                        #st.write(text)
                                        text_limit = 100
                                        if len(text) > text_limit:
                                            TEXT = st.container()
                                            if st.button('more...', key='more'+str(r)):
                                                with TEXT:
                                                    st.write(text)
                                            else:
                                                with TEXT:
                                                    st.write(text[0:100])
                                        else:
                                            st.write(text)
                                    with RESULT_COL[1]:
                                        if st.button(f"**{row['page']}**", key = f'search{r}'):
                                            go_page = row['page']
                                            st.session_state.page_selector = int(go_page) 
                with PAGE_CONT:
                    if dig.selected_page_index == -1:
                        default = 0
                        st.session_state.page_selector = 0
                    else:
                        default = dig.selected_page_index
                    PAGE_COL1, PAGE_COL2, PAGE_COL3, PAGE_COL4 = st.columns([3,1,1,1])  
                    with PAGE_COL2:
                        if st.button('<'):
                            st.session_state.page_selector = max(st.session_state.page_selector-1,0,)
                    with PAGE_COL4:
                        if st.button('\>'):
                            st.session_state.page_selector = min(st.session_state.page_selector+1,dig.pdf.page_count - 1)
                    with PAGE_COL3:
                        specified_page = st.text_input('Go to page:',value=st.session_state.page_selector,label_visibility='collapsed')
                    if specified_page != st.session_state.page_selector:
                        st.session_state.page_selector = int(specified_page)
                    with PAGE_COL1: 
                        selected_page_index = st.slider("Select Page Number", max_value=int(dig.pdf.page_count - 1),  step=1, key='page_selector',value=default)
                return selected_page_index