import streamlit as st
def ux_edited_data_editor_on_change():

    edits = st.session_state.edited_data_editor
    for row, edit in edits['edited_rows'].items():
        column = list(edit.keys())[0]
        value = edit[column]
        column_index = st.session_state.digitization_session.selected_table_obj.table_data_edited.columns.get_loc(column)
        st.session_state.digitization_session.selected_table_obj.table_data_edited.iat[row,column_index] = value

def ux_mapped_data_editor_on_change():

    edits = st.session_state.mapped_data_editor
    for row, edit in edits['edited_rows'].items():
        column = list(edit.keys())[0]
        value = edit[column]
        column_index = st.session_state.digitization_session.selected_table_obj.table_data_mapped.columns.get_loc(column)
        st.session_state.dig.selected_table_obj.table_data_mapped.iat[row,column_index] = value

def ux_decrement_tab(idx):
    idx = idx-1
    st.session_state.digitization_session.tab = st.session_state.tabs[idx]

def ux_increment_tab(idx):
    idx = idx+1
    st.session_state.digitization_session.tab = st.session_state.tabs[idx]

def ux_mark_inferred_type_option(option):
    if option == st.session_state.digitization_session.selected_table_obj.predicted_table_type:
        option = '✨' + option 
    return option