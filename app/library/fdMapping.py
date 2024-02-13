import pandas as pd
import numpy as np
import re
from fuzzywuzzy import fuzz, process
from library import fdCommon
from library import fdValidation
from library import fdConfiguration
from library import fdNormalization
from library import fdReport
import sys
import os
import copy
import uuid
from datetime import datetime
from PIL import Image

# this class holds the mapping between data extracted from the source report and the fluidsdata model
# in includes:
# original column: name of the column in the extracted data table, after editing including setting the table heading
# edited column: updated name for the original column, e.g. for readability. Defaults to original column name
# predicted column: predicted column name in the fd data model for the selected table type
# mapped column: column name in the fd model selected by or accepted by user. Defaults to predicted column
class TableColumnMapping:
    def __init__(self,original_column='', edited_column='', predicted_column='', mapped_column='', original_uom='', predicted_uom='=',
                 mapped_uom='', std_uom='', has_uom=False, uom_dimension=''):
        self.original_column = original_column
        self.edited_column = edited_column
        self.predicted_column = predicted_column
        self.mapped_column = mapped_column
        self.original_uom = original_uom
        self.predicted_uom = predicted_uom
        self.mapped_uom = mapped_uom
        self.std_uom = std_uom
        self.has_uom = has_uom
        self.uom_dimension = uom_dimension
        
def extract_float(text):
    numeric_text = ''.join(filter(lambda x: x.isdigit() or x in ['.', '-'], text))
    try:
        result = float(numeric_text)
        return result
    except ValueError:
        return None  # Or handle the case where the string cannot be converted to float

# ToDo: conflicts with extract_saturation_pressure_from_table. Need to rationalize
def extract_saturation_pressure(table, dig):
      
    #look for pressure labels in the table:
    # Iterate through DataFrame rows and check for matches
    saturation_pressure_labels = ['saturation', 'pressure','psat','dew point','bubble point','pb', 'dew point pressure']
    contains_saturation_pressure_label = table.table_data_edited.applymap(lambda x: any(text.lower() in str(x).lower() for text in saturation_pressure_labels))


    
    # look in the raw table for marker text, since it will be removed in the edited table
    indices_saturation_pressure_label = table.table_data_edited[contains_saturation_pressure_label.any(axis=1)].index
    # but the edited table has had row(s) promoted to header, so calculate the row offset
    offset = 0#dig.selected_table_obj.table_data_raw.index[0] - dig.selected_table_obj.table_data_edited.index[0]
    matched_label = fdCommon.check_terms_in_dataframe(table.table_data_raw, saturation_pressure_labels)
    pressure_kind = None
    if matched_label is not None: 
        if matched_label in ['dew point', 'dew point pressure']:
            pressure_kind = 'dew point'
        elif matched_label in ['bubble point', 'pb']:
            pressure_kind = 'bubble point'
    mapped_column_dict = {obj.mapped_column: obj for obj in dig.selected_table_obj.table_column_mappings}
    if 'StepPressure' in mapped_column_dict:
        psat_columns = mapped_column_dict['StepPressure'].original_column
        psat_uoms = mapped_column_dict['StepPressure'].original_uom
        if (len(indices_saturation_pressure_label) > 0) and (psat_columns is not None) and (psat_columns in dig.selected_table_obj.table_data_edited.columns):
            pressure = table.table_data_edited.loc[indices_saturation_pressure_label[0]+offset][psat_columns]
            pressure = extract_float(pressure) #remove text in case value is like psta=xxxx
            #update the step pressure in case value is like psta=xxxx
            table.table_data_edited.at[indices_saturation_pressure_label[0]+offset,psat_columns] = str(pressure) # data is still string, will be converted later
            return str(pressure), psat_uoms, pressure_kind
        # for r, row in mapped_headers.iterrows():

        #     if row['field_name'] == 'SaturationPressure':
        #         row['predicted_value'] = pressure
        #         if 'StepPressure_UOM' in table.table_data_mapped.columns:
        #             row['predicted_uom'] = table.table_data_mapped.iloc[0]['StepPressure_UOM']
    return None, None, None
def log_edit(auth, table, action,data=None):
    now = datetime.now()
    if data is not None:
        table.edit_log.append(f'{now}  {auth.user} {action} {data}')
    else:
        table.edit_log.append(f'{now}  {auth.user} {action}') 
# find the test temperature in page text
def extract_test_temperature(extracted_page_text):
            
    #look for temperature in page text:
    pattern = r'.*(?:at|@|=)\s*(\d+(?:\.\d+)?)\s*([^\s]+)'

    for text in extracted_page_text:
        print(text)
        match = re.search(pattern, str(text))
        if match:
            temperature = match.group(1)
            unit = match.group(2)
            
            return temperature, unit 
    return None, None

def get_default_sample_ID(dig):
    if 'ID' in dig.report_obj.samples.columns:
        if ('Default' in dig.report_obj.samples.columns): 
            for s, smpl in dig.report_obj.samples.iterrows():
                if (smpl['Default']==True):
                    return smpl['ID']
    return None
# Define function to check if a value is a non-blank string (can't be converted to a number)
def is_text(val):
    if val == '':
        return False
    try:
        float(val)
        return False
    except:
        return True

# Define function to check if a value is numeric
def is_number(val):
    if val == '':
        return False
    try:
        float(val)
        return True
    except:
        return False

# 
# Define function to find table headers in data frame rows by looking for rows that have most text fields
# Currently not used

def infer_header(df, dig):

# Count the number of cells containing non-numeric values in each row
    df = df.reset_index(drop=True)
    non_numeric_counts = df.apply(
        lambda x: sum(x.apply(is_text)), axis=1)
    num_col = len(df.columns)

    # Find the index of the row with the highest non-numeric count
    idx = non_numeric_counts.idxmax()
    
    # if the most non-numeric row is not the first, check the previous rows for merged text
    if idx > 0:
        for i in range(len(df.columns)-1):
            merged_text = str(df.iloc[idx-1,i]) + ' ' + str(df.iloc[idx-1,i+1])
            if merged_text in dig.extracted_page_text_df['text'].values: #does the merge term appear in a list of known text
                df.iloc[idx,i] = merged_text + ' ' + df.iloc[idx,i]
                df.iloc[idx,i+1] = merged_text + ' ' + df.iloc[idx,i+1]

    # check for non-unique column names in main row
    main_row = df.iloc[idx].to_list()
    main_row_unique = df.iloc[idx].unique()
    
    if len(main_row) > len(main_row_unique):
        # if the most non-numeric row is not the first, check the previous rows for merged text
        for i in range(len(df.columns)-1):
            merged_text = str(df.iloc[idx,i]) + ' ' + str(df.iloc[idx,i+1])
            if merged_text in dig.extracted_page_text_df['text'].values: #does the merge term appear in a list of known text
                df.iloc[idx,i] = merged_text + ' ' + df.iloc[idx+1,i]
                df.iloc[idx,i+1] = merged_text + ' ' + df.iloc[idx+1,i+1]

    # Delete all rows prior to the row with the highest non-numeric count
    df = df.loc[idx:].reset_index(
        drop=True)
    uoms = False
    # Set the column names to match the values in the row with the highest non-numeric count
    for c in df.columns:
        #check next row for UOMs
        if is_text(df[c].iloc[1]):
            #if df[c].iloc[1].startswith('(') and df[c].iloc[1].endswith(')'):
            uoms = True
                #df[c].iloc[0] = df[c].iloc[0] + ' ' + df[c].iloc[1]


    for c in df.columns:
    #check next row for UOMs
        if uoms == True:
            
            if is_text(df[c].iloc[1]):
                if str(df[c].iloc[1]).startswith('(') and df[c].iloc[1].endswith(')'):
                    df[c].iloc[0] = df[c].iloc[0] + ' ' + df[c].iloc[1]
                else:
                    df[c].iloc[0] = str(df[c].iloc[0]) + ' (' + str(df[c].iloc[1]) +')'
                
        if is_text(df[c].iloc[0]):
            df = df.rename(columns={c: df[c].iloc[0].replace('\n',' ')})  
    if uoms == True:
        df = df.drop([df.index[1]])  
    # Remove the row with the highest non-numeric count from the dataframe
    df = df.iloc[1:].reset_index(drop=True)
    
    # Find the columns where each value is null
    empty_cols = [col for col in df.columns if df[col].empty]
    # Drop these columns from the dataframe
    df.drop(empty_cols, axis=1, inplace=True)
    #drop empty rows
    df.dropna(axis=0,how='all',inplace=True)

    # df = df.loc[:, ~(df.columns == '') & df.columns.isnull().all()]
    return df

# find UOM names embedded in column names
def extract_uom(column_name, dig, column_mapping_df):
    #return ''
    uom = ''
    uom_set = set(column_mapping_df[column_mapping_df['Table']==dig.selected_table_obj.table_type]['UOM'].to_list())
    uom_list = list(uom_set)
    for u in uom_list:
        if (is_text(u)) and (str(u).lower() in str(column_name).lower()):
            if len(u) > len(uom):
                uom = u
    if uom == '':
    #find UOM embedded in column name
        if is_text(column_name):

            #remove footnotes in brackets
            regExp = "\(\s?\d\s?\)"

            column_name = re.sub(regExp,'', column_name)

            regExp = r"\(([^()]+)\)"

            try:
                parts = re.findall(regExp, column_name.lower())
                uom_part = parts[len(parts)-1]
                
            except:
                uom_part = ''

            if len(uom_part) > 0:
                uom = uom_part#[1:-1]
    #find UOM in first row\

    return uom

# extract uom from column name if applicable
def get_uom(original_column, mapped_col, dig, cfg, predicted_uom=''):
    uom = predicted_uom
    if dig.table_columns_config_df[(dig.table_columns_config_df['Field']==mapped_col) ]['HasUOM'].values[0] =='Y':
        extracted_uom = extract_uom(original_column, dig, cfg.column_mapping_df)
        if predicted_uom != '':
            uom = predicted_uom
        if extracted_uom != '':
            uom = extracted_uom
    # validate that uom is appropriate for column type
    if fdNormalization.get_standard_uom(uom, cfg.uom_mapping_df) != '':
        return uom
    else:
        return ''

# Define a function to check if a string contains an approximate match to any of the possible terms
def match_string(string, possible_terms, alphaOnly=False):
    if alphaOnly:
        string = re.findall("[\dA-Za-z -]*", string)[0]
    matches = process.extract(string.lower(), possible_terms, limit=1, scorer=fuzz.token_set_ratio)

    if matches and matches[0][1] >= 50:  # Set a threshold for fuzzy matching
        matched_term = matches[0][0]
        matched_substring = process.extractOne(string.lower(), [matched_term], scorer=fuzz.token_set_ratio)[0]
        return matched_term
    else:
        return ''#
  






# for a given column in a report table of a known type, try to determine the corresponding standard column name for the table type
def match_columns(col, mapping_df, table_type, mapped_col, mapped_uom, dig):

    if mapped_col == '':
        match = ''
        uom = ''
        #find previously save mapped columns, overwriting fuzzy matches. Assume a given field name cross all reports maps to a single standard column. Otherwise need to add different map configs
        try:
            uom = ''
            matches = mapping_df[(mapping_df['Table']==table_type) & (mapping_df['MappedColumn']==col) & (mapping_df['Rejected']!='Y')]


            
            for r, row in matches.iterrows():
                if row['Field'] in dig.columns_dropdown_list:
                    match = row['Field']
                    break
            #more than one match, likely different UOMs , choose the best match   
            for u in matches['UOM']:
                if (u != ''):
                    if len(u) > len(uom):
                        uom = u

            #match = matches['Field'].values[0]
            
        except:
            match = ''
            uom = ''
    else:
        match = ''
        uom = ''
   
    #if match == '':
    #    #infer column matching using fuzzy matching
    #    match = match_string(col, columns_df['Field'].values.tolist(), True)
    
    idx = 0
    if match != '':
        # find column names like xxx(H2) from composition tables and strip out the bracketed content e.g. H2 before looking up the column name
        if fdCommon.contains_parentheses(match): 
            pattern = r'\((.*?)\)'
            match = re.sub(pattern, '()', match)    

    if match == 'No Mapping':
        match = ''
        
    return match, uom
  

# Function to find matches, excluding empty text and terms
def count_matches(non_empty_text_list, terms):
    if not terms.strip() or not non_empty_text_list:
        return 0
    
    term_list = terms.split(',')
    return sum(term.lower().strip() in map(str.lower, non_empty_text_list) for term in term_list)

# infer the type of table on a page by looking for keywords in extracted page text
def infer_table_type(data_df,tables_df): # data_df contains all extracted text for the page, tables_df defines all standard tables and their relevant keywords (terms)
    match = ''
    idx = 0

    # Filter out empty strings from the text_list
    non_empty_text_list = list(filter(None, data_df['text']))

    # Apply the function to each row and create a new column 'match_count'
    tables_df['match_count'] = tables_df.apply(lambda row: count_matches(non_empty_text_list, row['Terms']), axis=1)

    # Exclude rows with empty terms or no matches
    tables_df = tables_df[(tables_df['match_count'] > 0) & (tables_df['Terms'].str.strip() != '')]

    # Check if the DataFrame is not empty before finding the type with the most matches
    if not tables_df.empty:
        # Find the type with the most matches
        max_match_row = tables_df.loc[tables_df['match_count'].idxmax()]

        print(f"Type with the most matches: {max_match_row['Table']}")
        print(f"Number of matches: {max_match_row['match_count']}")
    else:
        print("No matches found in the non-empty DataFrame.")
    for i, row in data_df.iterrows():
        
        for j, table in tables_df.iterrows():
            if str(table['Terms']) != '':
                for term in str(table['Terms']).split(","): #'Terms' is a comma separated list of terms that are relevant to a specific table type
                    term = term.removeprefix('  ').lower()
                    
                    if term.lower() in str(row['text']).lower():
                        match = table['Table']
                        
                        idx = j

                    if '!' in term: # ! indicates that the term should not be found in the text
                        term = term.replace('!','').strip()
                        text = str(row['text']).lower().strip()
                        if text.find(term) > -1:
                            
                            match = ''
                            idx = 0
                            break
            if match != '': 
               return match, idx+1
            
    return match, idx

def match_header(df, table_type, table_mapping_df):
    print(f'matching header for table: {table_type}')
    if table_type != '':
        mapping_df = table_mapping_df[table_mapping_df['Table']==table_type]
    else:
        mapping_df = table_mapping_df   
    mapping_df = mapping_df[mapping_df['Rejected']!='Y']
    
    gdf = df.reset_index(drop=True)
    gdf = gdf.drop(columns=fdCommon.find_empty_columns(gdf), axis=1)
    gdf_transposed = gdf.transpose()
    gdf_transposed = gdf_transposed.reset_index(drop=True)
    

    # Get the column names starting with 'col'
    col_names = [col for col in gdf.columns if col.startswith('col')]
    # Create a new column by concatenating the selected columns and replacing '\n' with a space
    gdf['row_key'] = gdf[col_names].apply(lambda row: '__'.join(str(val) for val in row).replace('\n', ' '), axis=1)


    # Apply the function to the 'text_field' column
    gdf_transposed['row_key'] = gdf_transposed[gdf_transposed.columns].apply(lambda row: '__'.join(str(val) for val in row).replace('\n', ' '), axis=1)

    mapping_df_expanded = mapping_df.assign(ColumnKeys=mapping_df['ColumnKeys'].str.split('\|\|')).explode('ColumnKeys')

    # Join DF1 and expanded DF2 based on row_key and column_keys
    merged_df = pd.merge(gdf, mapping_df_expanded, left_on='row_key', right_on='ColumnKeys')


    mapping_df = mapping_df.sort_values(by=['Table','ColumnKeys'], key=lambda x: x.str.len(), ascending=False)    
    for m, map in mapping_df.iterrows():
        ck = map['ColumnKeys']

        if 'nan' in ck:
            ck = ck.replace('nan','None')  
        if ck != '':
             
            row_nums = []
            keys = str(ck).split('||')
            #st.write('keys=',keys)
            match = False
            if str(ck).startswith('T_'):

                for r,row in gdf_transposed.iterrows():
                    for k in range(len(keys)):
                        transpose = True
                        keys[k] = keys[k].removeprefix('T_')
                    
                        if (r+k < len(gdf_transposed)): 
                            rk = gdf_transposed.loc[r+k,'row_key']
                            
                            if (rk.startswith('nan__nan__nan__nan')) & (keys[k].startswith('nan__nan__nan__nan')):
                                print('****')
                                print(rk)
                                print(keys[k])
                            if (rk != 'nan') and (rk == keys[k]):
                                row_nums.append(r+k)
                                match = True

                            else:
                                match = False

                                break
                    
                    if match == True:
                        print('matched',map['Table'], row_nums, transpose)
                        return map['Table'], row_nums, transpose, ck

                
            else:
                transpose = False
                
                for r, row in gdf.iterrows():
                    for k in range(len(keys)):
                        
                        if (r+k < len(gdf)) and (gdf.loc[r+k,'row_key'] != 'nan') and (gdf.loc[r+k,'row_key'] == keys[k]):

                            row_nums.append(r+k)
                            match = True
                            
                        else:
                            match = False
                            break
                    if match == True:
                        print('matched',map['Table'], row_nums, transpose)
                        return map['Table'], row_nums, transpose, ck
    print('didnt match')
    return '',[], False,''

def predict_table_type(table_data,table_type, dig, cfg):
    #use previously saved table type if known
    #if table is None:
    matched_type = ''
    row_nums = []
    transpose = False
    if table_type not in ['Select Table Type','Unknown'] :
        #predicted_table_type = table_type 
        matched_type, row_nums, transpose, column_keys = match_header(table_data, table_type, cfg.table_mapping_df) 
        
        #matched_type = predicted_table_type
        print('matched_type', matched_type)
        # if matched_type == '':
        #     matched_type, row_nums, transpose, column_keys = match_header(table_data, '', cfg)
        dig.selected_table_obj.transposed
        return table_type, row_nums, transpose, column_keys
    # elif len(st.session_state.selected_page_df.index) < 2:
    #     matched_type = ''
    #     row_nums = []
    #     transpose = False
    else:
        matched_type = ''
        #infer the table type from extracted raw page text and table keywords

        predicted_table_type, i = infer_table_type(dig.extracted_page_text_df,cfg.tables_df)
        predicted_table_type = ''

        #rewrite this part
        if predicted_table_type != '': #see if inferred table type matches a saved header
            matched_type, row_nums, transpose, column_keys = match_header(table_data, predicted_table_type, cfg.table_mapping_df)
        if matched_type == '': #if not, pick the first matching header
            matched_type, row_nums, transpose, column_keys = match_header(table_data, '', cfg.table_mapping_df)
        if matched_type == '': #no matching headers, go back to inferred
            matched_type = predicted_table_type  
    
    dig.selected_table_obj.transposed    

    # dig.selected_table_obj.predicted_transposed = transpose
    # dig.selected_table_obj.predicted_header = row_nums
    return matched_type, row_nums, transpose, column_keys

def map_table_columns(dig, cfg):
    #
    # Column Mapping
    # Allow user to map columns in selected report table to standard fields of the selected table type
    # Automatically map columns if possible using previous mappings for similar tables

    if dig.selected_table_obj.table_type == 'Sample':
        if 'Default' not in dig.selected_table_obj.table_data_edited.columns:
            dig.selected_table_obj.table_data_edited.insert(0, column='Default', value=False)  

    if len(dig.selected_table_obj.column_mapping) > 0:
        column_mappings = pd.DataFrame(dig.selected_table_obj.column_mapping)
    else:
        column_mappings = pd.DataFrame(columns = cfg.column_mapping_df.columns)

    if dig.selected_table_obj.table_column_mappings == []:
        for c in dig.selected_table_obj.table_data_edited.columns:
            orig_col = c.replace('\n',' ')
            dig.selected_table_obj.table_column_mappings.append(TableColumnMapping(original_column=orig_col, edited_column=orig_col))
    
    mapped_columns = []
    duplicate_col_idx = 1
    duplicate_columns = []
    c = 0
    for col  in dig.selected_table_obj.table_column_mappings:
        
        if col.original_column == 'Default':
            col.edited_column = 'Default'
            col.predicted_column = 'Default'
            col.mapped_column = 'Default'
            continue
        elif col.original_column == 'Selected':
            continue

        col.predicted_column, col.predicted_uom = match_columns(col.original_column, cfg.column_mapping_df, dig.selected_table_obj.table_type,'','', dig)   
        if col.predicted_column == 'No Mapping':
            col.predicted_column = ''
        if (col.mapped_column == '') and (col.predicted_column != ''):                                            
            col.mapped_column = col.predicted_column
            if col.predicted_uom != '':
                col.mapped_uom = col.predicted_uom
        
        if col.mapped_column == 'No Mapping':
            col.mapped_column = ''
        if (col.mapped_column is not None) and (col.mapped_column not in ['', 'No Mapping']):
            # create and populate uom text entry fields if applicable
            col.mapped_column = col.mapped_column.split(' [')[0] #remove embedded uom
            if dig.has_uoms[col.mapped_column] == 'Y':
                col.has_uom = True
                col.predicted_uom = get_uom(col.original_column,col.mapped_column, dig, cfg, col.predicted_uom)
                if col.original_uom == '':
                    col.original_uom = col.predicted_uom
            # composition-related fields need the component name added to them for uniqueness    
            if col.mapped_column.endswith('()'):
                col.mapped_column = col.mapped_column.removesuffix('()') + '(%s)'%col.original_column
            if col.predicted_column.endswith('()'):
                col.predicted_column = col.predicted_column.removesuffix('()') + '(%s)'%col.original_column

        new_row = pd.DataFrame([[dig.selected_table_obj.table_type,col.mapped_column,'Column',col.original_column,col.mapped_uom]] , columns=['Table', 'Field', 'FieldType', 'MappedColumn','UOM'] )   
        column_mappings = pd.concat([column_mappings,new_row]).drop_duplicates()
        
        if col.original_uom == '':
            col.mapped_uom = col.original_uom
        if (col.mapped_column is not None) and (col.mapped_column not in ['', 'No Mapping']):        
            col.std_uom = fdNormalization.get_standard_uom(col.original_uom, cfg.uom_mapping_df)
            if col.mapped_uom == '':
                col.mapped_uom = col.std_uom
            if fdCommon.contains_parentheses(col.mapped_column): #mapped column is a component
                col.mapped_column = col.mapped_column.split("(")[0] + '()'

        mapped_columns.append(col.mapped_column)
        c = c+1

  
    # Iterate through 'mapped_columns' by index to validate for duplicate column names
    
  
    # duplicate_columns = []
    # for i in range(len(mapped_columns)):
    #     for j in range(i+1,len(mapped_columns)):
    #         if (mapped_columns[i] == mapped_columns[j]) and (mapped_columns[i] != ''):
    #             # if duplicates are found can they be disambiguated by concatenating UOM?
    #             if mapped_uoms[i] and mapped_uoms[i] != 'n/a' and mapped_uoms[j] and mapped_uoms[j] != 'n/a':
    #                 mapped_columns[i] = f'{mapped_columns[i]} [{mapped_uoms[i]}]'
    #                 mapped_columns[j] = f'{mapped_columns[j]} [{mapped_uoms[j]}]'
    #             else:
    #                 duplicate_columns.append(f'Duplicate column name {mapped_columns[i]} mapped without distinct UOMs')
    # Creating a dictionary

    # cols_dict = dict(zip(original_columns, mapped_columns))
    # matches_dict = dict(zip(original_columns, predicted_columns))


    # update validation error list for duplicate columns
    # if len(duplicate_columns) > 0:
    #     dig.selected_table_obj.validation_errors['duplicate_columns']=duplicate_columns
    # else:
    #     dig.selected_table_obj.validation_errors.pop('duplicate_columns', None)
    
    dig.selected_table_type = dig.selected_table_obj.table_type

# find the sample number in page text
# ToDo: if samples have already been identified in the report, then look for those specific sample Ids instead of using patterns
def extract_sample_number(dig):
    sample_id = ''
    container_id = ''
    default_sample_id = get_default_sample_ID(dig)
    if default_sample_id is not None:
        sample_id = default_sample_id

    #compare samples listed in report against text on the page (sample and container are usually listed with each report table)
    for s, sample in dig.report_obj.samples.iterrows():
        if (sample['FluidSampleContainerID'] != 'unknown'): 
            if (sample['FluidSampleContainerID'] in dig.extracted_page_text_df['text']):
                sample_id = sample['ID']
        elif len(sample['FluidSampleID']) > 1: #don't match on simple sample number which can be repeated within a report. Unique sample ID may be like 1.02
            if (sample['FluidSampleID'] in dig.extracted_page_text_df['text']):
                sample_id = sample['ID']

    return sample_id, container_id

def get_sample_list(dig):
    IDs = []
    labels = []
    if 'ID' in dig.report_obj.samples:
        IDs = dig.report_obj.samples['ID'].tolist()
        IDs.insert(0,None)
        IDs = [str(value) for value in IDs]
        labels = (dig.report_obj.samples['FluidSampleID'] + ':' + dig.report_obj.samples['FluidSampleContainerID']).tolist()
        labels.insert(0,'Select Sample')
    return IDs, labels

def map_header_columns(mapped_headers, table, header_cfg, dig, cfg):
    
    if isinstance(mapped_headers,pd.DataFrame) == False:
        mapped_headers = pd.DataFrame()
    if mapped_headers.empty:
        mapped_headers = pd.DataFrame(columns=[
            'field_name',
            'predicted_uom',
            'uom',
            'mapped_uom',
            'value',
            'predicted_value',
            'std_uom',
            'has_uom'
        ])

    hdrs = mapped_headers['field_name'].tolist()
    predicted_hdr_uoms = mapped_headers['predicted_uom'].tolist()
    hdr_uoms = mapped_headers['uom'].tolist()
    mapped_hdr_uoms = mapped_headers['mapped_uom'].tolist()
    hdr_vals = mapped_headers['value'].tolist()
    predicted_hdr_vals = mapped_headers['predicted_value'].tolist()
    std_hdr_uoms = mapped_headers['std_uom'].tolist()

    has_uoms =  header_cfg['HasUOM'].tolist()                            
    
    for i,hdr in header_cfg.iterrows():
        if hdr['Field'] not in hdrs:
            hdrs.append(hdr['Field'])
            hdr_uoms.append('')
            hdr_vals.append('')
            predicted_hdr_uoms.append(None)
            predicted_hdr_vals.append(None)
            std_hdr_uoms.append('')
            mapped_hdr_uoms.append('')

    predicted_saturation_pressure, predicted_saturation_pressure_uom, predicted_saturation_pressure_kind = extract_saturation_pressure(table, dig)
    for i in range(len(hdrs)):
        # automatically populate header fields if possible
        if (predicted_hdr_vals[i] in ['','None']) or (predicted_hdr_vals[i] is None):
            if hdrs[i] == 'SampleID':
                predicted_sample_id, container_id = extract_sample_number(dig)
                sample_ids, sample_labels = get_sample_list(dig)
                if (str(predicted_sample_id) in sample_ids): 
                    predicted_hdr_vals[i] = predicted_sample_id
                else:
                    predicted_hdr_vals[i] = ''
            elif (hdrs[i] == 'SaturationPressure'):                               
                predicted_hdr_vals[i], predicted_hdr_uoms[i] = predicted_saturation_pressure, predicted_saturation_pressure_uom
            elif (hdrs[i] == 'SaturationPressure_Kind'):                               
                predicted_hdr_vals[i] = predicted_saturation_pressure_kind
            elif hdrs[i] == 'TestTemperature':
                predicted_hdr_vals[i], predicted_hdr_uoms[i] = extract_test_temperature(dig.extracted_page_text_df['text'].tolist())        
            else:
                predicted_hdr_vals[i] = None
                predicted_hdr_uoms[i] = None
        
        if (predicted_hdr_vals[i] is not None) and (hdr_vals[i] in ['','None']):
            hdr_vals[i] = predicted_hdr_vals[i]

        if has_uoms[i] == 'Y':
            if (predicted_hdr_uoms[i] is not None) and (hdr_uoms[i] == ''):
                hdr_uoms[i] = predicted_hdr_uoms[i]
        else:
            hdr_uoms[i] = 'n/a'
        std_hdr_uoms[i] = fdNormalization.get_standard_uom(hdr_uoms[i], cfg.uom_mapping_df)

    # Create output dataframe
    dig.selected_table_obj.header_data_mapped  = pd.DataFrame()
    # Add values and units of measure as new columns

    for c in range(len(hdr_vals)):
        dig.selected_table_obj.header_data_mapped[hdrs[c]] = [hdr_vals[c]]

        if hdr_uoms[c] != 'n/a':
            dig.selected_table_obj.header_data_mapped[hdrs[c] + '_UOM'] = [hdr_uoms[c]]

    mapped_headers = pd.DataFrame({
        'field_name': hdrs,
        'predicted_uom': predicted_hdr_uoms,
        'uom': hdr_uoms,
        'mapped_uom': hdr_uoms,
        'value': hdr_vals,
        'predicted_value': predicted_hdr_vals,
        'std_uom': std_hdr_uoms,
        'has_uom': has_uoms
    })

    return mapped_headers





def convert_composition_matrix_to_records(table_df):
    common_columns = []
    component_columns = []
    uom_columns = []
    common_df = pd.DataFrame()
    comps_df = None 
    # collect columns that aren't compositions
    for c in table_df.columns:

        if fdCommon.contains_parentheses(c) == False:
            common_columns.append(table_df[c])
            common_df[c] = table_df[c]
    # unpack matrix columns.
    # initially there is one column like 'vapor_fraction(<component name>)' and a corresponding uom column per component
    # each row represents a different test step
    # after unpacking, there will be 3 columns e.g.:
    #   vapor_fraction, containing the value
    #   fluidcomponentreference, containing the component name
    #   the corresponding uom
    # per component, per test step
    # columns in the original dataframe that are not related to components (e.g. the test pressure) need to be repeated for each component, per test step

    for c in table_df.columns:

        if fdCommon.contains_parentheses(c) == True:   
            # column is composition
            if '_UOM' not in c:
                
                # unpack the property name
                prop = c.split('(')[0] + '()'
                component_columns.append(prop) #track component column name without parantheses for later use
                # unpack the component name
                component = fdCommon.extract_text_inside_parentheses(c) # get the component name
                #component_col = dig.selected_table_df_obj.table_df_data_mapped[c] # get the component composition values
                comp_df = common_df.copy() # start with the common columns
                comp_df[prop] = table_df[c] # add the property name column
                comp_df['FluidComponentReference'] = component # add the component name column
                comp_df[prop + '_UOM'] = table_df[c + '_UOM'] # add the uom column

                # add each component's rows to an overall dataframe
                if comps_df is None:
                    comps_df = comp_df.copy()
                else:
                    comps_df = pd.concat([comps_df,comp_df])
            comps_df = comps_df.reset_index(drop=True)
    if comps_df is None:
        return table_df
    else:
        return comps_df


def normalize_table_data(table, cfg):

    if table.table_type in ['CVDComposition', 'CVDResidComposition', 'DLComposition', 'DLResidComposition','SeparatorComposition','SeparatorResidComposition']:
        table.table_data_mapped = convert_composition_matrix_to_records(table.table_data_mapped)
    table.table_data_normalized = table.table_data_mapped.copy()
    try:
        for c in table.table_data_mapped.columns:
            # if (c.endswith('_UOM')):
            #     for r, row in table.table_data_mapped.iterrows():
            #         table.table_data_mapped.iloc[r][c] = normalize_uom(row[c])
            if (c.endswith('_UOM') == False) & (c not in ['','Default']):
                val, text = fdValidation.validate_data_type(table.table_data_mapped,c, table.table_type, cfg)
                col_cfg = fdConfiguration.get_column_config(c,table.table_type, cfg)
                if col_cfg is None:
                    expected_data_type = 'str'
                else:
                    expected_data_type = col_cfg.iloc[0]['DataType']
                
                if expected_data_type =='date':
                    table.table_data_normalized[c] = pd.to_datetime(table.table_data_mapped[c], infer_datetime_format=True)
                    table.table_data_normalized[c] = table.table_data_normalized[c].astype('str')
                elif expected_data_type =='datetime':
                    table.table_data_normalized[c] = pd.to_datetime(table.table_data_mapped[c], infer_datetime_format=True)
                    table.table_data_normalized[c] = table.table_data_normalized[c].astype('str')
                elif expected_data_type =='time':
                    try:
                        table.table_data_normalized[c] = pd.to_datetime(table.table_data_mapped[c], infer_datetime_format=True)
                        table.table_data_normalized[c] = table.table_data_normalized[c].dt.time
                        table.table_data_normalized[c] = table.table_data_normalized[c].astype('str')
                    except:
                        table.table_data_normalized[c] = table.table_data_mapped[c].astype('str')
                elif expected_data_type =='float':
                        # Remove the space between 'E' and the exponent if needed
                        
                        table.table_data_normalized[c] = table.table_data_mapped[c].apply(fdCommon.string_convert_scientific_notation_to_float)
                        table.table_data_normalized[c] = pd.to_numeric(table.table_data_normalized[c], errors='coerce')

                        # Apply the function to format the numbers in the DataFrame
                        #table.table_data_normalized[c] = table.table_data_normalized[c].apply(format_number)

                elif expected_data_type.startswith('enum'):
                    table.table_data_normalized[c] = table.table_data_mapped[c].astype('str')
                elif expected_data_type == 'component':
                    for r, row in table.table_data_normalized.iterrows(): 
                        standard_comp = fdNormalization.get_standard_comp(row[c], cfg)
                        table.table_data_normalized.at[r,c] = standard_comp
                else:
                    table.table_data_normalized[c] = table.table_data_mapped[c].astype(expected_data_type)

                if table.table_data_normalized[c].dtype == 'int64':
                    table.table_data_normalized[c] = table.table_data_normalized[c].astype(int) 
                        
                
            elif c.endswith('_UOM') == True:
                standard_uom = fdNormalization.get_standard_uom(table.table_data_mapped.iloc[0][c], cfg.uom_mapping_df)
                table.table_data_normalized[c] = standard_uom
                # for r, row in normalized.iterrows():
                #     #print(i,table.table_data_mapped.iloc[i]['StepPressure'])
                #     standard_uom = get_standard_uom(row[c])
                    
                #     normalized.at[r,c] = standard_uom
                #     #value_col = c.removesuffix('_UOM')
                    #base_unit, converted_uom = convert_uom(standard_uom,table.table_data_mapped[value_col][r] )
                    #table.table_data_mapped.at[r,c] = base_unit
                    #table.table_data_mapped.at[r,value_col + '_original'] = table.table_data_mapped[value_col][r]
                    #table.table_data_mapped.at[r,value_col] = converted_uom


        table.table_data_normalized.replace({np.nan:None},inplace=True)
        table.table_data_normalized.replace({'':None},inplace=True)
        table.table_data_normalized.replace({'nan':None},inplace=True)
            
        table.header_data_normalized = table.header_data_mapped.copy()            
        
        for c in table.header_data_mapped.columns:
            if (c.endswith('_UOM') == False) & (c != 'Default'):
                val, text = fdValidation.validate_data_type(table.header_data_mapped ,c, table.table_type, cfg)
                col_cfg = fdConfiguration.get_column_config(c,table.table_type, cfg)
                expected_data_type = col_cfg.iloc[0]['DataType']
                if expected_data_type =='date':
                    table.header_data_normalized[c] = pd.to_datetime(table.header_data_mapped [c], infer_datetime_format=True)
                    table.header_data_normalized[c] = table.header_data_normalized[c].astype('str')
                elif expected_data_type =='datetime':
                    table.header_data_normalized[c] = pd.to_datetime(table.header_data_mapped [c], infer_datetime_format=True)
                    table.header_data_normalized[c] = table.header_data_normalized[c].astype('str')
                elif expected_data_type =='time':
                    table.header_data_normalized[c] = pd.to_datetime(table.header_data_mapped [c], infer_datetime_format=True)
                    table.header_data_normalized[c] = table.header_data_normalized[c].dt.time
                    table.header_data_normalized[c] = table.header_data_normalized[c].astype('str')
                elif expected_data_type =='float':
                        table.header_data_mapped[c] = table.header_data_mapped[c].apply(fdCommon.string_convert_scientific_notation_to_float)
                        table.header_data_normalized[c] = pd.to_numeric(table.header_data_normalized[c], errors='coerce')
                elif expected_data_type.startswith('enum'):
                    table.header_data_normalized[c] = table.header_data_normalized[c].astype('str')
                else:
                    table.header_data_normalized[c] = table.header_data_mapped[c].astype(expected_data_type)

            elif c.endswith('_UOM') == True:
                #value_col = c.removesuffix('_UOM')
                standard_uom = fdNormalization.get_standard_uom(table.header_data_mapped.iloc[0][c], cfg.uom_mapping_df)
                table.header_data_normalized.at[0,c] = standard_uom #dig.selected_table_obj.header_data_mapped .iloc[0][c]
                #dig.selected_table_obj.header_data_mapped .at[dig.selected_table_obj.header_data_mapped .index[0],c + '_original'] = standard_uom
                
            #     base_unit, converted_uom = convert_uom(standard_uom,dig.selected_table_obj.header_data_mapped [value_col][0] )
            #     dig.selected_table_obj.header_data_mapped .at[0,c] = base_unit
            #     dig.selected_table_obj.header_data_mapped .at[0,value_col + '_original'] = dig.selected_table_obj.header_data_mapped [value_col][0]
            #     dig.selected_table_obj.header_data_mapped .at[0,value_col] = converted_uom
        table.header_data_normalized.replace({np.nan:None},inplace=True)
        table.header_data_normalized.replace({'':None},inplace=True)
        table.header_data_normalized.replace({'nan':None},inplace=True)   
    except Exception as e:
        print(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)

def try_normalize_df_data_data_types(df, table_type, cfg,column_mappings = None):

    for c in df.columns:
        try:
        
            # if (c.endswith('_UOM')):
            #     for r, row in table.table_data_mapped.iterrows():
            #         table.table_data_mapped.iloc[r][c] = normalize_uom(row[c])
            if (c.endswith('_UOM') == False) & (c not in ['','Default']):
                #val, text = validate_data_type(table.table_data_mapped,c, table.table_type)

                if column_mappings is not None:
                    for col in column_mappings:
                        if (col.original_column == c) and (col.mapped_column != ''):
                            col_cfg = fdConfiguration.get_column_config(col.mapped_column,table_type, cfg)
                        else:
                            col_cfg = fdConfiguration.get_column_config(c,table_type, cfg)
                else:
                    col_cfg = fdConfiguration.get_column_config(c,table_type, cfg)
                if col_cfg is None:
                    continue
                else:
                    expected_data_type = col_cfg.iloc[0]['DataType']
                
                if expected_data_type =='date':
                    df[c] = pd.to_datetime(df, infer_datetime_format=True)
                    df[c] = df[c].astype('str')
                elif expected_data_type =='datetime':
                    df[c] = pd.to_datetime(df, infer_datetime_format=True)
                    df[c] = df[c].astype('str')
                elif expected_data_type =='time':
                    try:
                        df[c] = pd.to_datetime(df, infer_datetime_format=True)
                        df[c] = df[c].dt.time
                        df[c] = df[c].astype('str')
                    except:
                        df[c] = df[c].astype('str')
                elif expected_data_type =='float':
                        # Remove the space between 'E' and the exponent if needed
                        
                        df[c] = df[c].apply(fdCommon.string_convert_scientific_notation_to_float)
                        df[c] = pd.to_numeric(df[c], errors='coerce')

                        # Apply the function to format the numbers in the DataFrame
                        #df[c] = df[c].map(format_number)

                elif expected_data_type.startswith('enum'):
                    df[c] = df[c].astype('str')
                elif expected_data_type == 'component':
                    for r, row in df.iterrows(): 
                        standard_comp = fdConfiguration.get_standard_comp(row[c], cfg)
                        df.at[r,c] = standard_comp
                else:
                    df[c] = df[c].astype(expected_data_type)

        except Exception as e:
            print(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        df.replace({np.nan:None},inplace=True)
        df.replace({'':None},inplace=True)
        df.replace({'nan':None},inplace=True)
    return df


def update_mapped_sample_table(dig, table):
    sample_index = len(dig.report_obj.samples) + 1
    if 'FluidSampleID' not in table.columns:
        table['FluidSampleID'] = ''

    if 'FluidSampleContainerID' not in table.columns:
        table['FluidSampleContainerID'] = ''

    table.insert(1,'FluidSampleID',table.pop('FluidSampleID'))
    table.insert(2,'FluidSampleContainerID',table.pop('FluidSampleContainerID'))
    for i in range(len(table.index)):
        if table.iloc[i]['FluidSampleID'] == '':
            table.iat[i,1] = str(sample_index)
            sample_index = sample_index + 1
        if table.iloc[i]['FluidSampleContainerID'] == '':
            table.iat[i,2] = 'unknown'
    
    #table['SampleID'] = table['FluidSampleID'].map(str) + ':' + table['FluidSampleContainerID'].map(str)

        #re-order columns
    #table.insert(0,'SampleID',table.pop('SampleID'))

    return table

# Update the status of report table e.g. from saved to accepted
def write_table_status(da, report, page, tablename, status,updated, tablekey, tablenumber, source = 'cloud'):

    # Define the query using MERGE statement
    query = """
    MERGE INTO dbo.pvt_table_status AS target
    USING (VALUES (?, ?, ?, ?, ?, ?, ?)) AS source (Report, Page, Tablename, Status, Updated, TableKey, TableNumber)
    ON target.Report = source.Report AND target.Page = source.Page AND target.TableNumber = source.TableNumber
    WHEN MATCHED THEN
        UPDATE SET target.Tablename = source.Tablename, target.Status = source.Status,
                   target.Updated = source.Updated, target.TableKey = source.TableKey
    WHEN NOT MATCHED THEN
        INSERT (Report, Page, Tablename, Status, Updated, TableKey, TableNumber)
        VALUES (source.Report, source.Page, source.Tablename, source.Status, source.Updated, source.TableKey, source.TableNumber);
    """
    data = (report, page, tablename, status,updated, tablekey, tablenumber)
    
    da.update_azure_sql(query, data)

    # split an existing report table into 2 at a specified line number
def split_table(dig, table, row_num):
    
    new_table = copy.deepcopy(table)
    new_table.id = str(uuid.uuid4())
    page_table_number = 0
    for t in dig.report_obj.tables: # count existing tables on page
        if t.page == table.page:
            page_table_number = page_table_number + 1
    new_table.page_table_number = page_table_number
    new_table.status = 'Not set'
    new_table.table_data_mapped = pd.DataFrame()
    new_table.header_data_mapped = pd.DataFrame()
    new_table.table_data_normalized = pd.DataFrame()
    new_table.header_data_normalized = pd.DataFrame()
    new_table.table_data_saved = pd.DataFrame()
    new_table.table_json = {}
    new_table.column_mapping = []
    new_table.table_column_mappings = pd.DataFrame()
    new_table.split_from = table.id
    table.split_to = new_table.id
    new_table.table_data_raw = new_table.table_data_raw.iloc[table.table_data_raw.index.get_loc(row_num):]
    table.table_data_raw = table.table_data_raw.iloc[:table.table_data_raw.index.get_loc(row_num)]
    new_table.table_data_edited = new_table.table_data_edited.iloc[table.table_data_edited.index.get_loc(row_num):]
    table.table_data_edited = table.table_data_edited.iloc[:table.table_data_edited.index.get_loc(row_num)]
    dig.report_obj.tables.append(new_table)
    log_edit(dig.auth, table,'split table to', new_table.id)
    log_edit(dig.auth, new_table,'split table from', table.id)
    return new_table.page_table_number, new_table


# rejoin a previously split table
def unsplit_table(dig, table):
    delete = None
    for t in dig.report_obj.tables:
        if t.id == table.split_from:
            split_table = t
        # elif t.id == table.id:
        #     delete = t
    split_table.table_data_raw = pd.concat([split_table.table_data_raw, table.table_data_raw])
    split_table.table_data_edited = pd.concat([split_table.table_data_edited, table.table_data_edited])
    split_table.split_to = None
    dig.selected_table_obj = table
    dig.selected_page_number = table.page_table_number
    dig.report_obj.tables.remove(table)
    log_edit(dig.auth,table,'Unsplit table')

# make a copy of a table within a page
def copy_table(dig, table):
    
    new_table = copy.deepcopy(table)
    new_table.id = str(uuid.uuid4())
    page_table_number = 0
    for t in dig.report_obj.tables:
        if t.page == dig.selected_table_obj.page:
            page_table_number = page_table_number + 1
    new_table.page_table_number = page_table_number
    new_table.status = 'Not set'
    new_table.table_data_mapped = pd.DataFrame()
    new_table.header_data_mapped = pd.DataFrame()
    new_table.table_data_normalized = pd.DataFrame()
    new_table.header_data_normalized = pd.DataFrame()
    new_table.table_data_saved = pd.DataFrame()
    new_table.table_json = {}
    new_table.column_mapping = []
    new_table.copied_from = table.id
    
    dig.report_obj.tables.append(new_table)
    log_edit(dig.auth,table,'Copied table to', new_table.id)
    log_edit(dig.auth,new_table,'Copied table from', table.id)
    return new_table.page_table_number, new_table

def delete_copied_table(dig, table):
    if table.copied_from is not None:
        dig.report_obj.tables.remove(table)





# update the header of a table based on the contents of one or more rows
# for example, table may start out as col1, col2, etc.
# rows 0-3 of the table may contain extraneous data and should be ignored
# row 4 of the table may contain text that defines the actual meaning of each column
# row 5 of the table may be the uoms for each column
# in this example set_header would be called with row_nums = [4,5]
def set_table_column_headings(dig, cfg, df, row_nums):
    
    if dig.selected_table_obj.header != '': #header already set
        return df

    if 'Selected' in df.columns:
        df = df.drop(columns=['Selected'])
    if 'Default' in df.columns:
        df = df.drop(columns=['Default'])
    df = df.reset_index(drop=True)
    # create row key(s) from raw column names. Row keys are all the column names concatenated with '__' between them.
    # row keys are used later to match with previously stored mappings for identical tables
    row_cols = '' 
    rc = []  
    for r in row_nums:
        row_cols_list = []
        for c in df.columns:
            if c != 'Selected':
                s = str(df.iloc[r][c])
                s = s.replace('\n',' ')
                row_cols_list.append(s)
        # if dataset has already been transposed, annotate the row key so that later matching logic takes this into account
        if dig.selected_table_obj.transposed == True:
            prefix = 'T_'
        else:
            prefix = ''
        rc.append(prefix + '__'.join(row_cols_list))
    row_cols = '||'.join(rc)
    
    # check for text that spans multiple columns by comparing adjacent columns to previusly save column keys and text extracted from the report
    if (len(row_nums) > 1):
        idx = row_nums[0]
        for i in range(len(df.columns)-1):
            merged_text = fdCommon.none_to_empty_string(df.iloc[idx,i]) + ' ' + fdCommon.none_to_empty_string(df.iloc[idx,i+1])
            
            if merged_text in cfg.column_mapping_df['MappedColumn'].values:
                df.iloc[idx,i] = merged_text 
                df.iloc[idx,i+1] = merged_text 
            elif merged_text in dig.extracted_page_text_df['text'].values: #does the merge term appear in a list of known text
                df.iloc[idx,i] = merged_text 
                df.iloc[idx,i+1] = merged_text 
    new_cols = []
    col_suffix = 2
    for c in df.columns:
        # create new column names by concatenating multiple rows (if applicable) and cleaning out invalid character
        if c != 'Selected':
            col = fdCommon.none_to_empty_string(df.iloc[row_nums[0]][c])
            for i in range(1,len(row_nums)):
                c2 = fdCommon.none_to_empty_string(df.iloc[row_nums[i]][c])
                if c2 != '':
                    col = col + ' ' + c2
            if col == '':
                col = c #keep original column name if the new name is blank

            try:
                col = col.replace('\n',' ').lstrip()
            except:
                col = c.replace('\n',' ').lstrip()
            
            #check for duplicate columns:
            if col in new_cols:
                col = col + '_' + str(col_suffix)
                col_suffix = col_suffix + 1
            new_cols.append(col)

            df = df.rename(columns={c:col }) 



    # drop rows up to and including first row
    
    for r in reversed(row_nums):
        df = df.drop([r])
    
    df = df.iloc[row_nums[0]:]
    dig.selected_table_obj.header = row_cols
    log_edit(dig.auth,dig.selected_table_obj,'Set Header', row_nums)
    return df


# transpose a dataframe and update global flag to prevent additional transposition
def transpose_df(table, auth):
    if table.transposed == False:
        if 'Selected' in table.table_data_edited.columns:
            table.table_data_edited = table.table_data_edited.drop(columns=['Selected'])
        if 'Default' in table.table_data_edited.columns:
            table.table_data_edited = table.table_data_edited.drop(columns=['Default'])
        table.table_data_edited = table.table_data_edited.transpose()
        for c in table.table_data_edited.columns:
            table.table_data_edited.rename(columns = {c:str(c)}, inplace = True)
        table.table_data_edited=table.table_data_edited.reset_index(drop=True)
       
        table.transposed = True
        log_edit(auth, table,'Transposed')

# write digitized table data to storage and update table status
def update_file_and_status(dig, cfg, _da,selected_file,selected_page_index, selected_table_number, selected_table_type, status, source='cloud'):
    filename = selected_file + '_' + str(selected_page_index) + '_' + str(selected_table_number) + '_' + selected_table_type
    filename = filename.replace('\n',' ')
    filename = filename + '.csv'
    
    table = dig.selected_table_obj
    #build a merged data frame with both header and column data
    output_df = table.header_data_mapped.copy()
    for c in reversed(range(len(table.header_data_mapped.columns))):
        if table.header_data_mapped.columns[c] not in output_df.columns:
            output_df.insert(loc=0, column = table.header_data_mapped.columns[c], value = table.header_data_mapped .iloc[0][table.header_data_mapped.columns[c]])
    _da.write_csv(output_df,dig.csv_output_path,filename, source)
    print('write csv',dig.csv_output_path,filename)
    
    fdReport.save(dig.report_obj, dig)
    if dig.selected_table_obj.header != '':
        new_row = pd.DataFrame([[selected_table_type,dig.selected_table_obj.header, '',1]] , columns=['Table', 'ColumnKeys', 'Rejected','Count'] ).reset_index(drop=True) 
        cfg.table_mapping_df = pd.concat([cfg.table_mapping_df,new_row])

# clean up mapping file
    if (dig.selected_table_obj.header != '') and (dig.selected_table_obj.predicted_header != '') and \
        (dig.selected_table_obj.table_type != '') and (dig.selected_table_obj.predicted_table_type != '') and \
        (dig.selected_table_obj.table_type != dig.selected_table_obj.predicted_table_type): #prediction wasn't used
        rejected_maps = ((cfg.table_mapping_df['ColumnKeys'] == dig.selected_table_obj.predicted_header) & (cfg.table_mapping_df['Table'] == dig.selected_table_obj.predicted_table_type))
        cfg.table_mapping_df.loc[rejected_maps,'Rejected'] = 'Y'
        cfg.table_mapping_df.loc[rejected_maps,'Count'] = 1
            #st.write(table_mapping_df)
    
    cfg.table_mapping_df = cfg.table_mapping_df.groupby(['Table', 'ColumnKeys','Rejected']).agg({'Count': 'sum'}).reset_index()

    _da.write_csv(cfg.table_mapping_df,dig.config_path, "/pvt_table_mapping.csv", source)
    dig.selected_table_obj.table_status = status


    return dig.selected_table_obj.table_status



# update app when a new table type is assigned to a table and try to update the table header based on previous mapping
def table_type_changed(dig, cfg, selected_table_type):
    #update cached configuration data for selected table_type
    all_columns_config_df = cfg.table_columns_df[(cfg.table_columns_df['Table']==selected_table_type)].sort_values(by='SortOrder').loc[:].reset_index(drop=True)
    all_columns_config_df.reset_index(drop=True)
    dig.header_columns_config_df = all_columns_config_df[all_columns_config_df['FieldType']=='Header'].sort_values(by='SortOrder').loc[:].reset_index(drop=True)
    dig.header_columns_config_df = dig.header_columns_config_df.sort_values(by=['SortOrder','Field'])
    dig.header_columns_config_df.reset_index(drop=True)
    dig.table_columns_config_df = all_columns_config_df[all_columns_config_df['FieldType'] =='Column'].loc[:].reset_index(drop=True)
    dig.table_columns_config_df = dig.table_columns_config_df.sort_values(by=['SortOrder','Field'])
    dig.table_columns_config_df.reset_index(drop=True)
    dig.columns_dropdown_list = ['No Mapping'] + dig.table_columns_config_df['Field'].to_list()
    dig.has_uoms =  dict(zip(all_columns_config_df['Field'].values,all_columns_config_df['HasUOM'].values))
    dig.uom_dimensions =  dict(zip(all_columns_config_df['Field'].values,all_columns_config_df['UOMDimension'].values))
    
    if dig.selected_table_obj.table_type != selected_table_type: 
        dig.selected_table_obj.table_column_mappings = []
        dig.selected_table_obj.header_column_mappings = []
    dig.selected_table_obj.table_type = selected_table_type
    dig.selected_table_type = selected_table_type
    print(f'selected table_type {dig.selected_table_obj.table_type}')


# update app when a new PVT  file is selected for processing
def file_selected(dig, cfg, selected_file, source='cloud'):
    print(f'selected file: {selected_file}')
    dig.pdf = dig.da.get_pdf_images(dig.processed_pdf_path,selected_file,source)  #get and cache pdf document
    
    fdReport.load_report_object(selected_file, dig, cfg)   
    
    dig.selected_page_index = -1 # force page initialization
    dig.selected_file = selected_file #cache current file
    # reset mapping workflow
    dig.tab = 'Edit'
    
    


# update app when a new page is selected for processing
def page_selected(dig, selected_page_index):
    
    selected_page = dig.pdf[selected_page_index] #cache pdf page
    pix = selected_page.get_pixmap() 
    image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    #filter extracted data for page (extracted text is free form direct from OCR)
    dig.extracted_page_text_df = pd.DataFrame(columns=(['text']))
    if (dig.report_obj.extracted_text is not None):
        try:
            if dig.report_obj.extracted_text == []:
                dig.report_obj.extracted_text = None
        except:
            do='nothing'
            
        if (len(dig.report_obj.extracted_text.index) > 0):
            dig.extracted_page_text_df = dig.report_obj.extracted_text[dig.report_obj.extracted_text['page']==selected_page_index]

    print(f'selected page {selected_page_index}')
    dig.tab = 'Edit'
    return image

# update app when a new table is selected in a page
def table_selected(dig, cfg, table):
    print('table_selected', table.id, table.page, table.report_table_number, table.predicted_table_type, table.table_type)

    if table.table_data_edited.empty:
        table.table_data_edited = table.table_data_raw

    if table.header == '': #header has not been set
        table.predicted_table_type, row_nums, table.predicted_transposed, table.predicted_header = predict_table_type(table.table_data_edited, table.table_type,dig, cfg)
        if (table.table_type in ['Select Table Type','Unknown']) and (table.predicted_table_type != ''):
            table.table_type = table.predicted_table_type
        #transpose if necessary and update header for matched header     
        if row_nums != []:
            if table.predicted_transposed == True:
                transpose_df(table, dig.auth)
            else:
                table.transposed = False
            dig.selected_table_obj.table_data_edited = set_table_column_headings(dig, cfg, dig.selected_table_obj.table_data_edited, row_nums)

    else:  #header is set but table type is not. Use raw table to find type based on header
        table.predicted_table_type, row_nums, transpose, table.predicted_header = predict_table_type(table.table_data_raw, table.table_type,dig, cfg)
        if (table.table_type in ['Select Table Type','Unknown']) and (table.predicted_table_type != ''):
            table.table_type = table.predicted_table_type
        if (table.table_type == table.predicted_table_type) and (table.predicted_header != ''):
            table.header = table.predicted_header
    #else: #header and type are set, keep them


    # if table.predicted_header == '':
    #     #dig.selected_table_obj.predicted_header = dig.selected_table_obj.header
    #     dig.selected_table_obj.predicted_table_type = selected_table_type
    return 

# save current table column mapping to mapping df
def save_column_mapping(table, da,column_mapping_df, config_path):

    # backup existing file
    now = datetime.now()
    da.write_csv(column_mapping_df,config_path + "/column_mapping_history/",f"pvt_column_mapping_{str(now)}.csv", source='cloud')
    # cfg.table_mapping_df = pd.concat([cfg.table_mapping_df,new_row])
    # cfg.table_mapping_df = cfg.table_mapping_df.drop_duplicates().reset_index(drop=True)

    # dig.da.write_csv(cfg.table_mapping_df,config_path + "/","pvt_table_mapping.csv", source)
    column_mappings = pd.DataFrame()
    for col in table.table_column_mappings:
        if col.mapped_column != 'No Mapping':
            new_row = {'Table': [table.table_type], 
                       'Field': [col.mapped_column], 
                       'FieldType': ['Column'],
                       'MappedColumn': [col.original_column],
                       'UOM': [col.mapped_uom],
                       'Rejected': [''],
                       'Count': [1]
                       }
            column_mappings = pd.concat([column_mappings, pd.DataFrame(new_row)])
            rejected = False
            if (col.predicted_column != '') and (col.predicted_column != col.mapped_column):
                rejected_maps = ((column_mapping_df['MappedColumn'] == col.original_column) & (column_mapping_df['Field'] == col.predicted_column) )
                column_mapping_df.loc[rejected_maps,'Rejected'] = 'Y'
            if ((col.has_uom) and (col.predicted_uom != '') and (col.predicted_uom != col.original_uom)):
                rejected_maps = ((column_mapping_df['MappedColumn'] == col.original_column) & (column_mapping_df['Field'] == col.predicted_column) & \
                                (column_mapping_df['UOM'] == col.predicted_uom))
                column_mapping_df.loc[rejected_maps,'Rejected'] = 'Y'
            #st.write(table_mapping_df)
    
    
    column_mapping_df = pd.concat([column_mapping_df,column_mappings])
    # clean up mapping file
    column_mapping_df['Count'] = column_mapping_df['Count'].replace('',1)
    unique_rows = column_mapping_df.groupby(['Table', 'Field','FieldType','MappedColumn','UOM','Rejected']).agg({'Count': 'sum'}).reset_index()
    column_mapping_df = unique_rows
    da.write_csv(column_mapping_df,config_path + "/","pvt_column_mapping.csv", source='cloud')

def update_table_data_mapped(table, table_column_mappings, table_columns_config_df, dig, cfg):
    table.table_data_mapped = table.table_data_edited.copy()
    table.table_data_mapped.replace({np.nan:None},inplace=True)
    table.table_data_mapped.replace({'':None},inplace=True)
    table.table_data_mapped.replace({'nan':None},inplace=True)
    table.table_data_mapped = table.table_data_mapped.drop(columns = fdCommon.find_empty_columns(table.table_data_mapped), axis=1)

    original_cols_mapped = [obj.original_column for obj in table_column_mappings if obj.mapped_column != '']
    mapped_cols = [obj.mapped_column for obj in table_column_mappings if obj.mapped_column != '']
    for col in table.table_data_mapped:
        if table.table_type == 'Sample':
            if (col not in original_cols_mapped) and (col != 'Default'): # Defailt is added to Sample table to indicate that a sample is the default
                table.table_data_mapped = table.table_data_mapped.drop(columns = [col])
            
        elif (col not in original_cols_mapped):
            table.table_data_mapped = table.table_data_mapped.drop(columns = [col])
    table.table_data_mapped.columns = mapped_cols

    #add mandatory columns
    for c, col in table_columns_config_df[table_columns_config_df['RequiredColumn']=='Y'].iterrows():
        required_field = col['Field']
        if required_field not in table.table_data_mapped.columns:
            table.table_data_mapped[required_field] = ''


    # Add units of measure as new columns
    # Iterate over rows in reverse using iterrows()
    mapped_column_dict = {obj.mapped_column: obj for obj in table_column_mappings}
    for c, col in table_columns_config_df[table_columns_config_df['HasUOM']=='Y'].iloc[::-1].iterrows():
        col_name = col['Field']
        if col_name in table.table_data_mapped.columns:
            uom_col = col_name +'_UOM'
            if uom_col not in table.table_data_mapped.columns:
                i = table.table_data_mapped.columns.get_loc(col_name)
                if col_name in mapped_column_dict:
                    uom = mapped_column_dict[col_name].mapped_uom
                else:
                    uom = ''
                table.table_data_mapped.insert(loc=i+1, column = uom_col, value = uom)

    # Add header fields and their uoms to output dataframe
    # ToDo: This will change to hierarchical JSON in future

    if table.table_type == 'Sample':
        table.table_data_mapped = update_mapped_sample_table(dig, table.table_data_mapped)
    

    table.header_data_mapped  = table.header_data_mapped.drop(columns = fdCommon.find_empty_columns(table.header_data_mapped ), axis=1)
    output_df = table.table_data_mapped.copy()
    for c in reversed(range(len(table.header_data_mapped.columns))):
        output_df.insert(loc=0, column = table.header_data_mapped.columns[c], value = table.header_data_mapped .iloc[0][table.header_data_mapped.columns[c]])
    #st.session_state['output_df'] = output_df                
    
    #validation_status, validation_message, output_df = validate_table_type(output_df, table.table_type)
        
    column_validation, table.table_data_mapped = fdValidation.validate_table(table.table_data_mapped,table.table_type, 'Column', cfg)
    header_validation, table.header_data_mapped = fdValidation.validate_table(table.header_data_mapped,table.table_type, 'Header', cfg)
    validation = column_validation + header_validation
    return validation, output_df

