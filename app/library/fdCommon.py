import pandas as pd
import numpy as np
import re 

def find_empty_columns(df):
    empty_columns = []
    for column in df.columns:
        values = df[column].values
        if np.all(pd.isna(values) | (values == '')):
            empty_columns.append(column)
    return empty_columns

# Function to find parantheses in text
def contains_parentheses(text):
    pattern = r'\(.*\)'
    contains = re.search(pattern, text)
    if contains is not None:
        return True
    else:
        return False
    
# try to get a variable from a dictionary. Use default value in case of error
def try_get(dict, key, default=None):
    try:
        return dict[key]
    except:
        return default

def convert_subscript_to_normal(text):
    # Regular expression to identify subscript numbers
    subscript_pattern = re.compile(r'(\w)([₀₁₂₃₄₅₆₇₈₉])')

    # Dictionary to map subscript characters to normal numbers
    subscript_map = {
        '₀': '0', '₁': '1', '₂': '2', '₃': '3', '₄': '4',
        '₅': '5', '₆': '6', '₇': '7', '₈': '8', '₉': '9'
    }

    # Function to replace subscript numbers with normal ones
    def replace(match):
        return match.group(1) + subscript_map[match.group(2)]

    # Replace subscripted numbers using regex
    converted_text = subscript_pattern.sub(replace, text)
    return converted_text
 
 # Function to convert scientific notation strings to floats
def string_convert_scientific_notation_to_float(x):
    if isinstance(x, str):
        if 'E' in x:
            x = x.replace(' ','')
    return x
def format_number(x):
    if x == np.nan:
        return x
    if abs(x) < 0.001 or abs(x) >= 10000:
        return '{:.2e}'.format(x)  # Scientific notation
    else:
        decimal_places = min(max(0, 5 - int(np.floor(np.log10(abs(x))))),0)  # Determine number of decimal places
        formatted_number = '{:.{}f}'.format(x, decimal_places)  # Float with dynamically determined decimal places
        if abs(x) < 1000:  # Remove comma for values greater than 999
            return formatted_number
        else:
            return formatted_number.replace(',', '')
        
# Function to extract text inside parentheses
def extract_text_inside_parentheses(text):
    pattern = r'\((.*?)\)'
    matches = re.findall(pattern, text)
    return matches[0] if matches else None

# replaces unsupported data types like int64 with their python equivalent
def np_encoder(object):
    if isinstance(object, np.generic):
        return object.item()

# Function to check if any term from a list appears in any text in the DataFrame
def check_terms_in_dataframe(df, terms):
    # Iterate through each term in the list
    for term in sorted(terms, key=len, reverse=True):
        # Iterate through each cell in the DataFrame
        for col in df.columns:
            for value in df[col]:
                # Check if the term appears in the cell value
                if term in str(value).lower():
                    return term  # Return the matched term if found
    return None  # Return None if none of the terms are found in the DataFrame

# Define a custom aggregation function to choose the first non-None value
def choose_first_non_none(series):
    for value in series:
        if pd.notna(value):
            return value
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

# replace None or Nan with an empty string
def none_to_empty_string(txt):
    # Converting None to empty string
    if txt is None:
        txt = ''
    if is_text(txt) == False:
        if is_number(txt) == False:
            txt = ''
        else:
            txt = str(txt)
            if txt == 'nan':
                txt = ''
    return txt

# Function to remove trailing "__None"
def remove_trailing_none(text):
    return re.sub(r'__None*$', '', text)

# Replace "nan" with None in the Python object
def replace_nan(obj):
    if isinstance(obj, str) and obj.lower() == 'nan':
        return None
    return obj

# Replace "nan" with None in the Python object
def replace_nan(obj):
    if isinstance(obj, str) and obj.lower() == 'nan':
        return None
    return obj

def get_index_in_list(item, list):
    idx = 0
    if (item != '') and (item in list):
        idx = list.index(item)
    else:
        idx = 0
    return idx

# Data tables may have two values, including UOM, embedded in a single cell. These functions split them out
def extract_value_tuples(text):
    pattern = re.compile(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)')
    matches = pattern.findall(str(text))
    if len(matches) == 2:
        value_tuples = [(float(match[0]), match[1]) for match in matches]
    else:
        value_tuples = []
    pattern_found = bool(value_tuples)
    return pattern_found,value_tuples

def process_double_value_column(df, column_name):
    # Apply the extract_value_tuples function to the entire column
    df['temp_tuples'] = df[column_name].apply(extract_value_tuples)
    # Create new columns with the extracted values
    val1 = []
    uom1 = ''
    val2 = []
    uom2 = ''
    for r in range(len(df.index)):
        tup = df.iloc[r]['temp_tuples']
        if tup[0] == True: #tuples discovered in column

            val1.append(tup[1][0][0])
            uom1 = tup[1][0][1]
            val2.append(tup[1][0][0])
            uom2 = tup[1][0][1]

            col1 = f'{column_name} ({uom1})'
            col2 = f'{column_name} ({uom2})'
            if col1 not in df.columns:
                df[col1] = df[column_name]
                df[col2] = None
            df.at[r,col1] = val1
            df.at[r,col2] = val2

    # if len(val1) > 0:
    
    #     df[f"{column_name} ({df['temp_tuples'].apply(lambda x: x[1][0][1] if x[0] else None).iloc[0]})"] = df['temp_tuples'].apply(lambda x: x[1][0][0] if x[0] else None)
    #     df[f"{column_name} ({df['temp_tuples'].apply(lambda x: x[1][1][1] if len(x) > 1 else None).iloc[0]})"] = df['temp_tuples'].apply(lambda x: x[1][1][0] if len(x) > 1 else None)
        # Drop the original column
    df.drop(column_name, axis=1, inplace=True)
    # Drop the temporary 'temp_tuples' column
    df.drop('temp_tuples', axis=1, inplace=True)
    return df