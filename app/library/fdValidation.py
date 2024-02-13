import pandas as pd
from library import fdConfiguration

# validate units of measure
def validate_uom(uom, cfg):
    if (uom is None) or (uom == '') or (uom == 'nan'):
        validation_status = False
        validation_message = f"UOM is not specified"
        return validation_status, validation_message
    uom = uom.strip()
    cfg.uom_mapping_df['ReportUOM'] = cfg.uom_mapping_df['ReportUOM'].str.strip()
    # Check if the specific value appears in the ReportUOM column
    if uom in cfg.uom_df['symbol'].values:
        validation_status = True
    elif uom in cfg.uom_mapping_df['ReportUOM'].values:
        # If the value is present, validate the corresponding Uom value
        idx = cfg.uom_mapping_df[cfg.uom_mapping_df['ReportUOM'] == uom].index[0]
        uom_value = cfg.uom_mapping_df.loc[idx, 'UOM']
        if (uom_value is None) or (str(uom_value).strip() == ''):
            validation_status = False
        else:
            validation_status = True
    else:
        # If the value does not appear in ReportUOM column, add a new row
        new_row = pd.DataFrame({'ReportUOM': [uom], 'UOM': ['']})
        cfg.uom_mapping_df = pd.concat([cfg.uom_mapping_df, new_row], ignore_index=True)
        print('added',new_row)
        validation_status = False  
    if validation_status == False:
        validation_message = f"{uom} doesn't map to a standard UOM. Update UOM mapping configuration"
    else:
        validation_message = 'OK'
    return validation_status, validation_message


# validate component name
def validate_component(component, component_mapping_df):
    if (component is None) or (component == '') or (component == 'nan'):
        validation_status = False
        validation_message = f"Component is not specified"
        return validation_status, validation_message
    component = component.strip()
    component_mapping_df['ReportComponent'] = component_mapping_df['ReportComponent'].str.strip()
    # Check if the specific value appears in the ReportComponent column
    if component in component_mapping_df['ReportComponent'].values:
        # If the value is present, validate the corresponding component value
        idx = component_mapping_df[component_mapping_df['ReportComponent'] == component].index[0]
        component_value = component_mapping_df.loc[idx, 'Component']
        if (component_value is None) or (str(component_value).strip() == ''):
            validation_status = False
        else:
            validation_status = True
    else:
        # If the value does not appear in ReportComponent column, add a new row
        new_row = pd.DataFrame({'ReportComponent': [component], 'Component': ['']})
        component_mapping_df = pd.concat([component_mapping_df, new_row], ignore_index=True)
        print('added',new_row)
        validation_status = False  
    if validation_status == False:
        validation_message = f"{component} doesn't map to a standard component. Update component mapping configuration"
    else:
        validation_message = 'OK'
    return validation_status, validation_message, component_mapping_df


        

def convert_uom(uom, value, cfg):
    
    if (uom is None) or (uom == '') or (uom == 'nan'):
        return uom, value
    value = float(value)
    uom = uom.strip()
    #cfg.uom_df['ReportUOM'] = cfg.uom_mapping_df['ReportUOM'].str.strip()
    # Check if the specific value appears in the ReportUOM column
    if uom in cfg.uom_df['symbol'].values:
        # If the value is present, validate the corresponding Uom value
        idx = cfg.uom_df[cfg.uom_df['symbol'] == uom].index[0]
        
        if cfg.uom_df.loc[idx, 'isBase']:
            return uom, value
        A = float(cfg.uom_df.loc[idx, 'A'])
        B = float(cfg.uom_df.loc[idx, 'B'])
        C = float(cfg.uom_df.loc[idx, 'C'])
        D = float(cfg.uom_df.loc[idx, 'D'])
        baseUnit = cfg.uom_df.loc[idx, 'baseUnit']
        converted_unit =(A+B*value)/(C+D*value)
        return baseUnit, converted_unit
    else:
        return uom, value       

# Function to check if the values in a column match the specified data type
def validate_data_type(df,column, tablename, cfg):
    print(column)
    if column == 'SampleID':
        return True,''
    filtered_row = fdConfiguration.get_column_config(column, tablename, cfg)
    if filtered_row is None:
        return True, ''
    # Get the expected data type from the filtered row
    expected_data_type = filtered_row.iloc[0]['DataType']

    if expected_data_type is None:
        return False, 'Data Type not found'

    # Check if each value in the df column matches the expected data type
    for value in df[column]:
        #check for mandatory values
        if (value is None) or (str(value)=='') or (str(value)=='nan'):
            if filtered_row.iloc[0]['RequiredValue'] == 'Y':
                return False, f'value {value} cannnot be empty'
        elif expected_data_type == 'integer' and not isinstance(value, int):
            return False, f'value {value} is invalid for integer'
        elif expected_data_type == 'float':
            try:
                float_val = float(value)
            except:
                return False, f'value {value} is invalid for float'
        elif expected_data_type == 'string' and not isinstance(value, str):
            return False, f'value {value} is invalid for float'

    return True, ''

# valdate that table meets table type requirements
def validate_table_type(df, table_type, field_type, cfg):
    validation_errors = []
    # Filter the table_columns_df based on table type and mandatory columns
    mandatory_columns = cfg.table_columns_df[(cfg.table_columns_df['Table'] == table_type) & (cfg.table_columns_df['RequiredColumn'] == 'Y') & (cfg.table_columns_df['FieldType'] == field_type)]
    validation_status = True
    for c in mandatory_columns['Field']:
        if c not in df.columns:
            validation_status = False
            validation_errors.append(f'Missing mandatory column {c} for {table_type}')
            df[c] = '' #add missing mandatory column
    return validation_status, validation_errors, df


# perform validation on a table
def validate_table(df, table_type, field_type, cfg):

    validation_errors = []
    if df is None:
        validation_errors.append(f'No_Data: Table is empty')
        return validation_errors
    #check columns
    validation_status, validation_message, df = validate_table_type(df, table_type, field_type, cfg)
    if validation_status == False:
        validation_errors.append(f'{validation_message}')
    comp_sum = 0
    for c in df.columns:
        split_comp = c.split('(')
        split_UOM = c.split(' [')
        if c.endswith('_UOM'):
            #check for mappable UOM
            validation_status, validation_message = validate_uom(df.iloc[0][c], cfg)
            if validation_status == False:
                validation_errors.append(f'{c}: {validation_message}')
        elif len(split_comp) > 1:
            #check for component name
            comp_name = split_comp[1].replace('(','').replace(')','')
            validation_status, validation_message, cfg.component_mapping_df = validate_component(comp_name, cfg.component_mapping_df)
            if validation_status == False:
                validation_errors.append(f'{c}: {validation_message}')
            comp_sum = comp_sum + float(df.iloc[0][c])
        elif c == 'FluidComponentReference':
            #check for component name
            validation_status, validation_message, cfg.component_mapping_df = validate_component(df.iloc[0][c], cfg.component_mapping_df)
            if validation_status == False:
                validation_errors.append(f'{c}: {validation_message}')
        elif c not in ['Default', 'No Mapping']:
            validation_status, validation_message = validate_data_type(df,c, table_type, cfg)
            if validation_status == False:
                validation_errors.append(f'{c}: {validation_message}')
    if comp_sum > 0 and round(comp_sum,2) != 100.0:
            validation_errors.append(f'Composition adds up to {comp_sum}')
    return validation_errors, df

# add validation error to app validation error list
def raise_validation_error(table, validation_message):
    if validation_message not in table.validation_errors:
        table.validation_errors.append(validation_message)
