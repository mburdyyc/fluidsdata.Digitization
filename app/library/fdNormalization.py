from library import fdCommon
from library import fdNormalization
from library import fdValidation
from library import fdConfiguration
import pandas as pd

def get_standard_uom(uom, uom_mapping_df, column=None):
    if (uom is None) or (uom == '') or (uom == 'nan'):
        return ''
    uom = uom.strip()
    uom_mapping_df['ReportUOM'] = uom_mapping_df['ReportUOM'].str.strip()
    # Check if the specific value is already standard
    if uom in uom_mapping_df['UOM'].values:
        return uom
    # Check if the specific value appears in the ReportUOM column
    if uom in uom_mapping_df['ReportUOM'].values:
        # If the value is present, validate the corresponding Uom value
        idx = uom_mapping_df[uom_mapping_df['ReportUOM'] == uom].index[0]
        standard_uom = uom_mapping_df.loc[idx, 'UOM']
        if (standard_uom is None) or (str(standard_uom).strip() == ''):
            return ''
        else:
            return standard_uom
    else:
        return ''
    
def check_valid_uom(uom, cfg, column):
    if (uom is None) or (uom == '') or (uom == 'nan'):
        return False
    uom_dimension = cfg.column_config_df[cfg.column_config_df['Field']==column]['UOMDimension'][0]
    
    uom = uom.strip()
    cfg.uom_mapping_df['ReportUOM'] = cfg.uom_mapping_df['ReportUOM'].str.strip()
    # Check if the specific value is already standard
    if uom in cfg.uom_mapping_df['UOM'].values:
        standard_uom = uom
    # Check if the specific value appears in the ReportUOM column
    if uom in cfg.uom_mapping_df['ReportUOM'].values:
        # If the value is present, validate the corresponding Uom value
        idx = cfg.uom_mapping_df[cfg.uom_mapping_df['ReportUOM'] == uom].index[0]
        standard_uom = cfg.uom_mapping_df.loc[idx, 'UOM']
        if (standard_uom is None) or (str(standard_uom).strip() == ''):
            return False

    
def get_standard_comp(component, cfg):
    if (component is None) or (component == '') or (component == 'nan'):
        validation_status = False
        validation_message = f"Component is not specified"
        return None
    component = component.strip()
    
    component = fdCommon.convert_subscript_to_normal(component)
    
    cfg.component_mapping_df['ReportComponent'] = cfg.component_mapping_df['ReportComponent'].str.strip()
    # Check if the specific value appears in the ReportComponent column
    if component in cfg.component_mapping_df['ReportComponent'].values:
        # If the value is present, validate the corresponding component value
        idx = cfg.component_mapping_df[cfg.component_mapping_df['ReportComponent'] == component].index[0]
        component_value = cfg.component_mapping_df.loc[idx, 'Component']
        if (component_value is None) or (str(component_value).strip() == ''):
            return component
        else:
            return component_value.strip()
    else:
        return component
    
def normalize_column_df(df, table_type, cfg):
    for c in df.columns:
        # if (c.endswith('_UOM')):
        #     for r, row in df.iterrows():
        #         df.iloc[r][c] = normalize_uom(row[c])
        if (c.endswith('_UOM') == False) & (c not in ['','Default','ID']):
            val, text = fdValidation.validate_data_type(df,c, table_type, cfg)
            print(c,val,text)
            col_cfg = fdConfiguration.get_column_config(c,table_type, cfg)
            
            if col_cfg is None:
                expected_data_type = 'str'
            else:
                expected_data_type = col_cfg.iloc[0]['DataType']
            
            print('data Type', expected_data_type)
            if c in ['Default', 'Selected']:
                df[c] = df[c].astype('bool')
            elif expected_data_type =='date':
                df[c] = pd.to_datetime(df[c], infer_datetime_format=True)
                df[c] = df[c].astype('str')
            elif expected_data_type =='datetime':
                df[c] = pd.to_datetime(df[c], infer_datetime_format=True)
                df[c] = df[c].astype('str')
            elif expected_data_type =='time':
                try:
                    df[c] = pd.to_datetime(df[c], infer_datetime_format=True)
                    df[c] = df[c].dt.time
                    df[c] = df[c].astype('str')
                except:
                    df[c] = df[c].astype('str')
            elif expected_data_type =='float':
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            elif expected_data_type.startswith('enum:'):
                df[c] = df[c].astype('str')
            else:
                df[c] = df[c].astype(expected_data_type)
            print(c, type(df[c]))
            #df[c] = df[c].astype('str')
            if c == 'FluidComponentReference':
                for r, row in df.iterrows(): 
                    standard_comp = fdNormalization.get_standard_comp(row[c], cfg)
                    
                    df.at[r,c] = standard_comp
            if df[c].dtype == 'int64':
                df[c] = df[c].astype(int)        
            
        elif c.endswith('_UOM') == True:
            standard_uom = get_standard_uom(df.iloc[0][c], cfg.uom_mapping_df)
            df[c] = standard_uom
    return df

def table_data_to_json(table, tables_df):
    header_table_name = tables_df[(tables_df['Table']==table.table_type)]['HeaderTable'].values[0]
    column_table_name = tables_df[(tables_df['Table']==table.table_type)]['ChildTable'].values[0]

    # Convert dataframes to dictionaries
    column_dict = table.table_data_normalized.to_dict(orient='records')
    
    hdr_json = {}
    for c in table.header_data_normalized.columns:
        hdr_json[c] = table.header_data_normalized .iloc[0][c]

    hdr_json[column_table_name] = column_dict

    if header_table_name != column_table_name:
        table_json = {
            header_table_name: hdr_json
        }
    else: # there is no header table
        table_json = {
            column_table_name: column_dict
        }
    
    table.table_data_normalized = table.table_data_normalized.where(pd.notnull(table.table_data_normalized), None)
    table.header_data_normalized  = table.header_data_normalized.where(pd.notnull(table.header_data_normalized ), None)

    column_dict = table.table_data_normalized.to_dict(orient='records')
    
    hdr_json = {}
    for c in table.header_data_normalized.columns:
        hdr_json[c] = table.header_data_normalized.iloc[0][c]

    hdr_json[column_table_name] = column_dict

    if header_table_name != column_table_name:
        table_json = {
            header_table_name: hdr_json
        }
    else: # there is no header table
        table_json = {
            column_table_name: column_dict
        }
    return table_json