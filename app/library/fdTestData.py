from library import fdDataAccess
from library import fdCommon
import json


def save_sample(report_obj,sample_df, auth):      

    column_table_name = 'Data'
    # Convert dataframes to dictionaries
    column_dict = sample_df.to_dict(orient='records')

    for s in column_dict:#samples:
        s['Source'] = report_obj.report_name
        s['Location'] = {
            'Asset':report_obj.asset,
            'Field':report_obj.field,
            'Reservoir':report_obj.reservoir,
            'Well':report_obj.well,
        }
        
        for r,report_sample in report_obj.samples.iterrows():
            #print(s['FluidSampleID'],es['fluidSampleID'],s['FluidSampleContainerID'], ['es.fluidSampleContainerID)'])
            if ('FluidSampleContainerID' in report_sample) and ('FluidSampleContainerID' in s.keys()) and (s['FluidSampleContainerID'] not in ['','unknown','Unknown']): #container ID is most typical distimction of there are multiple samples
                if (s['FluidSampleContainerID'] == report_sample['FluidSampleContainerID']) and ('ID' in report_sample):
                    s['ID'] = report_sample['ID']
                    s['FluidSampleID'] = report_sample['FluidSampleID']
            elif ('FluidSampleID' in report_sample) and ('FluidSampleID' in s.keys()) and (s['FluidSampleID']): #otherwise us sample if
                if (s['FluidSampleID'] == report_sample['FluidSampleID']) and ('ID' in report_sample):
                    s['ID'] = report_sample['ID']

        if ('ID' in s.keys()) and (s['ID'] is not None) and (s['ID'] != ''):
            ID = s['ID']
        elif 'ID' in s.keys():
            del s['ID']
            ID = ''
        else:
            ID = ''

        action = 'Create' if ID == '' else 'Update'
    
        data = {}
        sample = {}
        sample['sample'] = s
        data['data'] = sample
        
        data_json_str = json.dumps(data, default=fdCommon.np_encoder)
        #data = json.loads(data_text)
        response_code, response_text = fdDataAccess.save_json(data_json_str, 'Sample', auth.token, auth.tenant_id, ID)
        return response_code, response_text