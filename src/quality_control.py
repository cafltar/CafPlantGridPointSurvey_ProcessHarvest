import pandas as pd

from importlib.machinery import SourceFileLoader
cafcore_qc_0_1_4 = SourceFileLoader('qc', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/qc.py').load_module()


def qc_points(df, path_qc_bounds):
    # Make sure we're not gaining any rows
    rows_original = df.shape[0]

    # Load file with bounds checks
    qc_point_params = pd.read_csv(path_qc_bounds).dropna(subset=['Lower', 'Upper'])

    # Get unique crops in df, filer df and params for each unique crop, merge them to new df
    crops_in_data = df['Crop'].unique()

    result = pd.DataFrame()
    
    for crop in crops_in_data:
        # Some Crops have values of None, so handle that with generic bounds
        if crop == None:
            df_crop = df[df['Crop'].isna()]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == 'Generic']
        else:
            df_crop = df[df['Crop'] == crop]
            qc_point_params_crop = qc_point_params[qc_point_params['Crop'] == crop]

        #qcPointParamsCrop = qcPointParams[qcPointParams["Crop"] == crop]
        #dfCrop = df_result[df_result["Crop"] == crop]

        for paramIndex, param_row in qc_point_params_crop.iterrows():
            df_crop = cafcore_qc_0_1_4.process_qc_bounds_check(df_crop, param_row['FieldName'], param_row['Lower'], param_row['Upper'])

        result = pd.concat([result, df_crop], axis=0, ignore_index=True)

    rows_result = result.shape[0]

    if rows_original != rows_result:
        raise Exception("Resultant dataframe is different size than original")
    
    return result
    
def qc_observation(df):

    result = df.copy()

    result = cafcore_qc_0_1_4.process_qc_greater_than_check(result, 'GrainYieldAirDry_P2', 'GrainYieldOvenDry_P2')
    result = cafcore_qc_0_1_4.process_qc_greater_than_check(result, 'BiomassAirDryPerArea_P2', 'GrainYieldAirDry_P2')
    result = cafcore_qc_0_1_4.process_qc_greater_than_check(result, 'BiomassAirDryPerArea_P2', 'ResidueMassAirDryPerArea_P2')
    result = cafcore_qc_0_1_4.process_qc_less_than_check(result, 'GrainYieldAirDry_P2', 'BiomassAirDryPerArea_P2')
    result = cafcore_qc_0_1_4.process_qc_less_than_check(result, 'GrainYieldOvenDry_P2', 'GrainYieldAirDry_P2')
    result = cafcore_qc_0_1_4.process_qc_less_than_check(result, 'ResidueMassAirDryPerArea_P2', 'BiomassAirDryPerArea_P2')

    return result
