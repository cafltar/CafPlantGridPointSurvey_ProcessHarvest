import pathlib
import pandas as pd
import polars as pl
import sys

import read_transform_qa
import calculate_qa
import quality_control

from importlib.machinery import SourceFileLoader
cafcore_qc_0_1_4 = SourceFileLoader('qc', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/qc.py').load_module()
cafcore_file_io_0_1_4 = SourceFileLoader('qc', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/file_io.py').load_module()

def generate_p1a1(harvest_year, args):
    print('Reading, transforming, and quality assurancing: ' + str(harvest_year))

    hy_df = pd.DataFrame([])

    # Read in all data for HY, transform to standard structure, process QA change file
    if(harvest_year == 2017):
        hy_df = read_transform_qa.hy2017(args)

    elif(harvest_year == 2018):
        hy_df = read_transform_qa.hy2018(args)
    elif(harvest_year == 2019):
        hy_df = read_transform_qa.hy2019(args)
    elif(harvest_year == 2020):
        hy_df = read_transform_qa.hy2020(args)
    elif(harvest_year == 2021):
        hy_df = read_transform_qa.hy2021(args)
    elif(harvest_year == 2022):
        hy_df = read_transform_qa.hy2022(args)
    elif(harvest_year == 2023):
        hy_df = read_transform_qa.hy2023(args)
    elif(harvest_year == 2024):
        hy_df = read_transform_qa.hy2024(args)
    else:
        print('WARNING: no function to process harvest year: ' + str(harvest_year))

    # Assign processing col names
    hy_df = cafcore_qc_0_1_4.add_processing_suffix(hy_df, args['metric_vars'], 1, True)

    # Drop non-standard cols
    extra_cols = [col for col in hy_df.columns if col not in list(args['pandas_schema_p1'].keys())]
    hy_df = hy_df.drop(columns=extra_cols)

    # Add standard cols if missing
    missing_cols = [col for col in list(args['pandas_schema_p1'].keys()) if col not in hy_df.columns]
    for missing_col in missing_cols:
        hy_df[missing_col] = float('nan')

    # Specify data types
    hy_df = hy_df.astype(args['pandas_schema_p1'])

    # Confirm data are complete records (all observations)
    if len(hy_df['HarvestYear'].unique()) > 1:
        raise Exception('Dataset more years than expected')
    if hy_df.shape[0] != len(args['observation_ids']):
        raise Exception('Dataset has different number of observations than expected')
    if set(hy_df['ID2'].unique()) != set(args['observation_ids']):
        raise Exception('Dataset has different observations than expected')
    if len(hy_df.columns) != len(args['pandas_schema_p1']):
        print([col for col in hy_df.columns if col not in list(args['pandas_schema_p1'].keys())])
        raise Exception('Dataset does not contain all expected columns')
    
    # Return results
    return hy_df

def generate_p2a1(df, args):
    print('Calculating some values...')
    p2a1 = calculate_qa.calculate_p2(df, args)

    return p2a1.sort_values(by=['HarvestYear', 'ID2'])

def generate_p3a1(df, args):
    print('Modeling some values...')

def generate_p2a3(df, args):
    print('Doing quality control...')

    # Do point checks
    p2a2 = quality_control.qc_points(df, args['path_qc_bounds_p2'])    
    
    # Do observation checks
    p2a3 = quality_control.qc_observation(p2a2)
    
    return p2a3.sort_values(by=['HarvestYear', 'ID2'])

def output(df, processing_level, accuracy_level, args):
    print('Writing output...')
    df_trim = cafcore_file_io_0_1_4.prune_columns_outside_p_level(df, processing_level, args['p_suffixes'], args['qc_suffixes'])
    
    df_trim_qc = cafcore_file_io_0_1_4.append_qc_summary_cols(df_trim, args['dimension_vars'], args['index_cols'], {'HarvestYear': pl.Int64, 'ID2': pl.Int64})
    
    df_trim_qc_p = cafcore_file_io_0_1_4.condense_processing_columns(df_trim_qc, processing_level, args['p_suffixes'])

    # Order the columns to make them look nice
    non_qc_col_order = ['HarvestYear','FieldId','Crop','ID2','SampleId','Latitude','Longitude','QCCoverage','QCFlags','Comments','CropExists_P1','HarvestDate_P1','SeedMoisture_P1','SeedTestWeight_P1','SeedProtein_P1','SeedStarch_P1','SeedGluten_P1','SeedOil_P1','ResidueNitrogen_P1','ResidueCarbon_P1','SeedNitrogen_P1','SeedCarbon_P1','SeedYieldAirDry_P2','SeedYieldOvenDry_P2','BiomassAirDryPerArea_P2','ResidueMassAirDryPerArea_P2','CropExists','HarvestDate','BiomassAirDryPerArea','ResidueMassAirDryPerArea','SeedYieldAirDry','SeedYieldOvenDry','SeedMoisture','SeedTestWeight','SeedProtein','SeedStarch','SeedGluten','SeedOil','ResidueNitrogen','ResidueCarbon','SeedNitrogen','SeedCarbon']

    ordered_cols = []
    for col in non_qc_col_order:
        ordered_cols.append(col)
        if(('_P1' in col) | ('_P2' in col) | ('_P3' in col)):
            ordered_cols.append(col + '_qcApplied')
            ordered_cols.append(col + '_qcResult')
            ordered_cols.append(col + '_qcPhrase')

    df_trim_qc_p_ordered = df_trim_qc_p.select(ordered_cols)
    
    df_qc, df_clean = cafcore_file_io_0_1_4.write_csv_files(
        df_trim_qc_p_ordered, 
        args['index_cols'], 
        args['file_base_name'], 
        processing_level, 
        accuracy_level, 
        args['path_output'], 
        args['p_suffixes'], 
        args['qc_suffixes'])

def main(args):
    # Generate p1a1
    df_p1a1 = pd.DataFrame([])

    for hy in args['harvest_years']:
        hy_df = generate_p1a1(hy, args)

        if df_p1a1.empty:
            df_p1a1 = hy_df
        else:
            if not hy_df.empty:
                df_p1a1 = pd.concat([df_p1a1, hy_df], ignore_index = True)

    if not cafcore_qc_0_1_4.ensure_columns(df_p1a1, args['dimension_vars'], args['metric_vars'], True):
        raise Exception('Dataset columns are not formatted correctly')

    # Generate p2a1
    df_p2a1 = generate_p2a1(df_p1a1, args)

    # Generate p2a3
    df_p2a3 = generate_p2a3(df_p2a1, args)

    # Convert to polars for output
    output(pl.from_pandas(df_p2a3.astype(args['pandas_schema_p2'])).sort(by = ['HarvestYear', 'ID2']), 2, 3, args)    

    print('...Done')

if __name__ == "__main__":
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {
        'path_input': path_input,
        'path_output': path_output,
        'path_qc_bounds_p2': (path_input / 'qcBounds_p2.csv'),
        'harvest_years': [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
        #'harvest_years': [2018, 2019, 2020, 2021, 2022],
        'dimension_vars': ['HarvestYear', 'FieldId', 'ID2', 'SampleId', 'Latitude', 'Longitude', 'Crop', 'Comments'],
        'metric_vars': ['HarvestDate', 'BiomassAirDry', 'SeedMassAirDry', 'SeedMassOvenDry', 'SeedTestWeight', 'CropExists', 'SeedMoisture', 'SeedGluten', 'SeedStarch', 'SeedOil', 'SeedProtein', 'SeedNitrogen', 'SeedCarbon', 'ResidueNitrogen', 'ResidueCarbon'],
        'index_cols': ['HarvestYear', 'ID2'],
        'observation_ids': [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,348,349,350,351,352,353,354,355,356,357,358,359,360,371,372,373,374,375,376,377,378,379,380,381,394,395,396,397,398,399,400,401,402,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568,569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643,644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675],
        'harvest_areas': {
            2017: 2.4384,
            2018: 2.4384,
            2019: 2.4384,
            2020: 2.4384,
            2021: 2.4257,
            2022: 2.4384,
            2023: 2,
            2024: 2.4257
        },
        'qc_suffixes': ['_qcApplied', '_qcResult', '_qcPhrase'],
        'p_suffixes': ['_P1', '_P2', '_P3'],
        'pandas_schema_p1': {
            'HarvestYear': 'int64',
            'FieldId': 'object',
            'ID2': 'int64',
            'SampleId': 'object',
            'Latitude': 'float64',
            'Longitude': 'float64',
            'Crop': 'object',
            'Comments': 'object',
            'CropExists_P1': 'int64',
            'HarvestDate_P1': 'datetime64[ns]',
            'BiomassAirDry_P1': 'float64',
            'SeedMassAirDry_P1': 'float64',
            'SeedMassOvenDry_P1': 'float64',
            'SeedTestWeight_P1': 'float64',
            'HarvestDate_P1_qcApplied': 'object',
            'HarvestDate_P1_qcResult': 'object',
            'HarvestDate_P1_qcPhrase': 'object',
            'BiomassAirDry_P1_qcApplied': 'object',
            'BiomassAirDry_P1_qcResult': 'object',
            'BiomassAirDry_P1_qcPhrase': 'object',
            'SeedMassAirDry_P1_qcApplied': 'object',
            'SeedMassAirDry_P1_qcResult': 'object',
            'SeedMassAirDry_P1_qcPhrase': 'object',
            'SeedMassOvenDry_P1_qcApplied': 'object',
            'SeedMassOvenDry_P1_qcResult': 'object',
            'SeedMassOvenDry_P1_qcPhrase': 'object',
            'SeedTestWeight_P1_qcApplied': 'object',
            'SeedTestWeight_P1_qcResult': 'object',
            'SeedTestWeight_P1_qcPhrase': 'object',
            'CropExists_P1_qcApplied': 'object',
            'CropExists_P1_qcResult': 'object',
            'CropExists_P1_qcPhrase': 'object',
            'SeedMoisture_P1': 'float64',
            'SeedOil_P1': 'float64',
            'SeedProtein_P1': 'float64',
            'SeedMoisture_P1_qcApplied': 'object',
            'SeedMoisture_P1_qcResult': 'object',
            'SeedMoisture_P1_qcPhrase': 'object',
            'SeedOil_P1_qcApplied': 'object',
            'SeedOil_P1_qcResult': 'object',
            'SeedOil_P1_qcPhrase': 'object',
            'SeedProtein_P1_qcApplied': 'object',
            'SeedProtein_P1_qcResult': 'object',
            'SeedProtein_P1_qcPhrase': 'object',
            'SeedNitrogen_P1': 'float64',
            'SeedCarbon_P1': 'float64',
            'SeedNitrogen_P1_qcPhrase': 'object',
            'SeedCarbon_P1_qcPhrase': 'object',
            'SeedNitrogen_P1_qcApplied': 'object',
            'SeedNitrogen_P1_qcResult': 'object',
            'SeedCarbon_P1_qcApplied': 'object',
            'SeedCarbon_P1_qcResult': 'object',
            'ResidueNitrogen_P1': 'float64',
            'ResidueCarbon_P1': 'float64',
            'ResidueNitrogen_P1_qcPhrase': 'object',
            'ResidueCarbon_P1_qcPhrase': 'object',
            'ResidueNitrogen_P1_qcApplied': 'object',
            'ResidueNitrogen_P1_qcResult': 'object',
            'ResidueCarbon_P1_qcApplied': 'object',
            'ResidueCarbon_P1_qcResult': 'object',
            'SeedStarch_P1': 'float64',
            'SeedGluten_P1': 'float64',
            'SeedStarch_P1_qcApplied': 'object',
            'SeedStarch_P1_qcResult': 'object',
            'SeedStarch_P1_qcPhrase': 'object',
            'SeedGluten_P1_qcApplied': 'object',
            'SeedGluten_P1_qcResult': 'object',
            'SeedGluten_P1_qcPhrase': 'object'
        }
    }
    args['file_base_name'] = 'CookHandHarvest_HY' + str(min(args['harvest_years'])) + '-HY' + str(max(args['harvest_years']))
    args['pandas_schema_p2'] = args['pandas_schema_p1'] | {
        'SeedYieldAirDry_P2': 'float64',
        'SeedYieldAirDry_P2_qcApplied': 'object',
        'SeedYieldAirDry_P2_qcResult': 'object',
        'SeedYieldAirDry_P2_qcPhrase': 'object',
        'SeedYieldOvenDry_P2': 'float64',
        'SeedYieldOvenDry_P2_qcApplied': 'object',
        'SeedYieldOvenDry_P2_qcResult': 'object',
        'SeedYieldOvenDry_P2_qcPhrase': 'object',
        'BiomassAirDryPerArea_P2': 'float64',
        'BiomassAirDryPerArea_P2_qcApplied': 'object',
        'BiomassAirDryPerArea_P2_qcResult': 'object',
        'BiomassAirDryPerArea_P2_qcPhrase': 'object',
        'ResidueMassAirDryPerArea_P2': 'object',
        'ResidueMassAirDryPerArea_P2_qcApplied': 'object',
        'ResidueMassAirDryPerArea_P2_qcResult': 'object',
        'ResidueMassAirDryPerArea_P2_qcPhrase': 'object'
    }

    main(args)