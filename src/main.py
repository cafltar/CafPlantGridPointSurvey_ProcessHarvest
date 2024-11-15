import pathlib
import pandas as pd
import polars as pl
import sys

import read_transform_qa

from importlib.machinery import SourceFileLoader
cafcore_qc_0_1_4 = SourceFileLoader('qc', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/qc.py').load_module()

def generate_p1a1(harvest_year, args):
    print('---- read_transform_qa: ' + str(harvest_year) + ' ----')

    hy_df = pd.DataFrame([])

    # Read in all data for HY, transform to standard structure, process QA change file
    if(harvest_year == 2017):
        hy_df = read_transform_qa.hy2017(args)

    if(harvest_year == 2018):
        hy_df = read_transform_qa.hy2018(args)
        
        # Clean up non-standard columns
        hy_df = hy_df.drop(hy_df.filter(regex='Residue13C').columns, axis=1)
        hy_df = hy_df.drop(hy_df.filter(regex='Grain13C').columns, axis=1)

    if(harvest_year == 2019):
        hy_df = read_transform_qa.hy2019(args)

        # Clean up non-standard columns
        hy_df = hy_df.drop(hy_df.filter(regex='Residue13C').columns, axis=1)
        hy_df = hy_df.drop(hy_df.filter(regex='Grain13C').columns, axis=1)

    if(harvest_year == 2020):
        hy_df = read_transform_qa.hy2020(args)

        # Clean up non-standard columns
        hy_df = hy_df.drop(hy_df.filter(regex='Residue13C').columns, axis=1)
        hy_df = hy_df.drop(hy_df.filter(regex='Grain13C').columns, axis=1)
    
    if(harvest_year == 2021):
        hy_df = read_transform_qa.hy2021(args)
    
    if(harvest_year == 2022):
        hy_df = read_transform_qa.hy2022(args)
    
    if(harvest_year == 2023):
        hy_df = read_transform_qa.hy2023(args)

    # Confirm data are complete records (all observations)
    if len(hy_df['HarvestYear'].unique()) > 1:
        raise Exception('Dataset more years than expected')
    #if len(hy_df['ID2'].unique()) != len(args['observation_ids']):
    #    raise Exception('Dataset has different number of observations than expected')
    if set(hy_df['ID2'].unique()) != set(args['observation_ids']):
        raise Exception('Dataset has different observations than expected')
    

    # Return results
    return hy_df

def calculate(df, args):
    print('---- calculate ----')

def model(df, args):
    print('---- model ----')

def qc(df, args):
    print('---- qc ----')

def output(df, args):
    print('---- output ----')

def main(args):
    print('---- main ----')

    # TODO: I may want to define column names and dtypes here
    df = pd.DataFrame([])

    for hy in args['harvest_years']:
        hy_df = generate_p1a1(hy, args)

        if df.empty:
            df = hy_df
        else:
            df = pd.concat([df, hy_df])

    if not cafcore_qc_0_1_4.ensure_columns(df, args['dimension_vars'], args['metric_vars'], True):
        raise Exception('Dataset columns are not formatted correctly')

    print(df)

if __name__ == "__main__":
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {
        'path_input': path_input,
        'harvest_years': [2017, 2018, 2019, 2020, 2021, 2022, 2023],
        #'harvest_years': [2021],
        'dimension_vars': ['HarvestYear', 'FieldId', 'ID2', 'Longitude', 'Latitude', 'SampleId', 'Crop', 'Comments'],
        'metric_vars': ['HarvestDate', 'BiomassAirDry', 'GrainMassAirDry', 'GrainMassOvenDry', 'GrainTestWeight', 'CropExists', 'GrainMoisture', 'GrainGluten', 'GrainStarch', 'GrainOil', 'GrainProtein', 'GrainNitrogen', 'GrainCarbon', 'ResidueNitrogen', 'ResidueCarbon'],
        'index_cols': ['HarvestYear', 'ID2'],
        'observation_ids': [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,163,164,165,166,167,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,185,186,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267,268,269,270,271,272,273,274,275,276,277,278,279,280,281,282,283,284,285,286,287,288,289,290,291,292,293,297,298,299,300,301,302,303,304,305,306,307,308,309,310,311,312,313,314,315,316,323,324,325,326,327,328,329,330,331,332,333,334,335,336,337,338,348,348,349,350,351,352,353,354,355,356,357,358,359,360,371,372,373,374,375,376,377,378,379,380,381,394,395,396,397,398,399,400,401,402,419,420,421,422,423,424,425,426,427,428,429,430,431,432,433,434,435,436,437,438,439,440,441,442,443,444,445,446,447,448,449,450,451,452,453,454,455,456,457,458,459,460,461,462,463,464,465,466,467,468,469,470,471,472,473,474,475,476,477,478,479,480,481,482,483,484,485,486,487,488,489,490,491,492,493,494,495,496,497,498,499,500,501,502,503,504,505,506,507,508,509,510,511,512,513,514,515,516,517,518,519,520,521,522,523,524,525,526,527,528,529,530,531,532,533,534,535,536,537,538,539,540,541,542,543,544,545,546,547,548,549,550,551,552,553,554,555,556,557,558,559,560,561,562,563,564,565,566,567,568,569,570,571,572,573,574,575,576,577,578,579,580,581,582,583,584,585,586,587,588,589,590,591,592,593,594,595,596,597,598,599,600,601,602,603,604,605,606,607,608,609,610,611,612,613,614,615,616,617,618,619,620,621,622,623,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,639,640,641,642,643,644,645,646,647,648,649,650,651,652,653,654,655,656,657,658,659,660,661,662,663,664,665,666,667,668,669,670,671,672,673,674,675]
    }

    main(args)