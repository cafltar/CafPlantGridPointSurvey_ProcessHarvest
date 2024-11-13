import pathlib
import pandas as pd
import polars as pl
import sys

import read_transform_qa

def generate_p1a1(harvest_year, args):
    print('---- read_transform_qa: ' + str(harvest_year) + ' ----')

    hy_df = pd.DataFrame([])

    # Read in all data for HY, transform to standard structure, process QA change file
    if(harvest_year == 2017):
        hy_df = read_transform_qa.hy2017(args)
    if(harvest_year == 2018):
        hy_df = read_transform_qa.hy2018(args)
    if(harvest_year == 2019):
        hy_df = read_transform_qa.hy2019(args)
    if(harvest_year == 2020):
        hy_df = read_transform_qa.hy2020(args)
    if(harvest_year == 2021):
        hy_df = read_transform_qa.hy2021(args)
    if(harvest_year == 2022):
        hy_df = read_transform_qa.hy2022(args)
    if(harvest_year == 2023):
        hy_df = read_transform_qa.hy2023(args)

    # Confirm data structure and data are complete

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

    print(df)

if __name__ == "__main__":
    path_data = pathlib.Path.cwd() / 'data'
    path_input = path_data / 'input'
    path_output = path_data / 'output'

    path_output.mkdir(parents=True, exist_ok=True)
    
    args = {
        'path_input': path_input,
        'harvest_years': [2017, 2018, 2019, 2020, 2021, 2022, 2023],
        'dimension_vars': ['HarvestYear', 'FieldId', 'ID2', 'Longitude', 'Latitude', 'SampleId', 'Crop', 'Comments']

    }

    main(args)