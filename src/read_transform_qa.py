import pandas as pd
import numpy as np
import pathlib
import glob
import datetime
import sys

from importlib.machinery import SourceFileLoader
cafcore_qc_0_1_4 = SourceFileLoader('qc', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/qc.py').load_module()
cafcore_cook_transform_0_1_4 = SourceFileLoader('cook_transform', '../../CafLogisticsCorePythonLibrary/CafCore/cafcore/cook_transform.py').load_module()
#https://github.com/cafltar/cafcore/releases/tag/v0.1.3
#import cafcore.qc
#import cafcore.file_io

import core

def hy2017(args):
    harvest_year = 2017
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'HandHarvestHY2017' / 'LTAR_CAF_HY2017_CropBiomass-10-31-2017_IL_20191209.xlsx'
    path_harvest_qa = path_hy / 'qaChangeFile_HandHarvestHY2017.csv'
    
    # TODO: Rename this to be more specific to the actual instrument used
    path_ea = path_hy / 'EANsarV1'
    path_ea_qa = path_hy / 'qaChangeFile_EANsarV1.csv'

    # Harvest DET includes NIR data in 2017
    harvest = process_harvest_2017(path_harvest, path_harvest_qa, harvest_year, args)
    ea = process_ea_nsar_v1(path_ea, path_ea_qa, harvest_year)

    df = harvest.merge(ea, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2018(args):
    harvest_year = 2018
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'HandHarvestHY2018' / 'LTARcafHarSamp2018HYBioGrainMasses10242018_IL_20191209.xlsx'
    path_harvest_qa = path_hy / 'qaChangeFile_HandHarvestHY2018.csv'

    path_ms = path_hy / 'MSNsarV1'
    path_ms_qa = path_hy / 'qaChangeFile_MSNsarV1.csv'

    harvest = process_harvest_2018(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)

    ms = process_ms_nsar_v1(
        path_ms, 
        path_ms_qa, 
        harvest_year)

    # Note: No NIR for garbs
    df = harvest.merge(ms, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2019(args):
    #colNotMeasure = ['HarvestYear', 'FieldId', 'ID2', 'Crop', 'SampleId', 'HarvestDate', 'Comments']
    harvest_year = 2019
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'Harvest01' / 'Harvest01_2019_GP-ART-Lime_INT__20191106_IL_20191209.xlsm'
    path_harvest_qa = path_hy / 'qaChangeFile_Harvest01.csv'

    path_ms = path_hy / 'MSNsarV1'
    path_ms_qa = path_hy / 'qaChangeFile_MSNsarV1.csv'

    path_nir = path_hy / 'NirInfratec1241'
    path_nir_qa = path_hy / 'qaChangeFile_NirInfratec1241.csv'
    
    harvest = process_harvest01(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)

    ms = process_ms_nsar_v1(
        path_ms,
        path_ms_qa,
        harvest_year)

    nir = process_nir_infratec1241(
        path_nir,
        path_nir_qa,
        harvest_year)

    df = harvest.merge(nir, on = 'ID2', how = 'left').merge(ms, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2020(args):
    harvest_year = 2020
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'Harvest01' / 'Harvest01_2020_GP-ART-Lime_INT_YYYYMMDD_Reviewed.xlsm'
    path_harvest_qa = path_hy / 'qaChangeFile_Harvest01.csv'

    path_harvest_reweigh = path_hy / 'Harvest01GrainReweigh' / 'Harvest01_GP_2020_GrainReweigh_ZMS_20230303_Reviewed.xlsm'
    path_harvest_reweigh_qa = path_hy / 'qaChangeFile_Harvest01GrainReweigh.csv'

    path_ms = path_hy / 'MSNsarV1'
    path_ms_qa = path_hy / 'qaChangeFile_MSNsarV1.csv'

    path_nir = path_hy / 'NirInfratec1241'
    path_nir_qa = path_hy / 'qaChangeFile_NirInfratec1241.csv'
    
    harvest = process_harvest01(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)
    
    harvest_reweigh = process_harvest01_grainreweigh(
        path_harvest_reweigh,
        path_harvest_reweigh_qa,
        harvest_year,
        args)
    
    # Grain was reweighed before NIR and then dried in oven in 'reweigh' DET since samples were stored for over a year after threshing
    # Check that rows match then merge
    if(len(harvest['SampleId'].unique()) == len(harvest_reweigh['SampleId'].unique())):
        # Drop GrainMassOvenDry from original and replace with reweigh
        cols = ['SeedMassOvenDry', 'SeedMassOvenDry_qcApplied', 'SeedMassOvenDry_qcResult', 'SeedMassOvenDry_qcPhrase']
        harvest_reweigh_prune = harvest_reweigh[(cols + ['SampleId'])]
        harvest = harvest.drop(cols, axis=1).merge(harvest_reweigh_prune, on = 'SampleId', how = 'left')

    ms = process_ms_nsar_v1(
        path_ms,
        path_ms_qa,
        harvest_year)

    nir = process_nir_infratec1241(
        path_nir,
        path_nir_qa,
        harvest_year)

    df = harvest.merge(nir, on = 'ID2', how = 'left').merge(ms, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2021(args):
    harvest_year = 2021
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'Harvest01' / 'Harvest01_2021_GP-ART-Lime_INT_YYYYMMDD_Reviewed.xlsm'
    path_harvest_qa = path_hy / 'qaChangeFile_Harvest01.csv'

    path_ms = path_hy / 'EANsarV1'
    path_ms_qa = path_hy / 'qaChangeFile_EANsarV1.csv'

    path_nir = path_hy / 'NirOilSeedLab'
    path_nir_qa = path_hy / 'qaChangeFile_NirOilSeedLab.csv'
    
    harvest = process_harvest01(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)

    ea = process_ea_nsar_v1(
        path_ms,
        path_ms_qa,
        harvest_year)

    nir = process_nir_oilseed_lab(
        path_nir, 
        path_nir_qa)

    df = harvest.merge(nir, on = 'ID2', how = 'left').merge(ea, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2022(args):
    harvest_year = 2022
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'Harvest01' / 'Harvest01_2022_GP-ART-Lime_INI_YYYYMMDD_Reviewed.xlsm'
    path_harvest_qa = path_hy / 'qaChangeFile_Harvest01.csv'

    path_ea = path_hy / 'EANsarV1'
    path_ea_qa = path_hy / 'qaChangeFile_EANsarV1.csv'

    path_nir = path_hy / 'NirInfratec1241'
    path_nir_qa = path_hy / 'qaChangeFile_NirInfratec1241.csv'

    harvest = process_harvest01(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)

    ea = process_ea_nsar_v1(
        path_ea,
        path_ea_qa,
        harvest_year)

    nir = process_nir_infratec1241(
        path_nir,
        path_nir_qa,
        harvest_year)

    df = harvest.merge(nir, on = 'ID2', how = 'left').merge(ea, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2023(args):
    harvest_year = 2023
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'Harvest01' / 'Harvest01_2023_GP-ART-Lime_INI_YYYYMMDD_Reviewed.xlsm'
    path_harvest_qa = path_hy / 'qaChangeFile_Harvest01.csv'

    path_ea = path_hy / 'EANsarV1'
    path_ea_qa = path_hy / 'qaChangeFile_EANsarV1.csv'

    # Some data, but still working out methods for a new NIR
    path_nir = path_hy / 'NirInfratecTM'
    path_nir_qa = path_hy / 'qaChangeFile_NirInfratecTM.csv'

    harvest = process_harvest01(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)

    ea = process_ea_nsar_v1(
        path_ea,
        path_ea_qa,
        harvest_year)

    # No data, still working out methods for a new NIR
    nir = process_nir_infratecTM(
        path_nir,
        path_nir_qa,
        harvest_year)

    df = harvest.merge(nir, on = 'ID2', how = 'left').merge(ea, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def hy2024(args):
    harvest_year = 2024
    path_hy = args['path_input'] / 'HarvestYear' / ('HY' + str(harvest_year))
    path_harvest =  path_hy / 'Harvest01' / 'Harvest01_2024_GP-ART-Lime_INI_YYYYMMDD_Reviewed.xlsm'
    path_harvest_qa = path_hy / 'qaChangeFile_Harvest01.csv'

    path_ea = path_hy / 'EANsarV1'
    path_ea_qa = path_hy / 'qaChangeFile_EANsarV1.csv'

    # No data, still working out methods for a new NIR
    path_nir = path_hy / 'NirInfratecTM'
    path_nir_qa = path_hy / 'qaChangeFile_NirInfratecTM.csv'

    harvest = process_harvest01(
        path_harvest,
        path_harvest_qa,
        harvest_year,
        args)

    ea = process_ea_nsar_v1(
        path_ea,
        path_ea_qa,
        harvest_year)

    # No data, still working out methods for a new NIR
    nir = process_nir_infratecTM(
        path_nir,
        path_nir_qa,
        harvest_year)


    df = harvest.merge(nir, on = 'ID2', how = 'left').merge(ea, on = 'ID2', how = 'left')
    df = cafcore_cook_transform_0_1_4.assign_geocoord_to_id2(df)

    return df

def process_harvest_2017(dirPathToHarvestFile, dirPathToQAFile, harvestYear, args):
    '''Loads the hand harvest DET for 2017 into a dataframe, formats it, and applies any changes specified in the quality assurance files
    :rtype: DataFrame
    '''
    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name='Sheet1',
        skiprows = 8,
        nrows = 532,
        na_values = ['N/A', '.', '']
    )

    # Standardize column names
    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: core.parse_fieldId_from_sampleId(row['Total Biomass Barcode ID'], harvestYear), axis=1),
            #ID2 = harvestDetRaw['Total Biomass Barcode ID'].str.split('_', expand = True)[0].str.replace('CE', '').str.replace('CW', ''),
            ID2 = harvest.apply(lambda row: core.parse_id2_from_sampleId(row['Total Biomass Barcode ID'], harvestYear), axis=1),
            SampleId = harvest['Total Biomass Barcode ID'],
            Crop = harvest['Total Biomass Barcode ID'].str.split('_', expand = True)[2],
            HarvestDate = None,
            BiomassAirDry = harvest['Dried Total Biomass mass + bag(g) + bags inside'] - harvest['Average Dried total biomass bag + empty grain bag & empty residue bag inside mass (g)'],
            SeedMassAirDry = np.where(
                (pd.isnull(harvest['Non-Oven dried grain mass (g) Reweighed after being sieved'])),  
                (harvest['Non-Oven dried grain mass (g)'] - harvest['Average Non-Oven dried grain bag mass (g)']),
                (harvest['Non-Oven dried grain mass (g) Reweighed after being sieved'] - harvest['Average Non-Oven dried grain bag mass (g)'])),
            SeedMassOvenDry = harvest['Oven dried grain mass (g)'] - harvest['Average Non-Oven dried grain bag mass (g)'],
            SeedMoisture = harvest['Moisture'],
            SeedProtein = harvest['Protein'],
            SeedStarch = harvest['Starch'],
            SeedGluten = harvest['WGlutDM'],
            SeedTestWeight = harvest['Test Weight\n(Manual, small container if no # in large container column)'],
            CropExists = 1,
            Comments = harvest['Notes and comments by Ian Leslie October 2019']
        )
    )

    colNames = [
        'HarvestYear',
        'FieldId',
        'ID2',
        'SampleId',
        'Crop',
        'HarvestDate',
        'BiomassAirDry',
        'SeedMassAirDry',
        'SeedMassOvenDry',
        'SeedMoisture',
        'SeedProtein',
        'SeedStarch',
        'SeedGluten',
        'SeedTestWeight',
        'CropExists',
        'Comments']

    harvestStandardClean = harvestStandard[colNames]

    #colNotMeasure = ['HarvestYear', 'FieldId', 'ID2', 'SampleId', 'HarvestDate', 'Comments']
    colNotMeasure = args['dimension_vars']

    # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore_qc_0_1_4.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore_qc_0_1_4.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        'SampleId')
    harvestStandardCleanQA = cafcore_qc_0_1_4.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def process_harvest_2018(dirPathToHarvestFile, dirPathToQAFile, harvestYear, args):
    '''Loads the hand harvest DET for 2018 into a dataframe, formats it, and applies any changes specified in the quality assurance files
    :rtype: DataFrame
    '''

    # Read raw data
    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name = 'CAF Harvest Biomass Grain Data',
        skiprows = 7,
        na_values = ['N/A', '.', ''],
        nrows = 532
    )

    harvestStandard = harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: core.parse_fieldId_from_sampleId(row['total biomass bag barcode ID'], harvestYear), axis=1),
            ID2 = harvest.apply(lambda row: core.parse_id2_from_sampleId(row['total biomass bag barcode ID'], harvestYear), axis=1),
            SampleId = harvest['total biomass bag barcode ID'],
            Crop = harvest['total biomass bag barcode ID'].str.split('_', expand = True)[2],
            HarvestDate = harvest.apply(lambda row: core.parse_harvest_date(row['date total biomass collected'], harvestYear), axis=1),
            BiomassAirDry = harvest['dried total biomass mass + bag + residue bag + grain bag (g)'] - harvest['average dried empty total biomass bag +  grain bag + residue bag  (g)'],
            SeedMassAirDry = harvest['non-oven dried grain mass + bag (g)'] - harvest['average empty dried grain bag mass (g)'],
            SeedMassOvenDry = None,
            CropExists = 1,
            Comments = harvest['notes'].astype(str) + '| ' + harvest['Notes by Ian Leslie 10/22/2019'],
            AreaOfInterestID2 = harvest['total biomass bag barcode ID'].str.split('_', expand = True)[0]            
        )

    harvestStandard = harvestStandard[(harvestStandard['AreaOfInterestID2'].str.contains('CW')) | (harvestStandard['AreaOfInterestID2'].str.contains('CE'))]

    colNames = [
        'HarvestYear',
        'FieldId',
        'ID2',
        'SampleId',
        'Crop',
        'HarvestDate',
        'BiomassAirDry',
        'SeedMassAirDry',
        'SeedMassOvenDry',
        'CropExists',
        'Comments']

    harvestStandardClean = harvestStandard[colNames]

    #colNotMeasure = ['HarvestYear', 'FieldId', 'ID2', 'SampleId', 'HarvestDate', 'Comments']
    colNotMeasure = args['dimension_vars']

    # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore_qc_0_1_4.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore_qc_0_1_4.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        'SampleId')
    harvestStandardCleanQA = cafcore_qc_0_1_4.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def process_harvest01(dirPathToHarvestFile, dirPathToQAFile, harvestYear, args):
    '''Loads the hand harvest DET (version 'Harvest01') into a dataframe, formats it, applies any changes specified in the quality assurance files
    :rtype: DataFrame
    '''

    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name=0, 
        skiprows=6,
        na_values=['N/A', '.', '', '`']
    )

    harvest = harvest[(harvest['Project ID'] == 'GP') & ((harvest['Total biomass bag barcode ID'].str.contains('CE')) | (harvest['Total biomass bag barcode ID'].str.contains('CW')))]

    testWtLargeCol = 'Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  '
    testWtSmallCol = 'Manual Test Weight: Small Kettle\n(lbs / bushel)\n(Manual, small container if no # in large container column)'

    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: core.parse_fieldId_from_sampleId(row['Total biomass bag barcode ID'], harvestYear), axis=1),
            ID2 = harvest.apply(lambda row: core.parse_id2_from_sampleId(row['Total biomass bag barcode ID'], harvestYear), axis=1),
            SampleId = harvest['Total biomass bag barcode ID'],
            HarvestDate = harvest.apply(lambda row: core.parse_harvest_date(row['Date total biomass collected (g)'], harvestYear), axis=1),
            Crop = harvest['Total biomass bag barcode ID'].str.split('_', expand = True)[3],
            BiomassAirDry = harvest.apply(lambda row: core.parse_harvest01_biomass(row, 'Dried total biomass mass + biomass bag + residue bag + grain bag (g)',  '2nd dried total biomass bag + biomass bag if required for same sample location (g)', 'Average dried empty total biomass bag +  grain bag + residue bag  (g)', 'Average dried empty total biomass bag (g)'), axis = 1),
            SeedMassAirDry = harvest['Non-oven-dried grain mass + bag (g)'] - harvest['Average empty dried grain bag mass (g)'],
            SeedMassOvenDry = harvest['Oven-dried grain mass + bag (g)'] - harvest['Average empty dried grain bag mass (g)'],
            #SeedTestWeight = harvest.apply(lambda row: int(row[testWtLargeCol]) * 0.0705 if row[testWtLargeCol] else int(row[testWtSmallCol]), axis=1),
            SeedTestWeight = harvest.apply(lambda row: core.parse_test_weight(row, testWtLargeCol, testWtSmallCol), axis = 1),
            #SeedTestWeight = harvest['Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  '] * 0.0705,
            CropExists = 1,
            #Comments = harvest['Notes'].astype(str) + '| ' + harvest['Notes made by Ian Leslie'],
            Comments = harvest['Notes'],
            ProjectID = harvest['Project ID'])
    )

    colNames = [
        'HarvestYear',
        'FieldId',
        'ID2',
        'SampleId',
        'Crop',
        'HarvestDate',
        'BiomassAirDry',
        'SeedMassAirDry',
        'SeedMassOvenDry',
        'SeedTestWeight',
        'CropExists',
        'Comments']

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = args['dimension_vars']

     # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore_qc_0_1_4.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore_qc_0_1_4.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        'SampleId')
    harvestStandardCleanQA = cafcore_qc_0_1_4.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def process_harvest01_grainreweigh(dirPathToHarvestFile, dirPathToQAFile, harvestYear, args):
    '''Loads the hand harvest DET re-weigh (version 'Harvest01') into a dataframe, formats it, applies any changes specified in the quality assurance files
    :rtype: DataFrame
    '''

    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name=0, 
        skiprows=5,
        na_values=['N/A', '.', '']
    )

    harvest = harvest[(harvest['Project ID'] == 'GP') & ((harvest['Total biomass bag barcode ID'].str.contains('CE')) | (harvest['Total biomass bag barcode ID'].str.contains('CW')))]

    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: core.parse_fieldId_from_sampleId(row['Total biomass bag barcode ID'], harvestYear), axis=1),
            ID2 = harvest.apply(lambda row: core.parse_id2_from_sampleId(row['Total biomass bag barcode ID'], harvestYear), axis=1),
            SampleId = harvest['Total biomass bag barcode ID'],
            HarvestDate = harvest.apply(lambda row: core.parse_harvest_date(row['Date total biomass collected (g)'], harvestYear), axis=1),
            Crop = harvest['Total biomass bag barcode ID'].str.split('_', expand = True)[3],
            SeedMassOvenDry = harvest['Oven-dried grain mass + bag (g)'] - harvest['Average empty dried grain bag mass (g)'],
            Comments = harvest['Notes'],
            ProjectID = harvest['Project ID'])
    )

    colNames = [
        'HarvestYear',
        'FieldId',
        'ID2',
        'SampleId',
        'Crop',
        'HarvestDate',
        'SeedMassOvenDry',
        'Comments']

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = args['dimension_vars']

     # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore_qc_0_1_4.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore_qc_0_1_4.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        'SampleId')
    harvestStandardCleanQA = cafcore_qc_0_1_4.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def process_ea_nsar_v1(dirPathToEAFiles, dirPathToQAFile, harvestYear):
    '''Loads all EA files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    '''

    filePaths = dirPathToEAFiles / '*.xls'
    eaFiles = glob.glob(str(filePaths))

    colKeep = [0,1,5,6,8]
    colNames = ['LabId', 'Sample', 'WeightNitrogen', 'WeightCarbon', 'Notes']
    colNamesNotMeasure = ['LabId', 'ID2', 'Sample', 'Notes']

    eaAll = pd.DataFrame(columns = colNames + ['ID2'])

    for eaFile in eaFiles:
        ea = pd.read_excel(eaFile, 
            sheet_name='Summary Table', 
            skiprows=11, 
            usecols=colKeep,
            names=colNames)
        ea = ea.assign(
                ID2 = ea.apply(lambda row: core.parse_id2_from_sampleId(row['Sample'], harvestYear), axis = 1)
            )

        if eaAll.empty:
            eaAll = ea
        else:
            if not ea.empty:
                eaAll = pd.concat([eaAll, ea], axis=0, ignore_index = True)

    # Remove data that did not get an ID2 assigned (since it likely belongs to a different project)
    eaAll = eaAll.dropna(subset=['ID2'])

    # Update/Delete values based on quality assurance review
    eaAllQA = cafcore_qc_0_1_4.quality_assurance(eaAll, dirPathToQAFile, 'LabId')
    eaAllQA = cafcore_qc_0_1_4.initialize_qc(eaAllQA, colNamesNotMeasure)
    eaAllQA = cafcore_qc_0_1_4.set_quality_assurance_applied(eaAllQA,colNamesNotMeasure)

    # Split dataset into grain analysis and residue analysis
    eaAllQASeed = eaAllQA[eaAllQA['Sample'].str.contains('_GR_|_MGR_|_FMGR_|_FMGR', na = False, case = False)]
    eaAllQAResidue = eaAllQA[eaAllQA['Sample'].str.contains('_RES_|_MRES_|_FMRES_|_BIO|_FMRES', na = False, case = False)]

    # Standardize column names
    eaAllQASeedClean = (eaAllQASeed
        .rename(columns = {
            'WeightCarbon': 'SeedCarbon',
            'WeightCarbon_qcApplied': 'SeedCarbon_qcApplied',
            'WeightCarbon_qcResult': 'SeedCarbon_qcResult',
            'WeightCarbon_qcPhrase': 'SeedCarbon_qcPhrase',
            'WeightNitrogen': 'SeedNitrogen',
            'WeightNitrogen_qcApplied': 'SeedNitrogen_qcApplied',
            'WeightNitrogen_qcResult': 'SeedNitrogen_qcResult',
            'WeightNitrogen_qcPhrase': 'SeedNitrogen_qcPhrase',
        })
        .drop(columns = ['LabId', 'Sample', 'Notes']))

    eaAllQAResidueClean = (eaAllQAResidue
        .rename(columns = {
            'WeightCarbon': 'ResidueCarbon',
            'WeightCarbon_qcApplied': 'ResidueCarbon_qcApplied',
            'WeightCarbon_qcResult': 'ResidueCarbon_qcResult',
            'WeightCarbon_qcPhrase': 'ResidueCarbon_qcPhrase',
            'WeightNitrogen': 'ResidueNitrogen',
            'WeightNitrogen_qcApplied': 'ResidueNitrogen_qcApplied',
            'WeightNitrogen_qcResult': 'ResidueNitrogen_qcResult',
            'WeightNitrogen_qcPhrase': 'ResidueNitrogen_qcPhrase',
        })
        .drop(columns = ['LabId', 'Sample', 'Notes']))
    
    # Deal with case that results are only residue, only grain, or both
    if len(eaAllQASeedClean) > 0 and len(eaAllQAResidueClean) == 0:
        return eaAllQASeedClean.merge(eaAllQAResidueClean, on = 'ID2', how = 'left')
    elif len(eaAllQASeedClean) == 0 and len(eaAllQAResidueClean) > 0:
        return eaAllQAResidueClean.merge(eaAllQASeedClean, on = 'ID2', how = 'left')
    else:
        return eaAllQASeedClean.merge(eaAllQAResidueClean, on = 'ID2', how = 'left')
    
def process_ms_nsar_v1(dirPathToMSFiles, dirPathToQAFile, harvestYear):
    '''Loads all MS files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    '''

    filePaths = dirPathToMSFiles / '*.xls'
    msFiles = glob.glob(str(filePaths))

    colKeep = [0,1,4,5,6,8]
    #colNames = ['LabId', 'Sample', 'SampleAmount', 'CArea', 'Delta13C', 'TotalN', 'TotalC', 'Sequence', 'Notes']
    colNames = ['LabId', 'Sample', 'Delta13C', 'TotalN', 'TotalC', 'Notes']
    colNamesNotMeasure = ['LabId', 'ID2', 'Sample', 'Notes']
    msAll = pd.DataFrame(columns = colNames + ['ID2'])

    for msFile in msFiles:
        # Check number of named columns to infer structure of xls file (JLC removed column around 2022)
        headerCheck = pd.read_excel(msFile,
            sheet_name='Summary Table',
            skiprows=10,
            nrows=1)
        if len(headerCheck.columns) == 8:
            colKeep = [0,1,3,4,5,7]

        ms = pd.read_excel(msFile, 
            sheet_name='Summary Table', 
            skiprows=10, 
            usecols=colKeep,
            names=colNames)
        ms = ms.assign(
                ID2 = ms.apply(lambda row: core.parse_id2_from_sampleId(row['Sample'], harvestYear), axis = 1)
            )

        #msAll = msAll.append(ms, ignore_index = True)

        if msAll.empty:
            msAll = ms
        else:
            if not ms.empty:
                msAll = pd.concat([msAll, ms], axis = 0, ignore_index=True)

    # Assign ID2
    #msAll = msAll.assign(
    #    ID2 = msAll.apply(lambda row: parse_id2_from_sampleId(row['Sample'], harvestYear), axis = 1)
    #)

    # Update/Delete values based on quality assurance review
    msAllQA = cafcore_qc_0_1_4.quality_assurance(msAll, dirPathToQAFile, 'LabId')

    msAllQA = cafcore_qc_0_1_4.initialize_qc(msAllQA, colNamesNotMeasure)
    msAllQA = cafcore_qc_0_1_4.set_quality_assurance_applied(msAllQA,colNamesNotMeasure)

    # Split dataset into grain analysis and residue analysis
    msAllQASeed = msAllQA[msAllQA['Sample'].str.contains('_Gr_|_MGr_|_FMGr_', na = False)]
    msAllQAResidue = msAllQA[msAllQA['Sample'].str.contains('_Res_|_MRes_|_FMRes_|_FMRes', na = False)]

    # Standardize column names
    msAllQASeedClean = (msAllQASeed
        .rename(columns = {
            'Delta13C': 'Seed13C',
            'Delta13C_qcApplied': 'Seed13C_qcApplied',
            'Delta13C_qcResult': 'Seed13C_qcResult',
            'Delta13C_qcPhrase': 'Seed13C_qcPhrase',
            'TotalC': 'SeedCarbon',
            'TotalC_qcApplied': 'SeedCarbon_qcApplied',
            'TotalC_qcResult': 'SeedCarbon_qcResult',
            'TotalC_qcPhrase': 'SeedCarbon_qcPhrase',
            'TotalN': 'SeedNitrogen',
            'TotalN_qcApplied': 'SeedNitrogen_qcApplied',
            'TotalN_qcResult': 'SeedNitrogen_qcResult',
            'TotalN_qcPhrase': 'SeedNitrogen_qcPhrase'
        })
        .drop(columns = ['LabId', 'Sample', 'Notes']))

    msAllQAResidueClean = (msAllQAResidue
        .rename(columns = {
            'Delta13C': 'Residue13C',
            'Delta13C_qcApplied': 'Residue13C_qcApplied',
            'Delta13C_qcResult': 'Residue13C_qcResult',
            'Delta13C_qcPhrase': 'Residue13C_qcPhrase',
            'TotalC': 'ResidueCarbon',
            'TotalC_qcApplied': 'ResidueCarbon_qcApplied',
            'TotalC_qcResult': 'ResidueCarbon_qcResult',
            'TotalC_qcPhrase': 'ResidueCarbon_qcPhrase',
            'TotalN': 'ResidueNitrogen',
            'TotalN_qcApplied': 'ResidueNitrogen_qcApplied',
            'TotalN_qcResult': 'ResidueNitrogen_qcResult',
            'TotalN_qcPhrase': 'ResidueNitrogen_qcPhrase'
        })
        .drop(columns = ['LabId', 'Sample', 'Notes']))

    # Deal with case that results are only residue, only grain, or both
    if len(msAllQASeedClean) > 0 and len(msAllQAResidueClean) == 0:
        return msAllQASeedClean.merge(msAllQAResidueClean, on = 'ID2', how = 'left')
    elif len(msAllQASeedClean) == 0 and len(msAllQAResidueClean) > 0:
        return msAllQAResidueClean.merge(msAllQASeedClean, on = 'ID2', how = 'left')
    else:
        return msAllQASeedClean.merge(msAllQAResidueClean, on = 'ID2', how = 'left')
    
def process_nir_infratec1241(dirPathToNirFiles, dirPathToQAFile, harvestYear):
    '''Loads all NIR files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    '''

    filePaths = dirPathToNirFiles / 'NIR*.csv'
    nirFiles = glob.glob(str(filePaths))

    colNames = ['Date_Time', 'ID2', 'ProtDM', 'Moisture', 'StarchDM', 'WGlutDM']
    colNamesNotMeasure = ['Date_Time', 'ID2']

    nirs = pd.DataFrame(columns = colNames)

    for nirFile in nirFiles:
        nir = pd.read_csv(nirFile)
        
        # Make sure this is a GP sample from CW or CE
        nirFilter = nir[(nir['Sample_ID'].str.upper().str.contains('GP')) & ((nir['Sample_ID'].str.upper().str.contains('CW')) | (nir['Sample_ID'].str.upper().str.contains('CE')))]
        nirParse = (nirFilter
            .assign(
                ID2 = nirFilter.apply(lambda row: core.parse_id2_from_nir_sample_id(row['Sample_ID'], harvestYear), axis=1)
            )
        )

        #nirs = nirs.append(nirParse, ignore_index = True, sort = True)
        if nirs.empty:
            nirs = nirParse
        else:
            if not nirParse.empty:
                nirs = pd.concat([nirs, nirParse], axis = 0, join = 'outer', ignore_index = True, sort = True)

    nirs = nirs[colNames]

    nirs = nirs.drop_duplicates()

    nirsQA = cafcore_qc_0_1_4.initialize_qc(nirs, colNamesNotMeasure)
    nirsQA = cafcore_qc_0_1_4.quality_assurance(nirsQA, dirPathToQAFile, 'Date_Time')
    nirsQA = cafcore_qc_0_1_4.set_quality_assurance_applied(nirsQA, colNamesNotMeasure)

    nirsQAClean = (nirsQA
        .drop(columns = ['Date_Time'])
        .rename(columns={
            'ProtDM': 'SeedProtein', 
            'ProtDM_qcApplied': 'SeedProtein_qcApplied',
            'ProtDM_qcResult': 'SeedProtein_qcResult',
            'ProtDM_qcPhrase': 'SeedProtein_qcPhrase',
            'Moisture': 'SeedMoisture', 
            'Moisture_qcApplied': 'SeedMoisture_qcApplied',
            'Moisture_qcResult': 'SeedMoisture_qcResult',
            'Moisture_qcPhrase': 'SeedMoisture_qcPhrase',
            'StarchDM': 'SeedStarch',
            'StarchDM_qcApplied': 'SeedStarch_qcApplied',
            'StarchDM_qcResult': 'SeedStarch_qcResult',
            'StarchDM_qcPhrase': 'SeedStarch_qcPhrase',
            'WGlutDM': 'SeedGluten',
            'WGlutDM_qcApplied': 'SeedGluten_qcApplied',
            'WGlutDM_qcResult': 'SeedGluten_qcResult',
            'WGlutDM_qcPhrase': 'SeedGluten_qcPhrase'}))

    return nirsQAClean

def process_nir_oilseed_lab(dirPathToNirFiles, dirPathToQAFile):
    '''Loads all NIR files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    '''

    filePaths = dirPathToNirFiles / '*.xlsx'
    nirFiles = glob.glob(str(filePaths))

    colNames = ['ProjectId', 'ID2', 'DryMatterContent', 'OilContent', 'ProteinContent']
    colNamesNotMeasure = ['ProjectId', 'ID2', 'IDCol']

    nirs = pd.DataFrame()

    for nirFile in nirFiles:
        nir = pd.read_excel(nirFile, skiprows=5, names=colNames)
        
        # Make sure this is a GP sample from CW or CE
        #nirFilter = nir[(nir['Sample_ID'].str.upper().str.contains('GP')) & ((nir['Sample_ID'].str.upper().str.contains('CW')) | (nir['Sample_ID'].str.upper().str.contains('CE')))]
        nirFilter = nir[(nir['ProjectId'].isin(['CE', 'CW']))]
        nirParse = (
            nirFilter
                .assign(
                    IDCol = nir['ProjectId'] + nir['ID2'].astype(str),
                    MoistureContent = 100 - nir['DryMatterContent'])
                .drop(columns = ['DryMatterContent']))

        if nirs.empty:
            nirs = nirParse
        else:
            if not nirs.empty:
                nirs = pd.concat([nirs, nirParse], axis = 0, join = 'outer', ignore_index = True, sort = True)

    nirs = nirs.drop_duplicates()

    nirsQA = cafcore_qc_0_1_4.initialize_qc(nirs, colNamesNotMeasure)
    nirsQA = cafcore_qc_0_1_4.quality_assurance(nirsQA, dirPathToQAFile, 'IDCol')
    nirsQA = cafcore_qc_0_1_4.set_quality_assurance_applied(nirsQA, colNamesNotMeasure)

    nirsQAClean = (nirsQA
        .drop(columns = ['IDCol', 'ProjectId'])
        .rename(columns={
            'ProteinContent': 'SeedProtein', 
            'ProteinContent_qcApplied': 'SeedProtein_qcApplied',
            'ProteinContent_qcResult': 'SeedProtein_qcResult',
            'ProteinContent_qcPhrase': 'SeedProtein_qcPhrase',
            'MoistureContent': 'SeedMoisture', 
            'MoistureContent_qcApplied': 'SeedMoisture_qcApplied',
            'MoistureContent_qcResult': 'SeedMoisture_qcResult',
            'MoistureContent_qcPhrase': 'SeedMoisture_qcPhrase',
            'OilContent': 'SeedOil',
            'OilContent_qcApplied': 'SeedOil_qcApplied',
            'OilContent_qcResult': 'SeedOil_qcResult',
            'OilContent_qcPhrase': 'SeedOil_qcPhrase'}))

    return nirsQAClean

def process_nir_infratecTM(dirPathToNirFiles, dirPathToQAFile, harvestYear):
    '''Loads all NIR files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    '''

    # NOTE: 2025-09-03: This is not done, we're still working on the structure of the new NIR file
    # NOTE: 2025-09-09: Updated based on SC file, which only has Oil, TW, and Moisture

    filePaths = dirPathToNirFiles / '*.csv'
    nirFiles = glob.glob(str(filePaths))

    colNames = ['Analysis Time', 'Product Name', 'Sample Number', 'Moisture', 'TW', 'Oil0%', 'Protein0%', 'Starch0%', 'Gluten0%', 'ID2']
    colNamesNotMeasure = ['Analysis Time', 'Product Name', 'Sample Number', 'ID2']

    nirs = pd.DataFrame(columns = colNames)

    for nirFile in nirFiles:
        nir = pd.read_csv(nirFile)
        
        # Make sure this is a GP sample from CW or CE
        nirFilter = nir[(nir['Sample Number'].str.upper().str.contains('GP')) & ((nir['Sample Number'].str.upper().str.contains('CW')) | (nir['Sample Number'].str.upper().str.contains('CE')))]
        nirParse = (nirFilter
            .assign(
                ID2 = nirFilter.apply(lambda row: core.parse_id2_from_sampleId(row['Sample Number'], harvestYear), axis=1)
            )
        )

        #nirs = nirs.append(nirParse, ignore_index = True, sort = True)
        if nirs.empty:
            nirs = nirParse
        else:
            if not nirParse.empty:
                nirs = pd.concat([nirs, nirParse], axis = 0, join = 'outer', ignore_index = True, sort = True)

    # Drop columns not in colNames
    for col in nirs.columns:
        if col not in colNames:
            nirs = nirs.drop(columns = [col])

    nirs = nirs.drop_duplicates()

    nirsQA = cafcore_qc_0_1_4.initialize_qc(nirs, colNamesNotMeasure)
    nirsQA = cafcore_qc_0_1_4.quality_assurance(nirsQA, dirPathToQAFile, 'Analysis Time')
    nirsQA = cafcore_qc_0_1_4.set_quality_assurance_applied(nirsQA, colNamesNotMeasure)

    nirsQAClean = (nirsQA
        .drop(columns = ['Analysis Time', 'Product Name', 'Sample Number'])
        .rename(columns={
            'Protein0%': 'SeedProtein', 
            'Protein0%_qcApplied': 'SeedProtein_qcApplied',
            'Protein0%_qcResult': 'SeedProtein_qcResult',
            'Protein0%_qcPhrase': 'SeedProtein_qcPhrase',
            'Moisture': 'SeedMoisture', 
            'Moisture_qcApplied': 'SeedMoisture_qcApplied',
            'Moisture_qcResult': 'SeedMoisture_qcResult',
            'Moisture_qcPhrase': 'SeedMoisture_qcPhrase',
            'Starch0%': 'SeedStarch',
            'Starch0%_qcApplied': 'SeedStarch_qcApplied',
            'Starch0%_qcResult': 'SeedStarch_qcResult',
            'Starch0%_qcPhrase': 'SeedStarch_qcPhrase',
            'Gluten0%': 'SeedGluten',
            'Gluten0%_qcApplied': 'SeedGluten_qcApplied',
            'Gluten0%_qcResult': 'SeedGluten_qcResult',
            'Gluten0%_qcPhrase': 'SeedGluten_qcPhrase',
            'Oil0%': 'SeedOil',
            'Oil0%_qcApplied': 'SeedOil_qcApplied',
            'Oil0%_qcResult': 'SeedOil_qcResult',
            'Oil0%_qcPhrase': 'SeedOil_qcPhrase',
            'TW': 'SeedTestWeight',
            'TW_qcApplied': 'SeedTestWeight_qcApplied',
            'TW_qcResult': 'SeedTestWeight_qcResult',
            'TW_qcPhrase': 'SeedTestWeight_qcPhrase'}))

    return nirsQAClean