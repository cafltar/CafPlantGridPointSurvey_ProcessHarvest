import pandas as pd
import numpy as np
import pathlib
import glob
import datetime

#https://github.com/cafltar/cafcore/releases/tag/v0.1.0
#import cafcore.qc
#import cafcore.file_io

import importlib.util
spec_trans = importlib.util.spec_from_file_location("cafcore", "C:\\Dev\\Projects\\CafLogisticsCorePythonLibrary\\CafCore\\cafcore\\cook_transform.py")
caf_transform = importlib.util.module_from_spec(spec_trans)
spec_trans.loader.exec_module(caf_transform)

spec_qc = importlib.util.spec_from_file_location("cafcore", "C:\\Dev\\Projects\\CafLogisticsCorePythonLibrary\\CafCore\\cafcore\\qc.py")
caf_qc = importlib.util.module_from_spec(spec_qc)
spec_qc.loader.exec_module(caf_qc)

spec_io = importlib.util.spec_from_file_location("cafcore", "C:\\Dev\\Projects\\CafLogisticsCorePythonLibrary\\CafCore\\cafcore\\file_io.py")
caf_io = importlib.util.module_from_spec(spec_io)
spec_io.loader.exec_module(caf_io)

def parse_id2_from_sampleId(sampleId, harvestYear):
    """Extracts the embedded ID2 value from the given sampleId. Expects sampleId to be in format similar to "CE39_Bio_GB_2018_22-B", split by "_"
    Returns None if harvest year is less than 2017 or sample ID does not start with "CE" or "CW"
    rtype: int
    """

    # ensures passed type is a string
    if not isinstance(sampleId, str):
        return None

    # ensures sampleId is for Cook field (starts with either "CE" or "CW")
    if not sampleId.startswith(tuple(["CE", "CW"])):
        return None

    id2 = 0
    if(harvestYear >= 2017):
        id2 = int(sampleId.upper().split("_")[0].replace("CE", "").replace("CW", ""))
    else:
        id2 = None

    return id2

def parse_fieldId_from_sampleId(sampleId, harvestYear):
    """Extracts the embedded Field ID (CE or CW) from the given sampleId. Expects sampleId to be in format similar to "CE39_Bio_GB_2018_22-B", split by "_"
    Returns None if harvest year is less than 2017 or sample ID does not start with "CE" or "CW"
    rtype: int
    """

    # ensures passed type is a string
    if not isinstance(sampleId, str):
        return None

    # ensures sampleId is for Cook field (starts with either "CE" or "CW")
    if not sampleId.startswith(tuple(["CE", "CW"])):
        return None
    
    fieldId = ""
    if(harvestYear >= 2017):
        fieldId = sampleId[:2]
    else:
        fieldId = None

    return fieldId

def parse_id2_from_nir_sample_id(sampleId, harvestYear):
    """Extracts the embedded ID2 value from the given sampleId. Expects sampleId to be in format similar to CW516GP2019WWGr, split by harvestYear (e.g. 2019)
    :rtype: int
    """

    if not isinstance(sampleId, str):
        return None

    id2 = 0

    if(harvestYear >= 2017):
        id2 = int(sampleId.upper().split(str(harvestYear))[0].replace("GP", "").replace("CE", "").replace("CW", ""))
    else:
        id2 = None
    
    return id2

def parse_harvest_date(harvestDate, harvestYear):
    """Converts the harvest date to ISO format YYYY-MM-DD and outputs as a string
    """

    result = ""
    if(harvestYear >= 2017):
        try:
            harvestDatetime = pd.to_datetime(harvestDate)
            result = datetime.datetime.strftime(harvestDatetime, '%Y-%m-%d')
        except:
            result = None

    return result

def parse_test_weight(row, largeTestWeightCol, smallTestWeightCol):
    """Returns the small test weight if it's not null or the large test weight multiplied by 0.0705 if the small weight is null
    """

    small = row[smallTestWeightCol]
    large = row[largeTestWeightCol]

    result = None

    if(pd.isna(small)):
        if(not pd.isna(large)):
            result = float(large) * 0.0705
    else:
        result = float(small)

    return result

def read_transform_hand_harvest_2017(dirPathToHarvestFile, dirPathToQAFile, harvestYear):
    """Loads the hand harvest DET for 2017 into a dataframe, formats it, and applies any changes specified in the quality assurance files
    :rtype: DataFrame
    """

    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name="Sheet1",
        skiprows = 8,
        nrows = 532,
        na_values = ["N/A", ".", ""]
    )

    # Standardize column names
    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: parse_fieldId_from_sampleId(row["Total Biomass Barcode ID"], harvestYear), axis=1),
            #ID2 = harvestDetRaw["Total Biomass Barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            ID2 = harvest.apply(lambda row: parse_id2_from_sampleId(row["Total Biomass Barcode ID"], harvestYear), axis=1),
            SampleId = harvest["Total Biomass Barcode ID"],
            Crop = harvest["Total Biomass Barcode ID"].str.split("_", expand = True)[2],
            HarvestDate = None,
            BiomassDry = harvest["Dried Total Biomass mass + bag(g) + bags inside"] - harvest["Average Dried total biomass bag + empty grain bag & empty residue bag inside mass (g)"],
            GrainMassDry = np.where(
                (pd.isnull(harvest["Non-Oven dried grain mass (g) Reweighed after being sieved"])),  
                (harvest["Non-Oven dried grain mass (g)"] - harvest["Average Non-Oven dried grain bag mass (g)"]),
                (harvest["Non-Oven dried grain mass (g) Reweighed after being sieved"] - harvest["Average Non-Oven dried grain bag mass (g)"])),
            GrainMoisture = harvest["Moisture"],
            GrainProtein = harvest["Protein"],
            GrainStarch = harvest["Starch"],
            GrainGluten = harvest["WGlutDM"],
            GrainTestWeight = harvest["Test Weight\n(Manual, small container if no # in large container column)"],
            CropExists = 1,
            Comments = harvest["Notes and comments by Ian Leslie October 2019"]
        )
    )

    colNames = [
        "HarvestYear",
        "FieldId",
        "ID2",
        "SampleId",
        "Crop",
        "HarvestDate",
        "BiomassDry",
        "GrainMassDry",
        "GrainMoisture",
        "GrainProtein",
        "GrainStarch",
        "GrainGluten",
        "GrainTestWeight",
        "CropExists",
        "Comments"]

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = ["HarvestYear", "FieldId", "ID2", "SampleId", "HarvestDate", "Comments"]

    # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = caf_qc.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = caf_qc.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        "SampleId")
    harvestStandardCleanQA = caf_qc.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def read_transform_hand_harvest_2018(dirPathToHarvestFile, dirPathToQAFile, harvestYear):
    """Loads the hand harvest DET for 2018 into a dataframe, formats it, and applies any changes specified in the quality assurance files
    :rtype: DataFrame
    """

    # Read raw data
    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name = "CAF Harvest Biomass Grain Data",
        skiprows = 7,
        na_values = ["N/A", ".", ""],
        nrows = 532
    )

    harvestStandard = harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: parse_fieldId_from_sampleId(row["total biomass bag barcode ID"], harvestYear), axis=1),
            #ID2 = harvestDetRaw["total biomass bag barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            ID2 = harvest.apply(lambda row: parse_id2_from_sampleId(row["total biomass bag barcode ID"], harvestYear), axis=1),
            SampleId = harvest["total biomass bag barcode ID"],
            Crop = harvest["total biomass bag barcode ID"].str.split("_", expand = True)[2],
            HarvestDate = harvest.apply(lambda row: parse_harvest_date(row["date total biomass collected"], harvestYear), axis=1),
            BiomassDry = harvest["dried total biomass mass + bag + residue bag + grain bag (g)"] - harvest["average dried empty total biomass bag +  grain bag + residue bag  (g)"],
            GrainMassDry = harvest["non-oven dried grain mass + bag (g)"] - harvest["average empty dried grain bag mass (g)"],
            CropExists = 1,
            Comments = harvest["notes"].astype(str) + "| " + harvest["Notes by Ian Leslie 10/22/2019"],
            AreaOfInterestID2 = harvest["total biomass bag barcode ID"].str.split("_", expand = True)[0]            
        )

    harvestStandard = harvestStandard[(harvestStandard['AreaOfInterestID2'].str.contains('CW')) | (harvestStandard['AreaOfInterestID2'].str.contains('CE'))]

    colNames = [
        "HarvestYear",
        "FieldId",
        "ID2",
        "SampleId",
        "Crop",
        "HarvestDate",
        "BiomassDry",
        "GrainMassDry",
        "CropExists",
        "Comments"]

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = ["HarvestYear", "FieldId", "ID2", "SampleId", "HarvestDate", "Comments"]

    # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = caf_qc.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = caf_qc.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        "SampleId")
    harvestStandardCleanQA = caf_qc.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def read_transform_harvest01Det(dirPathToHarvestFile, dirPathToQAFile, harvestYear):
    """Loads the hand harvest DET (version "Harvest01") into a dataframe, formats it, applies any changes specified in the quality assurance files
    :rtype: DataFrame
    """

    harvest = pd.read_excel(
        dirPathToHarvestFile,
        sheet_name=0, 
        skiprows=6,
        na_values=["N/A", ".", ""]
    )

    harvest = harvest[(harvest['Project ID'] == 'GP') & ((harvest['Total biomass bag barcode ID'].str.contains('CE')) | (harvest['Total biomass bag barcode ID'].str.contains('CW')))]

    testWtLargeCol = "Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  "
    testWtSmallCol = "Manual Test Weight: Small Kettle\n(lbs / bushel)\n(Manual, small container if no # in large container column)"

    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            FieldId = harvest.apply(lambda row: parse_fieldId_from_sampleId(row["Total biomass bag barcode ID"], harvestYear), axis=1),
            ID2 = harvest.apply(lambda row: parse_id2_from_sampleId(row["Total biomass bag barcode ID"], harvestYear), axis=1),
            SampleId = harvest["Total biomass bag barcode ID"],
            HarvestDate = harvest.apply(lambda row: parse_harvest_date(row["Date total biomass collected (g)"], harvestYear), axis=1),
            Crop = harvest["Total biomass bag barcode ID"].str.split("_", expand = True)[3],
            BiomassDry = harvest["Dried total biomass (g)"],
            GrainMassDry = harvest["Non-oven-dried grain (g)"],
            #GrainTestWeight = harvest.apply(lambda row: int(row[testWtLargeCol]) * 0.0705 if row[testWtLargeCol] else int(row[testWtSmallCol]), axis=1),
            GrainTestWeight = harvest.apply(lambda row: parse_test_weight(row, testWtLargeCol, testWtSmallCol), axis = 1),
            #GrainTestWeight = harvest["Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  "] * 0.0705,
            CropExists = 1,
            #Comments = harvest["Notes"].astype(str) + "| " + harvest["Notes made by Ian Leslie"],
            Comments = harvest["Notes"],
            ProjectID = harvest["Project ID"])
    )

    colNames = [
        "HarvestYear",
        "FieldId",
        "ID2",
        "SampleId",
        "Crop",
        "HarvestDate",
        "BiomassDry",
        "GrainMassDry",
        "GrainTestWeight",
        "CropExists",
        "Comments"]

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = ["HarvestYear", "FieldId", "ID2", "SampleId", "HarvestDate", "Comments"]

     # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = caf_qc.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = caf_qc.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        "SampleId")
    harvestStandardCleanQA = caf_qc.set_quality_assurance_applied(
        harvestStandardCleanQA,
        colNotMeasure)

    return harvestStandardCleanQA

def read_transform_nir(dirPathToNirFiles, dirPathToQAFile, harvestYear):
    """Loads all NIR files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    """

    filePaths = dirPathToNirFiles / "NIR*.csv"
    nirFiles = glob.glob(str(filePaths))

    colNames = ["Date_Time", "ID2", "ProtDM", "Moisture", "StarchDM", "WGlutDM"]
    colNamesNotMeasure = ["Date_Time", "ID2"]

    nirs = pd.DataFrame(columns = colNames)

    for nirFile in nirFiles:
        nir = pd.read_csv(nirFile)
        
        # Make sure this is a GP sample from CW or CE
        nirFilter = nir[(nir['Sample_ID'].str.upper().str.contains('GP')) & ((nir['Sample_ID'].str.upper().str.contains('CW')) | (nir['Sample_ID'].str.upper().str.contains('CE')))]
        nirParse = (nirFilter
            .assign(
                ID2 = nirFilter.apply(lambda row: parse_id2_from_nir_sample_id(row["Sample_ID"], harvestYear), axis=1)
            )
        )

        #nirs = nirs.append(nirParse, ignore_index = True, sort = True)
        nirs = pd.concat([nirs, nirParse], axis = 0, join = 'outer', ignore_index = True, sort = True)

    nirs = nirs[colNames]

    nirs = nirs.drop_duplicates()

    nirsQA = caf_qc.initialize_qc(nirs, colNamesNotMeasure)
    nirsQA = caf_qc.quality_assurance(nirsQA, dirPathToQAFile, "Date_Time")
    nirsQA = caf_qc.set_quality_assurance_applied(nirsQA, colNamesNotMeasure)

    nirsQAClean = (nirsQA
        .drop(columns = ["Date_Time"])
        .rename(columns={
            "ProtDM": "GrainProtein", 
            "ProtDM_qcApplied": "GrainProtein_qcApplied",
            "ProtDM_qcResult": "GrainProtein_qcResult",
            "ProtDM_qcPhrase": "GrainProtein_qcPhrase",
            "Moisture": "GrainMoisture", 
            "Moisture_qcApplied": "GrainMoisture_qcApplied",
            "Moisture_qcResult": "GrainMoisture_qcResult",
            "Moisture_qcPhrase": "GrainMoisture_qcPhrase",
            "StarchDM": "GrainStarch",
            "StarchDM_qcApplied": "GrainStarch_qcApplied",
            "StarchDM_qcResult": "GrainStarch_qcResult",
            "StarchDM_qcPhrase": "GrainStarch_qcPhrase",
            "WGlutDM": "GrainGluten",
            "WGlutDM_qcApplied": "GrainGluten_qcApplied",
            "WGlutDM_qcResult": "GrainGluten_qcResult",
            "WGlutDM_qcPhrase": "GrainGluten_qcPhrase"}))

    return nirsQAClean

def read_transform_nir_oilseed_lab(dirPathToNirFiles, dirPathToQAFile, harvestYear):
    """Loads all NIR files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    """

    filePaths = dirPathToNirFiles / "*.xlsx"
    nirFiles = glob.glob(str(filePaths))

    colNames = ["ProjectId", "ID2", "DryMatterContent", "OilContent", "ProteinContent"]
    colNamesNotMeasure = ["ProjectId", "ID2", "IDCol"]

    nirs = pd.DataFrame()

    for nirFile in nirFiles:
        nir = pd.read_excel(nirFile, skiprows=5, names=colNames)
        
        # Make sure this is a GP sample from CW or CE
        #nirFilter = nir[(nir['Sample_ID'].str.upper().str.contains('GP')) & ((nir['Sample_ID'].str.upper().str.contains('CW')) | (nir['Sample_ID'].str.upper().str.contains('CE')))]
        nirFilter = nir[(nir["ProjectId"].isin(["CE", "CW"]))]
        nirParse = (
            nirFilter
                .assign(
                    IDCol = nir["ProjectId"] + nir["ID2"].astype(str),
                    MoistureContent = 100 - nir["DryMatterContent"])
                .drop(columns = ["DryMatterContent"]))

        #nirs = nirs.append(nirParse, ignore_index = True, sort = True)
        nirs = pd.concat([nirs, nirParse], axis = 0, join = 'outer', ignore_index = True, sort = True)

    nirs = nirs.drop_duplicates()

    nirsQA = caf_qc.initialize_qc(nirs, colNamesNotMeasure)
    nirsQA = caf_qc.quality_assurance(nirsQA, dirPathToQAFile, "IDCol")
    nirsQA = caf_qc.set_quality_assurance_applied(nirsQA, colNamesNotMeasure)

    nirsQAClean = (nirsQA
        .drop(columns = ["IDCol", "ProjectId"])
        .rename(columns={
            "ProteinContent": "GrainProtein", 
            "ProteinContent_qcApplied": "GrainProtein_qcApplied",
            "ProteinContent_qcResult": "GrainProtein_qcResult",
            "ProteinContent_qcPhrase": "GrainProtein_qcPhrase",
            "MoistureContent": "GrainMoisture", 
            "MoistureContent_qcApplied": "GrainMoisture_qcApplied",
            "MoistureContent_qcResult": "GrainMoisture_qcResult",
            "MoistureContent_qcPhrase": "GrainMoisture_qcPhrase",
            "OilContent": "GrainOil",
            "OilContent_qcApplied": "GrainOil_qcApplied",
            "OilContent_qcResult": "GrainOil_qcResult",
            "OilContent_qcPhrase": "GrainOil_qcPhrase"}))

    return nirsQAClean

def read_transform_ea(dirPathToEAFiles, dirPathToQAFile, harvestYear):
    """Loads all EA files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    """

    filePaths = dirPathToEAFiles / "*.xls"
    eaFiles = glob.glob(str(filePaths))

    colKeep = [0,1,5,6,8]
    colNames = ["LabId", "Sample", "WeightNitrogen", "WeightCarbon", "Notes"]
    colNamesNotMeasure = ["LabId", "ID2", "Sample", "Notes"]

    eaAll = pd.DataFrame(columns = colNames + ["ID2"])

    for eaFile in eaFiles:
        ea = pd.read_excel(eaFile, 
            sheet_name="Summary Table", 
            skiprows=11, 
            usecols=colKeep,
            names=colNames)
        ea = ea.assign(
                ID2 = ea.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
            )

        #eaAll = eaAll.append(ea, ignore_index = True)
        eaAll = pd.concat([eaAll, ea], axis=0, ignore_index = True)

    # Assign ID2
    #eaAll = eaAll.assign(
    #    ID2 = eaAll.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
    #)

    # Update/Delete values based on quality assurance review
    eaAllQA = caf_qc.quality_assurance(eaAll, dirPathToQAFile, "LabId")

    eaAllQA = caf_qc.initialize_qc(eaAllQA, colNamesNotMeasure)
    eaAllQA = caf_qc.set_quality_assurance_applied(eaAllQA,colNamesNotMeasure)

    # Split dataset into grain analysis and residue analysis
    eaAllQAGrain = eaAllQA[eaAllQA["Sample"].str.contains("_Gr_|_MGr_|_FMGr_", na = False)]
    eaAllQAResidue = eaAllQA[eaAllQA["Sample"].str.contains("_Res_|_MRes_|_FMRes_", na = False)]


    # Standardize column names
    eaAllQAGrainClean = (eaAllQAGrain
        .rename(columns = {
            "WeightCarbon": "GrainCarbon",
            "WeightCarbon_qcApplied": "GrainCarbon_qcApplied",
            "WeightCarbon_qcResult": "GrainCarbon_qcResult",
            "WeightCarbon_qcPhrase": "GrainCarbon_qcPhrase",
            "WeightNitrogen": "GrainNitrogen",
            "WeightNitrogen_qcApplied": "GrainNitrogen_qcApplied",
            "WeightNitrogen_qcResult": "GrainNitrogen_qcResult",
            "WeightNitrogen_qcPhrase": "GrainNitrogen_qcPhrase",
        })
        .drop(columns = ["LabId", "Sample", "Notes"]))

    eaAllQAResidueClean = (eaAllQAResidue
        .rename(columns = {
            "WeightCarbon": "ResidueCarbon",
            "WeightCarbon_qcApplied": "ResidueCarbon_qcApplied",
            "WeightCarbon_qcResult": "ResidueCarbon_qcResult",
            "WeightCarbon_qcPhrase": "ResidueCarbon_qcPhrase",
            "WeightNitrogen": "ResidueNitrogen",
            "WeightNitrogen_qcApplied": "ResidueNitrogen_qcApplied",
            "WeightNitrogen_qcResult": "ResidueNitrogen_qcResult",
            "WeightNitrogen_qcPhrase": "ResidueNitrogen_qcPhrase",
        })
        .drop(columns = ["LabId", "Sample", "Notes"]))
    
    # Deal with case that results are only residue, only grain, or both
    if len(eaAllQAGrainClean) > 0 and len(eaAllQAResidueClean) == 0:
        return eaAllQAGrainClean.merge(eaAllQAResidueClean, on = "ID2", how = "left")
    elif len(eaAllQAGrainClean) == 0 and len(eaAllQAResidueClean) > 0:
        return eaAllQAResidueClean.merge(eaAllQAGrainClean, on = "ID2", how = "left")
    else:
        return eaAllQAGrainClean.merge(eaAllQAResidueClean, on = "ID2", how = "left")

def read_transform_ms(dirPathToMSFiles, dirPathToQAFile, harvestYear):
    """Loads all MS files into a dataframe, formats it, and makes quality assurance changes as specified
    :rtype: DataFrame
    """

    filePaths = dirPathToMSFiles / "*.xls"
    msFiles = glob.glob(str(filePaths))

    colKeep = [0,1,4,5,6,8]
    #colNames = ["LabId", "Sample", "SampleAmount", "CArea", "Delta13C", "TotalN", "TotalC", "Sequence", "Notes"]
    colNames = ["LabId", "Sample", "Delta13C", "TotalN", "TotalC", "Notes"]
    colNamesNotMeasure = ["LabId", "ID2", "Sample", "Notes"]
    msAll = pd.DataFrame(columns = colNames + ["ID2"])

    for msFile in msFiles:
        # Check number of named columns to infer structure of xls file (JLC removed column around 2022)
        headerCheck = pd.read_excel(msFile,
            sheet_name="Summary Table",
            skiprows=10,
            nrows=1)
        if len(headerCheck.columns) == 8:
            colKeep = [0,1,3,4,5,7]

        ms = pd.read_excel(msFile, 
            sheet_name="Summary Table", 
            skiprows=10, 
            usecols=colKeep,
            names=colNames)
        ms = ms.assign(
                ID2 = ms.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
            )

        #msAll = msAll.append(ms, ignore_index = True)
        msAll = pd.concat([msAll, ms], axis = 0, ignore_index=True)

    # Assign ID2
    #msAll = msAll.assign(
    #    ID2 = msAll.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
    #)

    # Update/Delete values based on quality assurance review
    msAllQA = caf_qc.quality_assurance(msAll, dirPathToQAFile, "LabId")

    msAllQA = caf_qc.initialize_qc(msAllQA, colNamesNotMeasure)
    msAllQA = caf_qc.set_quality_assurance_applied(msAllQA,colNamesNotMeasure)

    # Split dataset into grain analysis and residue analysis
    msAllQAGrain = msAllQA[msAllQA["Sample"].str.contains("_Gr_|_MGr_|_FMGr_", na = False)]
    msAllQAResidue = msAllQA[msAllQA["Sample"].str.contains("_Res_|_MRes_|_FMRes_|_FMRes", na = False)]

    # Standardize column names
    msAllQAGrainClean = (msAllQAGrain
        .rename(columns = {
            "Delta13C": "Grain13C",
            "Delta13C_qcApplied": "Grain13C_qcApplied",
            "Delta13C_qcResult": "Grain13C_qcResult",
            "Delta13C_qcPhrase": "Grain13C_qcPhrase",
            "TotalC": "GrainCarbon",
            "TotalC_qcApplied": "GrainCarbon_qcApplied",
            "TotalC_qcResult": "GrainCarbon_qcResult",
            "TotalC_qcPhrase": "GrainCarbon_qcPhrase",
            "TotalN": "GrainNitrogen",
            "TotalN_qcApplied": "GrainNitrogen_qcApplied",
            "TotalN_qcResult": "GrainNitrogen_qcResult",
            "TotalN_qcPhrase": "GrainNitrogen_qcPhrase"
        })
        .drop(columns = ["LabId", "Sample", "Notes"]))

    msAllQAResidueClean = (msAllQAResidue
        .rename(columns = {
            "Delta13C": "Residue13C",
            "Delta13C_qcApplied": "Residue13C_qcApplied",
            "Delta13C_qcResult": "Residue13C_qcResult",
            "Delta13C_qcPhrase": "Residue13C_qcPhrase",
            "TotalC": "ResidueCarbon",
            "TotalC_qcApplied": "ResidueCarbon_qcApplied",
            "TotalC_qcResult": "ResidueCarbon_qcResult",
            "TotalC_qcPhrase": "ResidueCarbon_qcPhrase",
            "TotalN": "ResidueNitrogen",
            "TotalN_qcApplied": "ResidueNitrogen_qcApplied",
            "TotalN_qcResult": "ResidueNitrogen_qcResult",
            "TotalN_qcPhrase": "ResidueNitrogen_qcPhrase"
        })
        .drop(columns = ["LabId", "Sample", "Notes"]))

    # Deal with case that results are only residue, only grain, or both
    if len(msAllQAGrainClean) > 0 and len(msAllQAResidueClean) == 0:
        return msAllQAGrainClean.merge(msAllQAResidueClean, on = "ID2", how = "left")
    elif len(msAllQAGrainClean) == 0 and len(msAllQAResidueClean) > 0:
        return msAllQAResidueClean.merge(msAllQAGrainClean, on = "ID2", how = "left")
    else:
        return msAllQAGrainClean.merge(msAllQAResidueClean, on = "ID2", how = "left")

def calculate(df, areaHarvested):
    """Calculates grain mass at 0% moisture and 12.5% moisture, biomass dry per area, yield per area, yield per area at 0% moisture, yield per area at 12.5% moisture, and harvest index at greenhouse dried mass
    :rtype: DataFrame
    """

    result = df.copy()

    result = result.assign(
        GrainMass0 = result["GrainMassDry"].astype(float) - (result["GrainMassDry"].astype(float) * (result["GrainMoisture"].astype(float) / 100.0))
    )

    result = result.assign(
        GrainMass125 = result["GrainMass0"].astype(float) + (result["GrainMass0"].astype(float) * 0.125)
    )
    result = result.assign(
        BiomassDryPerArea = result["BiomassDry"].astype(float) / areaHarvested,
        GrainYieldDryPerArea = result["GrainMassDry"].astype(float) / areaHarvested,
        GrainYield0PerArea = result["GrainMass0"].astype(float) / areaHarvested,
        GrainYield125PerArea = result["GrainMass125"].astype(float) / areaHarvested
    )

    result = result.assign(
        HarvestIndex = result["GrainYieldDryPerArea"] / result["BiomassDryPerArea"]
    )

    return result

def process_quality_control(df, pathToParameterFiles, colsOmit = []):
    """Orchestrates all quality control checks. Parameters are specified in hardcoded file names located in the directory specified in pathToParameterFiles
    :rtype: DataFrame
    """

    dfCopy = df.copy()

    if is_there_duplicate_id2(dfCopy):
        raise Exception("Duplicated ID2 values found")
    
    if is_there_missing_id2(dfCopy, "FieldId"):
        raise Exception("Not all ID2 values have an associated row")
    
    dfCopy = caf_qc.initialize_qc(dfCopy, colsOmit)
    dfCopy = caf_qc.set_quality_assurance_applied(dfCopy)

    qcBounds = process_quality_control_point(
        dfCopy, 
        (pathToParameterFiles / "qcBounds.csv"),
        colsOmit)

    # TODO: Check data completeness

    return qcBounds

def is_there_duplicate_id2(df) -> bool:
    """Checks if there are duplicated ID2 values in the provided dataframe
    """

    num_unique = len(df["ID2"].unique())
    num_rows = len(df)

    if(num_unique != num_rows):
        return True
    else:
        return False

def is_there_missing_id2(df, fieldIdCol) -> bool:
    """Checks that all georeference points have a corresponding row (no missing data)
    """

    total_rows_expected = 619
    ce_rows_expected = 369
    cw_rows_expected = 250

    if(df.shape[0] != total_rows_expected):
        return True

    if(df[df[fieldIdCol] == 'CE'].shape[0] != ce_rows_expected):
        return True
    
    if(df[df[fieldIdCol] == 'CW'].shape[0] != cw_rows_expected):
        return True
    
    return False
    
def process_quality_control_point(df, pathToParameterFile, colsOmit = []):
    """Conducts a series of point (bounds) checks using parameters specified in pathToParameterFile. This function adds three columns for each measurement checked (_qcApplied, _qcResult, _qcPhrase).
    :rtype: DataFrame
    """
    dfCopy = df.copy()

    qcPointParameters = pd.read_csv(pathToParameterFile).dropna()

    # Get unique crops in df, filer df and params for each unique crop, merge them to new df
    cropsInData = dfCopy["Crop"].unique()

    result = pd.DataFrame()
    for crop in cropsInData:
        qcPointParamsCrop = qcPointParameters[qcPointParameters["Crop"] == crop]
        dfCrop = dfCopy[dfCopy["Crop"] == crop]

        for paramIndex, paramRow in qcPointParamsCrop.iterrows():
            dfCrop = caf_qc.process_qc_bounds_check(dfCrop, paramRow["FieldName"], paramRow["Lower"], paramRow["Upper"])

        #result = result.append(dfCrop)
        result = pd.concat([result, dfCrop], axis=0, ignore_index=True)

    return result


def to_csv(df, harvestYear, outputPath, processingLevel = None, accuracyLevel = None):
    """Outputs the data as a csv file named hy{harvestYear}.csv
    """
    # TODO: Output version with QC columns scrubbed and append _P#A# to filename
    # TODO: Add current date to output filename
    # TODO: Both of the above are probably worthy of being in CafCore
    #filePath = outputPath / ("hy" + str(harvestYear) + ".csv")
    #caf_qc.sort_qc_columns(df, True).to_csv(filePath)

    caf_io.write_data_csv(
        df.sort_values(by='ID2'),
        (outputPath),
        ("hy" + str(harvestYear)),
        processingLevel,
        accuracyLevel
    )

def standardize_cols(df):
    
    result = df.copy()

    standardCols = get_standard_col_names()

    # Create missing columns, if needed, and set QA
    for col in standardCols:
        if col not in result:
            result[col] = np.nan

    colsNonMeasure = get_standard_col_names_nonmeasure()
    result = caf_qc.initialize_qc(result, colsNonMeasure)
    result = caf_qc.set_quality_assurance_applied(result, colsNonMeasure)

    # Reorder columns, standard first, then QC ones (applied, result, then phrase)
    qcApplied = [c for c in result.columns if "_qcApplied" in c]
    qcResult = [c for c in result.columns if "_qcResult" in c]
    qcPhrase = [c for c in result.columns if "_qcPhrase" in c]

    allColsOrdered = standardCols + qcApplied + qcResult + qcPhrase

    # Double check that ordered cols is same length as original cols
    if len(allColsOrdered) != len(result.columns):
        raise Exception("Ordered column length do not match original column length")

    # Return df with new columns, ordered, and grouped
    result = caf_qc.sort_qc_columns(result[allColsOrdered], True)

    return result

def get_standard_col_names():
    cols = [
        "HarvestYear",
        "FieldId",
        "ID2",
        "SampleId",
        "Crop",
        "CropExists",
        "HarvestDate",
        "BiomassDry",
        "GrainMassDry",
        "GrainTestWeight",
        "GrainMoisture",
        "GrainProtein",
        "GrainStarch",
        "GrainGluten",
        "GrainOil",
        "GrainCarbon",
        "Grain13C",
        "GrainNitrogen",
        "ResidueCarbon",
        "Residue13C",
        "ResidueNitrogen",
        "GrainMass0",
        "GrainMass125",
        "GrainYieldDryPerArea",
        "GrainYield0PerArea",
        "GrainYield125PerArea",
        "BiomassDryPerArea",
        "HarvestIndex",
        "Comments"
    ]

    return cols

def get_standard_col_names_nonmeasure():
    cols = [
        "HarvestYear",
        "FieldId",
        "ID2",
        "SampleId",
        "Crop",
        "Comments"
    ]

    return cols

if __name__ == "__main__":
    print("Not implemented")