import pandas as pd
import numpy as np
import pathlib
import glob

import cafcore.qc
import cafcore.file_io

def parse_id2_from_sampleId(sampleId, harvestYear):
    """Extracts the embedded ID2 value from the given sampleId. Expects sampleId to be in format similar to "CE39_Bio_GB_2018_22-B", split by "_"
    rtype: int
    """

    if not isinstance(sampleId, str):
        return None

    id2 = 0
    if(harvestYear >= 2017):
        id2 = int(sampleId.upper().split("_")[0].replace("CE", "").replace("CW", ""))
    else:
        id2 = None

    return id2

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
            HarvestYear = 2017,
            #ID2 = harvestDetRaw["Total Biomass Barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            ID2 = harvest.apply(lambda row: parse_id2_from_sampleId(row["Total Biomass Barcode ID"], 2017), axis=1),
            SampleId = harvest["Total Biomass Barcode ID"],
            Crop = harvest["Total Biomass Barcode ID"].str.split("_", expand = True)[2],
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
        "ID2",
        "SampleId",
        "Crop",
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

    colNotMeasure = ["HarvestYear", "ID2", "SampleId", "Comments"]

    # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore.qc.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore.qc.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        "SampleId")
    harvestStandardCleanQA = cafcore.qc.set_quality_assurance_applied(
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

    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            #ID2 = harvestDetRaw["total biomass bag barcode ID"].str.split("_", expand = True)[0].str.replace("CE", "").str.replace("CW", ""),
            ID2 = harvest.apply(lambda row: parse_id2_from_sampleId(row["total biomass bag barcode ID"], harvestYear), axis=1),
            SampleId = harvest["total biomass bag barcode ID"],
            Crop = harvest["total biomass bag barcode ID"].str.split("_", expand = True)[2],
            BiomassDry = harvest["dried total biomass mass + bag + residue bag + grain bag (g)"] - harvest["average dried empty total biomass bag +  grain bag + residue bag  (g)"],
            GrainMassDry = harvest["non-oven dried grain mass + bag (g)"] - harvest["average empty dried grain bag mass (g)"],
            CropExists = 1,
            Comments = harvest["notes"].astype(str) + "| " + harvest["Notes by Ian Leslie 10/22/2019"],
            AreaOfInterestID2 = harvest["total biomass bag barcode ID"].str.split("_", expand = True)[0]            
        )
        .query("(AreaOfInterestID2.str.contains('CW') | AreaOfInterestID2.str.contains('CE'))")
    )

    colNames = [
        "HarvestYear",
        "ID2",
        "SampleId",
        "Crop",
        "BiomassDry",
        "GrainMassDry",
        "CropExists",
        "Comments"]

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = ["HarvestYear", "ID2", "SampleId", "Comments"]

    # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore.qc.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore.qc.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        "SampleId")
    harvestStandardCleanQA = cafcore.qc.set_quality_assurance_applied(
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
    ).query("`Project ID` == 'GP' & (`Total biomass bag barcode ID`.str.contains('CE') | `Total biomass bag barcode ID`.str.contains('CW'))")

    testWtLargeCol = "Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  "
    testWtSmallCol = "Manual Test Weight: Small Kettle\n(lbs / bushel)\n(Manual, small container if no # in large container column)"

    harvestStandard = (
        harvest.assign(
            HarvestYear = harvestYear,
            ID2 = harvest.apply(lambda row: parse_id2_from_sampleId(row["Total biomass bag barcode ID"], harvestYear), axis=1),
            SampleId = harvest["Total biomass bag barcode ID"],
            Crop = harvest["Total biomass bag barcode ID"].str.split("_", expand = True)[3],
            BiomassDry = harvest["Dried total biomass (g)"],
            GrainMassDry = harvest["Non-oven-dried grain (g)"],
            GrainTestWeight = harvest.apply(lambda row: row[testWtLargeCol] * 0.0705 if row[testWtLargeCol] else row[testWtSmallCol], axis=1),
            #GrainTestWeight = harvest["Manual Test Weight: Large Kettle\n(large container, converted value in small container column) (grams) Conversion to lbs per Bu = 0.0705.  "] * 0.0705,
            CropExists = 1,
            Comments = harvest["Notes"].astype(str) + "| " + harvest["Notes made by Ian Leslie"],
            ProjectID = harvest["Project ID"])
    )

    colNames = [
        "HarvestYear",
        "ID2",
        "SampleId",
        "Crop",
        "BiomassDry",
        "GrainMassDry",
        "GrainTestWeight",
        "CropExists",
        "Comments"]

    harvestStandardClean = harvestStandard[colNames]

    colNotMeasure = ["HarvestYear", "ID2", "SampleId", "Comments"]

     # Update/Delete values based on quality assurance review
    harvestStandardCleanQA = cafcore.qc.initialize_qc(
        harvestStandardClean, 
        colNotMeasure)
    harvestStandardCleanQA = cafcore.qc.quality_assurance(
        harvestStandardCleanQA, 
        dirPathToQAFile,
        "SampleId")
    harvestStandardCleanQA = cafcore.qc.set_quality_assurance_applied(
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
        nirFilter = nir.query("(Sample_ID.str.upper().str.contains('GP')) & ((Sample_ID.str.upper().str.contains('CW')) | (Sample_ID.str.upper().str.contains('CE')))")
        nirParse = (nirFilter
            .assign(
                ID2 = nirFilter.apply(lambda row: parse_id2_from_nir_sample_id(row["Sample_ID"], harvestYear), axis=1)
            )
        )

        nirs = nirs.append(nirParse, ignore_index = True)

    nirs = nirs[colNames]

    nirsQA = cafcore.qc.initialize_qc(nirs, colNamesNotMeasure)
    nirsQA = cafcore.qc.quality_assurance(nirsQA, dirPathToQAFile, "Date_Time")
    nirsQA = cafcore.qc.set_quality_assurance_applied(nirsQA, colNamesNotMeasure)

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

        eaAll = eaAll.append(ea, ignore_index = True)

    # Assign ID2
    #eaAll = eaAll.assign(
    #    ID2 = eaAll.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
    #)

    # Update/Delete values based on quality assurance review
    eaAllQA = cafcore.qc.quality_assurance(eaAll, dirPathToQAFile, "LabId")

    eaAllQA = cafcore.qc.initialize_qc(eaAllQA, colNamesNotMeasure)
    eaAllQA = cafcore.qc.set_quality_assurance_applied(eaAllQA,colNamesNotMeasure)

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
        ms = pd.read_excel(msFile, 
            sheet_name="Summary Table", 
            skiprows=10, 
            usecols=colKeep,
            names=colNames)
        ms = ms.assign(
                ID2 = ms.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
            )

        msAll = msAll.append(ms, ignore_index = True)

    # Assign ID2
    #msAll = msAll.assign(
    #    ID2 = msAll.apply(lambda row: parse_id2_from_sampleId(row["Sample"], harvestYear), axis = 1)
    #)

    # Update/Delete values based on quality assurance review
    msAllQA = cafcore.qc.quality_assurance(msAll, dirPathToQAFile, "LabId")

    msAllQA = cafcore.qc.initialize_qc(msAllQA, colNamesNotMeasure)
    msAllQA = cafcore.qc.set_quality_assurance_applied(msAllQA,colNamesNotMeasure)

    # Split dataset into grain analysis and residue analysis
    msAllQAGrain = msAllQA[msAllQA["Sample"].str.contains("_Gr_|_MGr_|_FMGr_", na = False)]
    msAllQAResidue = msAllQA[msAllQA["Sample"].str.contains("_Res_|_MRes_|_FMRes_", na = False)]

    # Standardize column names
    msAllQAGrainClean = (msAllQAGrain
        .rename(columns = {
            "Delta13C": "Graind13C",
            "Delta13C_qcApplied": "Graind13C_qcApplied",
            "Delta13C_qcResult": "Graind13C_qcResult",
            "Delta13C_qcPhrase": "Graind13C_qcPhrase",
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
            "Delta13C": "Residued13C",
            "Delta13C_qcApplied": "Residued13C_qcApplied",
            "Delta13C_qcResult": "Residued13C_qcResult",
            "Delta13C_qcPhrase": "Residued13C_qcPhrase",
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
        GrainMass0 = result["GrainMassDry"] - (result["GrainMassDry"] * (result["GrainMoisture"] / 100.0))
    )

    result = result.assign(
        GrainMass125 = result["GrainMass0"] + (result["GrainMass0"] * 0.125)
    )
    result = result.assign(
        BiomassDryPerArea = result["BiomassDry"] / areaHarvested,
        GrainYieldDryPerArea = result["GrainMassDry"] / areaHarvested,
        GrainMass0PerArea = result["GrainMass0"] / areaHarvested,
        GrainMass125PerArea = result["GrainMass125"] / areaHarvested
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

    dfCopy = cafcore.qc.initialize_qc(dfCopy, colsOmit)
    dfCopy = cafcore.qc.set_quality_assurance_applied(dfCopy)

    if is_there_duplicate_id2(dfCopy):
        raise Exception("Duplicated ID2 values found")

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
            dfCrop = cafcore.qc.process_qc_bounds_check(dfCrop, paramRow["FieldName"], paramRow["Lower"], paramRow["Upper"])

        result = result.append(dfCrop)

    return result


def to_csv(df, harvestYear, outputPath):
    """Outputs the data as a csv file named hy{harvestYear}.csv
    """
    # TODO: Output version with QC columns scrubbed and append _P#A# to filename
    # TODO: Add current date to output filename
    # TODO: Both of the above are probably worthy of being in CafCore
    #filePath = outputPath / ("hy" + str(harvestYear) + ".csv")
    #cafcore.qc.sort_qc_columns(df, True).to_csv(filePath)

    cafcore.file_io.write_data_csv(
        cafcore.qc.sort_qc_columns(df, True),
        (outputPath),
        ("hy" + str(harvestYear))
    )


if __name__ == "__main__":
    print("Not implemented")