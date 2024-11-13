import pandas as pd
import numpy as np
import pathlib
import glob
import datetime

#https://github.com/cafltar/cafcore/releases/tag/v0.1.3
import cafcore.qc
import cafcore.file_io

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

def parse_id2_from_sampleId(sampleId, harvestYear):
    """Extracts the embedded ID2 value from the given sampleId. Expects sampleId to be in format similar to "CE39_Bio_GB_2018_22-B", split by "_"
    Returns None if harvest year is less than 2017 or sample ID does not start with "CE" or "CW"
    rtype: int
    """

    # ensures passed type is a string
    if not isinstance(sampleId, str):
        return None

    # ensures sampleId is for Cook field (starts with either "CE" or "CW")
    if not sampleId.upper().startswith(tuple(["CE", "CW"])):
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

def parse_harvest01_biomass(row, bag1Col, bag2Col, massBagsCol, massBag2Col):
    '''Calculates a air dried biomass value from the harvest01 DET by factoring in a potential second biomass bag.
    '''

    bag1Biomass = row[bag1Col] # Mass of biomass + biomass bag + residue bag + grain bag
    bag2Biomass = row[bag2Col] # Mass of biomass + 2nd biomass bag
    bagsMass = row[massBagsCol] # Mass of biomass bag + residue bag + grain bag
    bag2Mass = row[massBag2Col] # Mass of 2nd biomass bag

    result = None

    if(pd.isna(bag2Biomass)):
        if((not pd.isna(bag1Biomass)) & (not pd.isna(bagsMass))):
            result = float(bag1Biomass) - float(bagsMass)
    else:
        if((not pd.isna(bag1Biomass)) & (not pd.isna(bagsMass)) & (not pd.isna(bag2Mass))):
            result = (float(bag1Biomass) - float(bagsMass)) + (float(bag2Biomass) - float(bag2Mass))
    
    return result
