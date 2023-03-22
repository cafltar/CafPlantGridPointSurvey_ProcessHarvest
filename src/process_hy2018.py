import common
import pathlib
import pandas as pd
import sys

def main(dataPath, harvestDetFilename):
    colNotMeasure = ["HarvestYear", "FieldId", "ID2", "Crop", "SampleId", "HarvestDate", "Comments"]
    hy = 2018
    harvestedArea = 2.4384

    print("Processing " + str(hy))
    inputPath = pathlib.Path(dataPath, "input")
    outputPath = pathlib.Path(dataPath, "output")
    path = inputPath / "HarvestYear" / "HY2018"
    
    harvest = common.read_transform_hand_harvest_2018(
        (path / "HandHarvest" / harvestDetFilename),
        (path / "qaChangeFile_HandHarvest.csv"),
        hy)

    ms = common.read_transform_ms(
        (path / "MS"),
        (path / "qaChangeFile_MS.csv"),
        hy)

    nir = common.read_transform_nir(
        (path / "NIR"), 
        (path) / "qaChangeFile_NIR.csv", 
        hy)

    df = harvest.merge(ms, on = "ID2", how = "left").merge(nir, on = "ID2", how = "left")

    # Calc values, create qc cols, assign assurance check passed for all values
    df_calc = common.calculate(df, harvestedArea)
    df_qc = common.process_quality_control(
        df_calc, 
        inputPath,
        colNotMeasure)

    df_final = common.standardize_cols(df_qc)

    common.to_csv(df_final, hy, outputPath, 2, 2)

    print("done")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise Exception("Must specify path to datasets and name of harvest DET")
    
    dataPath = pathlib.Path(sys.argv[1])
    harvestDetFilename = sys.argv[2]

    main(dataPath, harvestDetFilename)