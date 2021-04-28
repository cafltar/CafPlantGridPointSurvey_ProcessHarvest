import common
import pathlib
import pandas as pd

def main():
    colNotMeasure = ["HarvestYear", "ID2", "Crop", "SampleId", "Comments"]
    hy = 2017
    harvestedArea = 2.4384

    print("Processing " + str(hy))
    inputPath = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\input")
    outputPath = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\output")
    path = inputPath / "HarvestYear" / "HY2017"
    
    harvest = common.read_transform_hand_harvest_2017(
        (path / "HandHarvest" / "LTAR_CAF_HY2017_CropBiomass-10-31-2017_IL_20191209.xlsx"),
        (path / "qaChangeFile_HandHarvest.csv"),
        hy)

    hy2017EA = common.read_transform_ea(
        (path / "EA"),
        (path / "qaChangeFile_EA.csv"),
        hy)

    df = harvest.merge(hy2017EA, on = "ID2", how = "left")

    # Calc values, create qc cols, assign assurance check passed for all values
    df_calc = common.calculate(df, harvestedArea)
    df_qc = common.process_quality_control(
        df_calc, 
        inputPath,
        colNotMeasure)

    common.to_csv(df_qc, hy, outputPath)

    print("done")

if __name__ == "__main__":
    main()
