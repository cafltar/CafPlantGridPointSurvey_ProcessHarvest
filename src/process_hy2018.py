import common
import pathlib
import pandas as pd

def main():
    colNotMeasure = ["HarvestYear", "ID2", "Crop", "SampleId", "Comments"]
    hy = 2018
    harvestedArea = 2.4384

    print("Processing " + str(hy))
    inputPath = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\input")
    outputPath = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\output")
    path = inputPath / "HarvestYear" / "HY2018"
    
    harvest = common.read_transform_hand_harvest_2018(
        (path / "HandHarvest" / "LTARcafHarSamp2018HYBioGrainMasses10242018_IL_20191209.xlsx"),
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

    common.to_csv(df_qc, hy, outputPath)

    print("done")

if __name__ == "__main__":
    main()