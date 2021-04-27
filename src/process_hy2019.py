import common
import pathlib
import pandas as pd

def main():
    colNotMeasure = ["HarvestYear", "ID2", "Crop", "SampleId", "Comments"]
    hy = 2019
    harvestedArea = 2.4384

    print("===2019")
    path = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\input\\HarvestYear\\HY2019")
    
    harvest = common.read_transform_harvest01Det(
        (path / "HandHarvest" / "Harvest01_2019_GP-ART-Lime_INT__20191106_IL_20191209.xlsm"),
        (path / "qaChangeFile_HandHarvest.csv"),
        hy)

    #ms = common.read_transform_ms(
    #    (path / "MS"),
    #    (path / "qaChangeFile_MS.csv"),
    #    hy)

    nir = common.read_transform_nir(
        (path / "NIR"), 
        (path) / "qaChangeFile_NIR.csv", 
        hy)

    df = harvest.merge(nir, on = "ID2", how = "left")

    # Calc values, create qc cols, assign assurance check passed for all values
    df_calc = common.calculate(df, harvestedArea)
    df_qc = common.process_quality_control(df_calc, colNotMeasure)

    df_qc.to_csv("foo2019.csv")

    print("done")

if __name__ == "__main__":
    main()