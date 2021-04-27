import common
import pathlib
import pandas as pd

def main():
    colNotMeasure = ["HarvestYear", "ID2", "Crop", "SampleId", "Comments"]
    hy = 2017
    harvestedArea = 2.4384

    print("===2017")
    hy2017Path = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\input\\HarvestYear\\HY2017")
    
    hy2017Harvest = common.read_transform_hand_harvest_2017(
        (hy2017Path / "HandHarvest" / "LTAR_CAF_HY2017_CropBiomass-10-31-2017_IL_20191209.xlsx"),
        (hy2017Path / "qaChangeFile_HandHarvest.csv"),
        hy)

    hy2017EA = common.read_transform_ea(
        (hy2017Path / "EA"),
        (hy2017Path / "qaChangeFile_EA.csv"),
        hy)

    hy2017 = hy2017Harvest.merge(hy2017EA, on = "ID2", how = "left")

    # Calc values, create qc cols, assign assurance check passed for all values
    hy2017_calc = common.calculate(hy2017, harvestedArea)
    hy2018_qc = common.process_quality_control(hy2017_calc, colNotMeasure)

    hy2018_qc.to_csv("foo2017.csv")

    print("done")

if __name__ == "__main__":
    main()
