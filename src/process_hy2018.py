import common
import pathlib
import pandas as pd

def main():
    colNotMeasure = ["HarvestYear", "ID2", "Crop", "SampleId", "Comments"]
    hy = 2018
    harvestedArea = 2.4384

    print("===2018")
    hy2018Path = pathlib.Path("C:\\Dev\\Projects\\CafPlantGridPointSurvey\\ProcessHarvest\\data\\input\\HarvestYear\\HY2018")
    
    hy2018Harvest = common.read_transform_hand_harvest_2018(
        (hy2018Path / "HandHarvest" / "LTARcafHarSamp2018HYBioGrainMasses10242018_IL_20191209.xlsx"),
        (hy2018Path / "qaChangeFile_HandHarvest.csv"),
        hy)

    hy2018MS = common.read_transform_ms(
        (hy2018Path / "MS"),
        (hy2018Path / "qaChangeFile_MS.csv"),
        hy)

    hy2018Nir = common.read_transform_nir(
        (hy2018Path / "NIR"), 
        (hy2018Path) / "qaChangeFile_NIR.csv", 
        hy)

    hy2018 = hy2018Harvest.merge(hy2018MS, on = "ID2", how = "left").merge(hy2018Nir, on = "ID2", how = "left")

    # Calc values, create qc cols, assign assurance check passed for all values
    hy2018_calc = common.calculate(hy2018, harvestedArea)
    hy2018_qc = common.process_quality_control(hy2018_calc, colNotMeasure)

    hy2018_qc.to_csv("foo2018.csv")

    print("done")

if __name__ == "__main__":
    main()