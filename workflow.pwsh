#requires -PSEdition Core

echo "Starting Workflow"

python.exe .\src\process_hy2017.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "LTAR_CAF_HY2017_CropBiomass-10-31-2017_IL_20191209.xlsx"
python.exe .\src\process_hy2018.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "LTARcafHarSamp2018HYBioGrainMasses10242018_IL_20191209.xlsx"
python.exe .\src\process_hy2019.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "Harvest01_2019_GP-ART-Lime_INT__20191106_IL_20191209.xlsm"

echo "Finished Worfklow"