#requires -PSEdition Core

echo "Starting Workflow"

python.exe .\src\process_hy2017.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "LTAR_CAF_HY2017_CropBiomass-10-31-2017_IL_20191209.xlsx"
python.exe .\src\process_hy2018.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "LTARcafHarSamp2018HYBioGrainMasses10242018_IL_20191209.xlsx"
python.exe .\src\process_hy2019.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "Harvest01_2019_GP-ART-Lime_INT__20191106_IL_20191209.xlsm"
python.exe .\src\process_hy2020.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "Harvest01_2020_GP-ART-Lime_INT_YYYYMMDD.xlsm"
python.exe .\src\process_hy2021.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "Harvest01_2021_GP-ART-Lime_Archive2022-04-13T2305240842351Z.xlsm"
#python.exe .\src\process_hy2022.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data" "Harvest01_2022_GP-ART-Lime_INI_YYYYMMDD.xlsm"

python.exe .\src\aggregate_simplify.py "C:\Dev\Projects\CafPlantGridPointSurvey\ProcessHarvest\data"

echo "Finished Worfklow"