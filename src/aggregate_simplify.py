import common
import pathlib
import pandas as pd
import sys
import glob
import numpy as np

def main(dataPath):
    print("Aggregating data")
    # Set out directory
    outputPath = pathlib.Path(dataPath, "output")

    # glob all *_QCCodes_* files
    fileNameTemplate = outputPath / "*_QCCodes_*.csv"
    dataFiles = glob.glob(str(fileNameTemplate))

    # Concat all globed files into single aggregated dataset
    hys = pd.DataFrame()
    for dataFile in dataFiles:
        hy = pd.read_csv(dataFile)

        hys = pd.concat([hy, hys], axis = 0, ignore_index = True)

    # Reduce QC columns into a single QC column (flag or no flag)
    # TODO: Consider moving this to cafcore.qc
    #   Get list of qcResult cols
    qcResultCols = [col for col in hys.columns if "_qcResult" in col]
    qcAppliedCols = [col for col in hys.columns if "_qcApplied" in col]
    qcPhraseCols = [col for col in hys.columns if "_qcPhrase" in col]

    #   Sum cols, check if greater than 1, if so, set to 1
    hys["QualityControlFlag"] = np.where(hys[qcResultCols].sum(axis=1) > 1, 1, 0)

    #   Remove qc cols
    qcCols = qcAppliedCols + qcResultCols + qcPhraseCols
    hys = hys.drop(columns = qcCols)

    # Write output: Name with start year and end year (derived from the file names?) and lowest P and A levels
    startYear = hys["HarvestYear"].min()
    endYear = hys["HarvestYear"].max()


    common.to_csv_agg(hys, startYear, endYear, outputPath, 2, 0)

    print("done")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Must specify path to datasets")

    dataPath = pathlib.Path(sys.argv[1])

    main(dataPath)
