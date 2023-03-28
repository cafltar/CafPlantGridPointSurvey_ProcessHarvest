# Files

## 2021-02-16

* [HarvestYear](HarvestYear)
  * Raw data generated from samples representing the given harvest year (HY) and associated quality assurance (QA) and quality control (QC) files
* [cookeast_georeferencepoint_20190924.geojson](cookeast_georeferencepoint_20190924.geojson)
  * From Github repo: CafGeospatialBoundariesAndGridPoints\CleanAndStandardize\output
* [cookwest_georeferencepoint_20190924.geojson](cookwest_georeferencepoint_20190924.geojson)
  * From Github repo: CafGeospatialBoundariesAndGridPoints\CleanAndStandardize\output

## 2022-10-24

* [qcBounds.csv](qcBounds.csv)
  * This file was added in 2022-02-07 but not documented. I believe it was adapted from the file on the Google Shared Drive: `CafPlantGridPointSurvey\2021\Working\DetermineVariableBounds`
  * Today I (Bryan Carlson) updated the file to include upper and lower bounds as calculated using extreme outlier values (3*IQR), as calculated by Eddie Steiner, using 1999-2015 values. See the Google Shared Drive folder for more information: `G:\Shared drives\CafPlantGridPointSurvey\2022\Working\DetermineVariableBounds`

## 2023-03-28

- [normAndSpatialResultFlags.csv](normAndSpatialResultFlags.csv)
  - Results of a dataset check that Joaquin Casanova did on 1999-2019 data
  - Copied from email from Joaquin Casanova to Bryan Carlson on 12/22/2022 with subject "RE: Finalizing harvest data at Cook"
  - "I went through all harvest years and normalized all variables by year and crop type (meaning scaled and zero-centered, ie, converted to z-scores). Then I flagged points for each variable (yield, protein, etc) if they were more than 3 standard deviations. Then I went through all points, removed one at a time, interpolated to fill it, then compared the measured to the filled value. If that difference was greater than 3 standard deviations, I assigned a SpatialFlag. Then I plotted the suspect points and wrote them to file.
  - Bryan renamed the original file from "flagged.csv", deleted columns "ResidueMassNormFlag" and "ResidueMassNormSpatialFlag" (new data now is Biomass and stat results don't apply here), renamed "GrainYieldNorm" -> "GrainYieldDryPerAreaNorm" and "GrainYieldSpatialNorm" -> "GrainYieldDryPerAreaSpatialNorm"
 