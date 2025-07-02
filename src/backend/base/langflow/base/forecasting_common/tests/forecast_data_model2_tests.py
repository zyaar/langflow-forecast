import pandas as pd
import numpy as np

from langflow.schema.dataframe import DataFrame, Data
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)


def main():

    # Create a ForecastMetaDataSeries
    print("Create a ForecastMetaDataSeries")
    print("---------------------------")
    test_series_1 = ForecastMetaDataSeries()
    print(f"{test_series_1}\n\n")



    # Add meta_data (ALL DATA)
    print("Add meta_data (ALL DATA)")
    print("------------------------")
    test_series_1.set_forecast_meta_data(id = "123",
                                         step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                         action = ForecastDataSeriesMetaDataAction.DATES,
                                         data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                         display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                         display_name = "Dates (end-of)",
                                         validation = list[ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY],
                                         pred = list["pred"],
                                         args = list["args"],
                                         objs = list["objs"],)
    print(f"{test_series_1}\n\n")


    
    # Add meta_data (REQ DATA ONLY)
    print("Add meta_data (REQ DATA ONLY, but not enforced by function)")
    print("-----------------------------------------------------------")
    test_series_2 = ForecastMetaDataSeries()
    test_series_2.set_forecast_meta_data(id = "456",
                                         step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                         action = ForecastDataSeriesMetaDataAction.INPUT,
                                         data_type = ForecastDataSeriesMetaDataDataType.INT,
                                         display_type = ForecastDataSeriesMetaDataDataType.INT,
                                         display_name = "Newly Incident Stage IV Patients",
                                         validation = list[ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK],)
    print(f"{test_series_2}\n\n")


    # Get meta_data via bulk
    print("Get meta_data via bulk")
    print("----------------------")
    bulk_meta_data = test_series_1.get_forecast_meta_data_bulk()
    print(f"{bulk_meta_data}\n\n")



    # Add meta_data via bulk
    print("Add meta_data via bulk")
    print("----------------------")
    test_series_3 = ForecastMetaDataSeries()
    test_series_3.set_forecast_meta_data_bulk(bulk_meta_data)
    print(f"{test_series_3}\n\n")



    # concat 2 ForecastMetaDataSeries and check if we get the a ForecastMetaDataFrame back
    print("concat 2 ForecastMetaDataSeries and check if we get the a ForecastMetaDataFrame back")
    print("------------------------------------------------------------------------------------")
    test_frame_1 = ForecastMetaDataFrame.concat([test_series_1, test_series_2], verify_integrity=True, ignore_dups = False)
    print(f"type test_frame_1 = {type(test_frame_1)}")    
    print(f"test_frame_1:\n{test_frame_1}\n\n")



    # concat ForecastMetaDataFrame and ForecastMetaDataSeries
    print("concat ForecastMetaDataFrame and ForecastMetaDataSeries")
    print("-------------------------------------------------------")
    test_series_3.meta_data[ForecastMetaDataSeriesSchema.ID] = "ABC"
    test_frame_2 = ForecastMetaDataFrame.concat([test_frame_1, test_series_3], verify_integrity=True, ignore_dups = True)
    print(f"type test_frame_2 = {type(test_frame_2)}")    
    print(f"test_frame_2:\n{test_frame_2}\n\n")



    # Check concat of empty ForecastMetaDataFrame and ForecastMetaDataSeries
    print("Check concat of empty ForecastMetaDataFrame and ForecastMetaDataSeries")
    print("----------------------------------------------------------------------")
    test_series_3.meta_data[ForecastMetaDataSeriesSchema.ID] = "XYZ"
    test_frame = ForecastMetaDataFrame(input_type = ForecastModelInputTypes.TIME_BASED,
                                       timescale = ForecastModelTimescale.YEAR,
                                       start_year = 2026,
                                       start_month = 1,
                                       num_periods = 3)
    test_frame_3 = ForecastMetaDataFrame.concat([test_frame, test_series_3], verify_integrity=True, ignore_dups = False)
    print(f"type test_frame_3 = {type(test_frame_3)}")    
    print(f"test_frame_3:\n{test_frame_3}\n\n")



    print("concat ForecastDataframe and ForecastDataframe and confirm the meta-data is all gone")
    print("------------------------------------------------------------------------------------")

    test_frame_2.meta_data[ForecastMetaDataFrameSchema.INPUT_TYPE] = ForecastModelInputTypes.TIME_BASED
    test_frame_2.meta_data[ForecastMetaDataFrameSchema.TIMESCALE] = ForecastModelTimescale.YEAR
    test_frame_2.meta_data[ForecastMetaDataFrameSchema.START_YEAR] = 2026
    test_frame_2.meta_data[ForecastMetaDataFrameSchema.START_MONTH] = 1
    test_frame_2.meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS] = 3
    test_frame_4 = ForecastMetaDataFrame.concat([test_frame_2, test_frame_3], verify_integrity=True, ignore_dups = False)
    print(f"type test_frame_4 = {type(test_frame_4)}")
    print(f"test_frame_4:\n{test_frame_4}\n\n")
















if __name__ == "__main__":
    main()
