import pandas as pd
import numpy as np

from langflow.schema.dataframe import DataFrame, Data
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastDataSeries, 
                                                                          ForecastDataFrame, 
                                                                          ForecastDataSeriesMetaDataSchema, 
                                                                          ForecastDataSeriesMetaDataStepTypes, 
                                                                          ForecastDataSeriesMetaDataAction, 
                                                                          ForecastDataSeriesMetaDataDataType, 
                                                                          ForecastDataSeriesMetaDataValidationSchema, 
                                                                          ForecastDataSeriesMetaDataValidateInputRestrictions)


def main():

    # Create a ForecastDataSeries
    print("Create a ForecastDataSeries")
    print("---------------------------")
    test_series_1 = ForecastDataSeries([10,20,30,40,50])
    print(test_series_1)



    # Add meta_data (ALL DATA)
    print("Add meta_data (ALL DATA)")
    print("------------------------")
    test_series_1.set_forecast_meta_data(step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                         action = ForecastDataSeriesMetaDataAction.DATES,
                                         data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                         display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                         display_name = "Dates (end-of)",
                                         validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                         pred = ["pred"],
                                         args = ["args"],
                                         objs = ["objs"],)
    print(test_series_1)


    
    # Add meta_data (REQ DATA ONLY)
    print("Add meta_data (REQ DATA ONLY)")
    print("-----------------------------")
    test_series_2 = ForecastDataSeries([1,2,3,4,5])
    test_series_2.set_forecast_meta_data(step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                         action = ForecastDataSeriesMetaDataAction.INPUT,
                                         data_type = ForecastDataSeriesMetaDataDataType.INT,
                                         display_type = ForecastDataSeriesMetaDataDataType.INT,
                                         display_name = "Newly Incident Stage IV Patients",
                                         validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}],)
    print(test_series_2)



    # Get meta_data via bulk
    print("Get meta_data via bulk")
    print("----------------------")
    bulk_meta_data = test_series_1.get_forecast_meta_data_bulk()
    print(bulk_meta_data)
    print()



    # Add meta_data via bulk
    print("Add meta_data via bulk")
    print("----------------------")
    test_series_3 = ForecastDataSeries([5,6,7,8,9])
    test_series_3.set_forecast_meta_data_bulk(bulk_meta_data)
    print(test_series_3)



    # concat 2 ForecastDataSeries and check if we get the right type of DataFrame back (and confirm meta-data is lost)
    print("concat 2 ForecastDataSeries and check if we get the right type of DataFrame back (and confirm meta-data is lost)")
    print("----------------------------------------------------------------------------------------------------------------")
    test_frame_1 = pd.concat([test_series_1, test_series_2], axis=1, verify_integrity=True)
    print(f"type test_frame_1 = {type(test_frame_1)}")
    print(f"type test_frame_1[0] = {type(test_frame_1[0])}")
    print(f"type test_frame_1[1] = {type(test_frame_1[1])}\n")
    
    print(f"test_frame_1:\n{test_frame_1}\n")



    # add meta-data back into to the ForecastDataframe
    print("add meta-data back into to the ForecastDataframe")
    print("------------------------------------------------")
    all_meta_data = []
    all_meta_data.append(test_series_1.get_forecast_meta_data_bulk())
    all_meta_data.append(test_series_2.get_forecast_meta_data_bulk())
    test_frame_1.set_all_col_meta_data(all_meta_data_attribs = all_meta_data)

    print(f"test_frame_1:\n{test_frame_1}\n")



    # concat ForecastDataframe and ForecastDataSeries and confirm the meta-data is all gone
    print("concat ForecastDataframe and ForecastDataSeries and confirm the meta-data is all gone")
    print("-------------------------------------------------------------------------------------")
    test_series_3.name = 2
    test_frame_2 = pd.concat([test_frame_1, test_series_3], axis=1, verify_integrity=True)
    print(f"type test_frame_2 = {type(test_frame_2)}")    
    print(f"test_frame_2:\n{test_frame_2}\n")

    

    # add meta-data back into to the ForecastDataframe
    print("add meta-data back into to the ForecastDataframe")
    print("------------------------------------------------")
    all_meta_data_2 = test_frame_1.get_all_col_meta_data()
    all_meta_data_2.append(test_series_3.get_forecast_meta_data_bulk())
    test_frame_2.set_all_col_meta_data(all_meta_data_attribs = all_meta_data_2)

    print(f"test_frame_2:\n{test_frame_2}\n")



    # concat ForecastDataframe and ForecastDataframe and confirm the meta-data is all gone
    print("concat ForecastDataframe and ForecastDataframe and confirm the meta-data is all gone")
    print("------------------------------------------------------------------------------------")
    temp_meta_data = test_frame_2.get_all_col_meta_data()
    test_frame_2.columns = [2, 3, 4]
    test_frame_2.set_all_col_meta_data(temp_meta_data)
    test_frame_3 = pd.concat([test_frame_1, test_frame_2], axis=1, verify_integrity=True)
    print(f"type test_frame_3 = {type(test_frame_3)}")    
    print(f"test_frame_3:\n{test_frame_3}\n")



    # add meta-data back into to the ForecastDataframe
    print("add meta-data back into to the ForecastDataframe")
    print("------------------------------------------------")
    all_meta_data_3 = test_frame_1.get_all_col_meta_data()
    all_meta_data_3 = all_meta_data_3 + test_frame_2.get_all_col_meta_data()
    test_frame_3.set_all_col_meta_data(all_meta_data_attribs = all_meta_data_3)

    print(f"test_frame_3:\n{test_frame_3}\n")


    # convert to Langflow DataFrame and check the meta-data (if possible)
    print("convert to Langflow DataFrame and check the meta-data (if possible)")
    print("-------------------------------------------------------------------")
    all_meta_data_4 = test_frame_3.get_all_col_meta_data()
    test_frame_4 = DataFrame(data = test_frame_3)

    print(test_frame_4)















if __name__ == "__main__":
    main()
