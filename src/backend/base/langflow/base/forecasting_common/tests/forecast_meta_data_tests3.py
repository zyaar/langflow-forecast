import pandas as pd
import numpy as np

from langflow.schema.dataframe import DataFrame, Data
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame,
                                                                        ForecastMetaDataAction,
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastMetaDataActionSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)


def main():
    # class ForecastMetaDataActionSchema(str, Enum):
    #     ACTION = "action"
    #     PRED = "pred" # predecessors, a set of column ids necessary for the action
    #     ARGS = "args" # any additional values necessary for actions, or validations
    #     OBJS = "objs" # any additional objects which are required for this step


    # Create a ForecastMetaDataSeries
    print("Create a ForecastMetaDataAction")
    print("---------------------------")
    test_action_1 = ForecastMetaDataAction(action = ForecastDataSeriesMetaDataAction.PROD,
                                           count = 2,
                                           pred = ["ABC_ID", "XYZ_ID"])
    print(f"{test_action_1}\n\n")
    print(f"{test_action_1.to_json()}\n\n")


    # Create a ForecastMetaDataSeries with meta_data through __init__
    print("Create a ForecastMetaDataSeries with one action")
    print("-----------------------------------------------")
    test_series_1000 = ForecastMetaDataSeries(id = "123",
                                              step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                              action = test_action_1,
                                              data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                              display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                              display_name = "Dates (end-of)",
                                              validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                              pred = ["pred"],
                                              args = ["args"],
                                              objs = ["objs"],)
    print(f"\n\n{test_series_1000}\n\n")
    print(f"{test_series_1000.to_json()}\n\n")


    # Create a ForecastMetaDataSeries with meta_data through __init__
    print("Create a ForecastMetaDataSeries with list of actions")
    print("---------------------------------------------------------------")
    test_action_2 = ForecastMetaDataAction(action = ForecastDataSeriesMetaDataAction.PROD,
                                           count = 3,
                                           pred = ["123_ID", "456_ID"])


    test_action_3 = ForecastMetaDataAction(action = ForecastDataSeriesMetaDataAction.PROD,
                                           count = None,
                                           pred = ["123_ID", "456_ID"])




    test_series_1000 = ForecastMetaDataSeries(id = "123",
                                              step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                              action = [test_action_1, test_action_2, test_action_3],
                                              data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                              display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                              display_name = "Dates (end-of)",
                                              validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                              pred = ["pred"],
                                              args = ["args"],
                                              objs = ["objs"],)
    print(f"\n\n{test_series_1000}\n\n")
    print(f"{test_series_1000.to_json()}\n\n")











if __name__ == "__main__":
    main()
