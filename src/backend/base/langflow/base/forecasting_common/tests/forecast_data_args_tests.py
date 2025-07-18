from langflow.base.forecasting_common.models.forecast_data_args import ForecastDataArgsSchema, ForecastDataArgs
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)


data_args = ForecastDataArgs()

print(data_args.data)
print(data_args.meta_data)
print(data_args.table)
print()

data_args.data = "data"
data_args.meta_data = "meta_data"
data_args.table = "table"

print(data_args.data)
print(data_args.meta_data)
print(data_args.table)

# attach ForecastMetaDataFrame
test = ForecastMetaDataFrame()
print(test.meta_data["input_type"])
data_args.meta_data = ForecastMetaDataFrame()

# test meta_data forecast attributes
print(data_args.input_type)



