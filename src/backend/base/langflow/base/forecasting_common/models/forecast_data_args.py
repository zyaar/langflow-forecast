#####################################################################
# forecast_data_args.py
#
# Implements a standardized data class to pass all the arguments and information needed
# between components and static functions
#
#####################################################################

from enum import Enum
from langflow.schema.dataframe import DataFrame, Data
import json
from pathlib import Path
import pickle


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)


# COMPONENT SPECIFIC IMPORTS
# ==========================
#from datetime import datetime
#from enum import Enum
#import numpy as np
import pandas as pd



# CONSTANTS
# =========



# CLASSES
# =======

class ForecastDataArgsSchema[str, Enum]:
    FORECAST = "forecast"
    DATA = "data"
    META_DATA = "meta_data"
    TABLE_DATA = "table_data"
    TABLE_META_DATA = "table_meta_data"
    COL_GROUPS = "col_group"
    COLS = "col"
    MISC = "misc"
    



class ForecastDataArgs():
    # list of attributes
    attr_categories = ["data_", "meta_data", "table_", "col_group_", "col_"]
    attr_common = ["id", "prefix", "values"]
    attr_data_table_col_group = ["nrow", "ncol"]
    attr_col_sub_group = ["id", "prefix"]
    attr_col = ["data_type", "display_type", "value"]
    attr_misc = ["keep_granular"]

    # instance variables
    data = DataFrame()
    meta_data = ForecastMetaDataSeries()
    table = {}

    # hidden values
    _list_of_meta_data_attribs = [element.value for element in ForecastMetaDataFrameSchema]

    # __init__
    # constructor, can initialize based on values provided
    def __init__(self, **kwargs):
        setattr(self, ForecastDataArgsSchema.META_DATA, kwargs[ForecastDataArgsSchema.FORECAST].value if ForecastDataArgsSchema.META_DATA in kwargs else None)
        setattr(self, ForecastDataArgsSchema.DATA, kwargs[ForecastDataArgsSchema.DATA].value if ForecastDataArgsSchema.DATA in kwargs else None)
        setattr(self, ForecastDataArgsSchema.TABLE, kwargs[ForecastDataArgsSchema.TABLE].value if ForecastDataArgsSchema.TABLE in kwargs else None)


    # __getattr___
    # Called when attribute is not found, used to build support for getting attributes from the underlying function without having to build custom attributes
    def __getattr__(self, name):

        # check the forecast attributes
        if name in self._list_of_meta_data_attribs:
             print(f"_dispatch_forcast_attr: {name}")
             return self._dispatch_forcast_attr(name)
        
        # handle the common attributes
        # get the prefix
        match_prefix = None
        for prefix in self.attr_categories:
             if name.startwith(prefix):
                  match_prefix = prefix
                  break

        # get the suffix     
        if(match_prefix is not None):
             
             print(f"_dispatch_cat_attr: {name}")
             return self._dispatch_cat_attr(name, match_prefix)

        
             


                
        


            
        else:
            raise AttributeError(f"\n*  ForecastDataArgs:  error, unkown attribute {name}.")


    # _dispatch_forecast_attr
    def _dispatch_forcast_attr(self, name: str):
            if(getattr(self, ForecastDataArgsSchema.META_DATA) is not None):
                return self.meta_data.meta_data[name]
            else:
                raise AttributeError(f"\n*  ForecastDataArgs:  error, {ForecastDataArgsSchema.META_DATA} is not set.")



    # dispatch_cat_attr
    def dispatch_cat_attr(self, name: str, match_prefix: str):
             

        
            
    


        

