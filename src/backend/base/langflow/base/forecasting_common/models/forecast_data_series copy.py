#####################################################################
# forecast_data_series.py
#
# Implements a class which overrides Pandas Data Series to include support
# for forecast specific meta-data
#
#####################################################################

#from typing import List, Tuple
#import nanoid
#from langflow.schema.dataframe import DataFrame, Data

#from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
#from langflow.base.forecasting_common.models.date_utils import gen_dates, conv_dates_monthly_to_yearly, conv_dates_yearly_to_monthly


# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
#from datetime import datetime
from enum import Enum
import numpy as np
import pandas as pd



# CONSTANTS
# =========



# CLASSES
# =======

# ForecastDataSeriesMetaDataSchema, ForecastDataSeriesMetaDataStepTypes, ForecastDataSeriesMetaDataAction, ForecastDataSeriesMetaDataDataType, ForecastDataSeriesMetaDataValidationSchema, ForecastDataSeriesMetaDataValidateInputRestrictions

# Enum holding the schema of the meta-data model
# The different meta-data attributes stores for each pandas data series (i.e. each column) in the forecast model
class ForecastDataSeriesMetaDataSchema(str, Enum):
    STEP_TYPE = "step_type"
    ACTION = "action"
    DATA_TYPE = "data_type"
    DISPLAY_TYPE = "display_type"
    DISPLAY_NAME = "display_name"
    VALIDATION = "validation"
    PRED = "pred"
    ARGS = "args"
    OBJS = "objs"


# Enum of STEP_TYPE
# What are the different steps in the forecast process
class ForecastDataSeriesMetaDataStepTypes(str, Enum):
    EPIDEMIOLOGY = "epidemiology"
    POPULATION_CUT = "population_cut"
    PRICING = "pricing"
    SEGMENT = "segment"
    SUMMATION = "summation"
    TREATMENT = "treatment"


# Enum of ACTION
# Within in forecast step, what different actions are taken
class ForecastDataSeriesMetaDataAction(str, Enum):
    DATES = "dates"
    INPUT = "input"
    SUM = "sum"
    PROD = "prod"


# Enum of data types (used by:  DATA_TYPE and DISPLAY_TYPE)
# Within in forecast step, what different actions are taken
class ForecastDataSeriesMetaDataDataType(str, Enum):
    DATE = "date"
    INT = "int"
    FLOAT = "float"
    PCT = "percent"
    CURRENCY = "currency"




# Enum of VALIDATION Schema
# The different types of data validations allowed
class ForecastDataSeriesMetaDataValidationSchema(str, Enum):
    INPUT_RESTRICTION = "input_restriction"


# Enum of INPUT_RESTRICTION
class ForecastDataSeriesMetaDataValidateInputRestrictions(str, Enum):
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    TOKEN_CHECK = "token_check"




# ForecastDataSeries
# An extension of Pandas Series to hold all the meta-data attributes we need to render a forecast model
class ForecastDataSeries(pd.Series):
    # tells Pandas Series object that these meta-data attributes are permanent (from:  https://pandas.pydata.org/docs/development/extending.html)
    #_metadata = [attrib.value for attrib in ForecastDataSeriesMetaDataSchema]
    _metadata = ["step_type", "action", "data_type", "display_type", "display_name", "validation", "pred", "args", "objs"]


    @property
    def _constructor(self):
        return ForecastDataSeries
    
    @property
    def _constructor_expanddim(self):
        return ForecastDataFrame
    

    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, *args, **kwargs):
        super(ForecastDataSeries, self).__init__(*args, **kwargs)

        # init all meta_data attributes
        for attrib in ForecastDataSeriesMetaDataSchema:
            setattr(self, attrib, None)


    def __copy__(self):
        print("Series copy!")
        super(ForecastDataSeries, self).__copy__()


    def __deepcopy__(self, memo):
        print("Series deepcopy!")
        super(ForecastDataSeries, self).__copy__(memo)




    # set_forecast_meta_data
    # Takes all the meta_data forecast as a set of arguments and stuffs them in the attributes of the object
    # easier to do than manually updating each attribute in the DataFrame object
    #  
    # INPUTS:
    #   Each meta-data field in the ForecastDataSeriesMetaDataSchema
    # 
    # OUTPUTS:
    #   NA

    def set_forecast_meta_data(self, 
                               step_type: ForecastDataSeriesMetaDataStepTypes, 
                               action: ForecastDataSeriesMetaDataAction, 
                               data_type: ForecastDataSeriesMetaDataDataType, 
                               display_type: ForecastDataSeriesMetaDataDataType,
                               display_name: str,
                               validation: list,
                               pred: list[str] = None, 
                               args: list = None, 
                               objs: list = None,):
        
        # store all the meta-data as attrbiutes in the Pandas Series
        self.step_type = step_type
        self.action = action
        self.data_type = data_type
        self.display_type = display_type
        self.display_name = display_name
        self.validation = validation
        self.pred = pred
        self.args = args
        self.objs = objs


    # set_forecast_meta_data_bulk
    # Takes all the meta_data forecast as a set of arguments and stuffs them in the attributes of the object
    # but in a bulk format (dict), might be easier to do when constantly copying from only pandas data series to new ones
    # (after a concat operations, for example, which wipes out all the meta-data)
    #  
    # INPUTS:
    #   Dict with name_value pairs for all the meta-data
    # 
    # OUTPUTS:
    #   NA

    def set_forecast_meta_data_bulk(self, meta_data_attribs: dict):
        for key in meta_data_attribs.keys():
            setattr(self, key, meta_data_attribs[key])
        


    # get_forecast_meta_data_bulk
    # Returns a dump of all the meta-data_attributes from the pandas data series, but in a bulnk format (dict)
    # might be easier to do when constantly copying from only pandas data series to new ones
    # (after a concat operations, for example, which wipes out all the meta-data)
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Dict with name_value pairs for all the meta-data

    def get_forecast_meta_data_bulk(self) -> dict:
        meta_data_attribs = {}

        for attrib in ForecastDataSeriesMetaDataSchema:
            meta_data_attribs[attrib] = getattr(self, attrib)

        return meta_data_attribs
    

    # __str__
    # Overrides the Pandas Series objects default print to add the meta_data attributes as well
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   string to display of the objects state

    def __str__(self):
        # get the Pandas Data Series default string
        results = super().__str__() + "\n" #+ f"\n\nmodel_type = {self.model_type}\nmodel_action = {self.model_action}\ndisplay_type = {self.display_type}\ndata_validation = {self.data_validation}\n"

        # add all the meta-data attributes in the ForecastDataSeriesMetaDataSchema
        for attrib in ForecastDataSeriesMetaDataSchema:
            results += f"{attrib} = {getattr(self, attrib)}\n"

        return results
    








# ForecastDataFrame
# An extension of Pandas DataFrame to hold ForecastDataSeries as well as provide some convenience getter and setter methods
# for managing the meta-data in ForecastDataSeries
class ForecastDataFrame(pd.DataFrame):
    
    @property
    def _constructor(self):
        return ForecastDataFrame
    
    @property
    def _constructor_sliced(self):
        return ForecastDataSeries
    

    def __copy__(self):
        print("Frame copy!")
        super(ForecastDataSeries, self).__copy__()


    def __deepcopy__(self, memo):
        print("Frame deepcopy!")
        super(ForecastDataSeries, self).__copy__(memo)




    # set_col_meta_data
    # Updates all the meta-data of a specific column (ForecastDataSeries) in the data frame
    #  
    # INPUTS:
    #   col:  Name of column (str) or position of column (0 based index)
    #   meta_data_attribs:  A dict of all the meta-data attributes to set
    # 
    # OUTPUTS:
    #   NA

    def set_col_meta_data(self, col: int | str, meta_data_attribs: dict):
        if(isinstance(col, int)):
            col = self.columns[col]

        # make sure the column is of type ForecastDataSeries
        if(not isinstance(self[col], ForecastDataSeries)):
            raise TypeError(f"* set_col_meta_data: column '{col}' is not of type 'ForecastDataSeries' but of type '{type(self[col])}'.")

        self[col].set_forecast_meta_data_bulk(meta_data_attribs)


    # get_col_meta_data
    # Get all the meta-data of a specific column (ForecastDataSeries) in the data frame
    #  
    # INPUTS:
    #   col:  Name of column (str) or position of column (0 based index)
    # 
    # OUTPUTS:
    #   meta_data_attribs:  A dict of all the meta-data attributes to set

    def get_col_meta_data(self, col: int | str) -> dict:
        if(isinstance(col, int)):
            col = self.columns[col]

        # make sure the column is of type ForecastDataSeries
        if(not isinstance(self[col], ForecastDataSeries)):
            raise TypeError(f"* get_col_meta_data: column '{col}' is not of type 'ForecastDataSeries' but of type '{type(self[col])}'.")

        return self[col].get_forecast_meta_data_bulk()
    

    # set_all_col_meta_data
    # Updates all the meta-data for all columns (ForecastDataSeries) in the Dataframe
    #  
    # INPUTS:
    #   all_meta_data_attribs:  A list of dicts, one dict (in order) for each column in the DataFrame
    # 
    # OUTPUTS:
    #   NA

    def set_all_col_meta_data(self, all_meta_data_attribs: list[dict]):
        colnames = self.columns

        # make sure we have meta-data to update for each column in the data frame.  I believe this strict validation will lower errors in the long term (losing or missing meta-data due to bugs in code)
        if(len(colnames) != len(all_meta_data_attribs)):
            raise ValueError(f"* set_all_col_meta_data: number of columns ({len(colnames)}) does not match number of meta_data_attributes provided ({len(all_meta_data_attribs)}).")

        # update each column with the new meta_data
        for i in range(len(all_meta_data_attribs)):
            self.set_col_meta_data(i, all_meta_data_attribs[i])

    
    # get_all_col_meta_data
    # Gets all the meta-data from all columns (ForecastDataSeries) in the Dataframe
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   all_meta_data_attribs:  A list of dicts, one dict (in order) for each column in the DataFrame

    def get_all_col_meta_data(self) -> list[dict]:
        all_meta_data_attribs = []
        num_columns = len(self.columns)

        # if there are no columns, raise an error
        if(num_columns < 1):
            raise ValueError(f"* get_all_col_meta_data:  no columns to update.")

        for i in range(num_columns):
            all_meta_data_attribs.append(self.get_col_meta_data(i))

        return(all_meta_data_attribs)


    # __str__
    # Overrides the Pandas DataFrame object default print to show the meta_data attributes for each column as well
    # (written because ForecastDataFrame does NOT call the __str__ method of each ForecastDataSeries)
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   string to display of the objects state

    def __str__(self):
        # get the Pandas Dataframe default string
        results = super().__str__() + "\n\n"

        # for each ForecastDataSeries, add it's meta-data to the print as well
        for colname in self.columns:
            results += f"col '{colname}':\n{self[colname].__str__()}\n"

        return results

    


    

    
