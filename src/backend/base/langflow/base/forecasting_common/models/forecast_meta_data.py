#####################################################################
# forecast_meta_data.py
#
# Implements a class which holds the meta-data for creating a forecast
# model
#
#####################################################################

from typing import Type
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
#import numpy as np
#import pandas as pd



# CONSTANTS
# =========



# CLASSES
# =======

# ForecastDataSeriesMetaDataSchema, ForecastDataSeriesMetaDataStepTypes, ForecastDataSeriesMetaDataAction, ForecastDataSeriesMetaDataDataType, ForecastDataSeriesMetaDataValidationSchema, ForecastDataSeriesMetaDataValidateInputRestrictions

# Enum holding the schema of the meta-data model
# The different meta-data attributes stores for each pandas data series (i.e. each column) in the forecast model
class ForecastMetaDataFrameSchema(str, Enum):
    INPUT_TYPE = "input_type"
    TIMESCALE = "timescale"
    START_YEAR = "start_year"
    START_MONTH = "start_month"
    NUM_PERIODS = "num_periods"
    MODEL = "model" # this attribute name cannot be changed (I explicilty use the attribute .model elsehwere in the code for simplicity)


# Enum holding the schema of the meta-data model
# The different meta-data attributes stores for each pandas data series (i.e. each column) in the forecast model
class ForecastMetaDataSeriesSchema(str, Enum):
    ID = "id"
    STEP_TYPE = "step_type" # this maps to the different component types in forecasting
    ACTION = "action"
    DATA_TYPE = "data_type"
    DISPLAY_TYPE = "display_type"
    DISPLAY_NAME = "display_name"
    VALIDATION = "validation" # a list of validation directives
    PRED = "pred" # predecessors, a set of column ids necessary for the action
    ARGS = "args" # any additional values necessary for actions, or validations
    OBJS = "objs" # any additional objects which are required for this step


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
    INPUT = "input" # set-up an input row for data entry
    SUM = "sum" # sum up a series of col ids (in preds) or constants
    PROD = "prod" # multiply a series of col ids (preds) or constants
    SUB = "sub"  # subtract a series of col ids (preds) or constants


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
    VALUE_CHECK = "value_check"


# Enum of INPUT_RESTRICTION
class ForecastDataSeriesMetaDataValidateInputRestrictions(str, Enum):
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    TOKEN_CHECK = "token_check"


# Enum of VALUE_CHECK
class ForecastDataSeriesMetaDataValidateValueChecks(str, Enum):
    LESS_EQUAL_THAN = "less_equal_than"




# ForecastMetaDataSeries
# Holds all the meta data for a pandas series (i.e. column) we need to render a forecast model
class ForecastMetaDataSeries():

    # INSTANCE VARIABLES
    # ------------------
    # meta_data - stores all the meta-data for this instance


    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, *args, **kwargs):
        self.meta_data = {}

        # init all meta_data attributes
        for attrib in ForecastMetaDataSeriesSchema:
            if attrib in kwargs:
                self.meta_data[attrib] = kwargs.get(attrib)
            else:
                self.meta_data[attrib] = None



    # set_forecast_meta_data
    # Takes all the meta_data forecast as a set of arguments and stuffs them in the attributes of the object
    # easier to do than manually updating each attribute in the DataFrame object
    #  
    # INPUTS:
    #   Each meta-data field in the ForecastDataSeriesMetaDataSchema
    # 
    # OUTPUTS:
    #   NA

    def set_forecast_meta_data(self, *args, **kwargs):
        for arg_name in kwargs:
            if arg_name in ForecastMetaDataSeriesSchema:
                self.meta_data[arg_name] = kwargs.get(arg_name)
            else:
                raise ValueError(f"*  set_forecast_meta_data:  invalid arg_name '{arg_name}'")
        

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
            if key in ForecastMetaDataSeriesSchema:
                self.meta_data[key] = meta_data_attribs[key]
            else:
                raise ValueError(f"*  set_forecast_meta_data_bulk:  invalid key '{key}'")
        


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

        for attrib in ForecastMetaDataSeriesSchema:
            meta_data_attribs[attrib] = self.meta_data[attrib]

        return meta_data_attribs
    


    # __str__
    # Return a printable version of the class instance
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Str with printable version of instance data

    def __str__(self):
        results = super().__str__()

        for attrib in ForecastMetaDataSeriesSchema:
            results += f"\n{attrib} = {self.meta_data[attrib]}"

        return results







# ForecastMetaDataFrame
# Holds all the meta data for a pandas dataframe we need to render a forecast model
class ForecastMetaDataFrame():

    # INSTANCE VARIABLES
    # ------------------
    # meta_data - stores all the forecast meta-data for this instance
    # model - {dict} stores all meta data for the specific columsn of the model (ForecastMetaDataSeries)


    # __init__
    # Initializing all meta-data attributes to None or to values passed in.  Initialize the model data structure
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, *args, **kwargs):
        self.meta_data = {}
        self.model = {}

        # init all meta_data attributes
        for attrib in ForecastMetaDataFrameSchema:
            if attrib in kwargs:
                self.meta_data[attrib] = kwargs.get(attrib)
            else:
                if(attrib != ForecastMetaDataFrameSchema.MODEL):
                    self.meta_data[attrib] = None



    # set_col_meta_data
    # Updates all the meta-data of a specific column (ForecastMetaDataSeries) in the model
    #  
    # INPUTS:
    #   col:  Name of column (str) or position of column (0 based index)
    #   meta_data_attribs:  A dict of all the meta-data attributes to set
    # 
    # OUTPUTS:
    #   NA

    def set_col_meta_data(self, col: int | str, meta_data_attribs: dict):
        if(isinstance(col, int)):
            col = list(self.model.keys())[col]

        self.model[col].set_forecast_meta_data_bulk(meta_data_attribs)


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
            col = list(self.model.keys())[col]

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
        num_columns = len(self.model.keys())

        # make sure we have meta-data to update for each column in the data frame.  I believe this strict validation will lower errors in the long term (losing or missing meta-data due to bugs in code)
        if(num_columns != len(all_meta_data_attribs)):
            raise ValueError(f"* set_all_col_meta_data: number of columns ({len(self.model.keys())}) does not match number of meta_data_attributes provided ({len(all_meta_data_attribs)}).")

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
        num_columns = len(self.model.keys())
        all_meta_data_attribs = []

        # if there are no columns, raise an error
        if(num_columns < 1):
            raise ValueError(f"* get_all_col_meta_data:  no columns to update.")

        for i in range(num_columns):
            all_meta_data_attribs.append(self.get_col_meta_data(i))

        return(all_meta_data_attribs)



    # concat_and_sum
    # Equivalent to forecast_data_model concat_and_sum, combines all the meta_datas using the concat function and,
    # if there is more than one data_object, adds a totals instruction line as well
    #  
    # INPUTS:
    #   datas:  List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    #   series_id:  If there ends up being a totals line, what is the unique ID to provide it
    #   display_name:  If there ends up being a totals line, what is the display name to provide it
    #   verify_integrity (optional: False) - Ensure that no columns have the same key (otherwise, it will write over the previous col value)
    #   drop_dups (optional:  False) - Drops columns with the same key (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame will all the elements combined

    @staticmethod
    def concat_and_sum(datas: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']], series_id: str, step_type: ForecastDataSeriesMetaDataStepTypes, display_name: str, verify_integrity: bool = False, drop_dups: bool = False, **kwargs) -> Type['ForecastMetaDataFrame']:
        if(datas is None or len(datas) < 1):
            raise ValueError("*  concat_and_sum:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # if there is only 1 element provided, no need to calculate and add total, simply run the concat with deduping of rows
        if len(datas) < 2:
            # we run concat even though there is one elements, because if that elements is a Series, concat will convert to a Frame
            meta_data = ForecastMetaDataFrame.concat(objs = datas, verify_integrity = verify_integrity, drop_dups = drop_dups)
        else:
            # get the ids of the last rows of all the meta_data fields, they will before the predecessor input into the totals
            list_of_pred_ids = ForecastMetaDataFrame._get_list_of_last_ids(datas = datas)

            # generate the instructions for the totals row
            meta_data_sum_series = ForecastMetaDataSeries(id = series_id,
                                                          step_type = step_type,
                                                          action = ForecastDataSeriesMetaDataAction.SUM,
                                                          data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                          display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                          display_name = display_name,
                                                          validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                          pred = list_of_pred_ids)
            
            # concat the list of data_objects provided (unpacked using '*') and the new totals line Series into one ForecastMetaDataFrame
            meta_data = ForecastMetaDataFrame.concat(objs = [*datas, meta_data_sum_series], verify_integrity = verify_integrity, drop_dups = drop_dups)

        return(meta_data)



    # _get_list_of_last_ids
    # Go through 
    #  
    # INPUTS:
    #   List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    # 
    # OUTPUTS:
    #   List of IDs

    @staticmethod
    def _get_list_of_last_ids(datas: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']]) -> list[str]:
        list_of_ids = []

        if(datas is None or len(datas) < 1):
            raise ValueError("*  _get_list_of_last_ids:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # iterate over all the data_objects in datas grabbing the last id
        for data_obj in datas:

            # handle the case that the data_obj is a ForecastMetaDataFrame
            if isinstance(data_obj, ForecastMetaDataFrame):
                # get the id of the last key in the model (the last column of ForecastMetaDataSeries to be added)
                list_of_ids.append(list(data_obj.model.keys())[-1])

            # handle the cast that the data_obj is ForecastMetaDataSeries
            else:
                list_of_ids.append(data_obj.meta_data[ForecastMetaDataSeriesSchema.ID])

        return(list_of_ids)







    # concat
    # Combine the meta_data from two or more ForecastMetaDataSeries or ForecastMetaDataFrames, designed to look similar to Pandas Concat
    #  
    # INPUTS:
    #   List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    #   verify_integrity (optional: False) - Ensure that no columns have the same key (otherwise, it will write over the previous col value)
    #   drop_dups (optional:  False) - Drops columns with the same key (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame will all the elements combined

    @staticmethod
    def concat(objs: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']], verify_integrity = False, drop_dups = False, **kwargs) -> Type['ForecastMetaDataFrame']:
        results_frame = ForecastMetaDataFrame()
        read_df = False

        if(objs is None or len(objs) < 1):
            raise ValueError("*  concat:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # grab the next object off the list
        for obj in objs:

            # handle the case that the next object is a ForecastMetaDataFrame
            if isinstance(obj, ForecastMetaDataFrame):

                # if we have already read a ForecastMetaDataFrame previously in the list, then we need 
                # to confirm that all meta_data in this ForecastMetaDataFrame (other than MODEL) matches
                # since we can't combine forecasts of different types
                if (read_df):
                    # make sure that all the forecast meta_data agrees, otherwise we can't merge it
                    if ForecastMetaDataFrame._check_meta_data(frame1 = obj, frame2 = results_frame):
                        results_frame = ForecastMetaDataFrame._append_cols(src_frame = obj, dest_frame = results_frame)
                    else:
                        raise ValueError("*  concat:  error ForecastMetaDataFrames do not have the same meta-data")
                    
                # if this is our first ForecastMetaDataFrame to concat, we can copy all the none_model meta_data over before merging the columns
                else:
                    results_frame = ForecastMetaDataFrame._copy_frame_meta_data(src_frame = obj, dest_frame = results_frame)
                    results_frame = ForecastMetaDataFrame._append_cols(src_frame = obj, dest_frame = results_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)
                    read_df = True

            # handle the case that the next object is a ForecastMetaDataSeries
            else:
                results_frame = ForecastMetaDataFrame._append_col(src_series = obj, dest_frame = results_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)

        return(results_frame)
    


    # _check_meta_data
    # Check all the NONE-model meta-data between two frames and ensure they are the same (since we can't combine different types of forecasts)
    #  
    # INPUTS:
    #   frame1 - First frame to check
    #   frame2 - Second frame to check
    # 
    # OUTPUTS:
    #   Bool - True is matches, False if not

    @staticmethod
    def _check_meta_data(frame1: Type['ForecastMetaDataFrame'], frame2: Type['ForecastMetaDataFrame']) -> bool:
        for attrib in ForecastMetaDataFrameSchema:
            if(attrib != ForecastMetaDataFrameSchema.MODEL):
                if(frame1.meta_data[attrib] != frame2.meta_data[attrib]):
                    return False
            
        return True
    
    

    # _copy_frame_meta_data
    # Copy all the NONE-model meta-data from one frame to another
    #  
    # INPUTS:
    #   src_frame - source of meta-data
    #   dest_frame - destination for meta-data
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's meta-data added

    @staticmethod
    def _copy_frame_meta_data(src_frame: Type['ForecastMetaDataFrame'], dest_frame: Type['ForecastMetaDataFrame']) -> Type['ForecastMetaDataFrame']:
        for attrib in ForecastMetaDataFrameSchema:
            if(attrib != ForecastMetaDataFrameSchema.MODEL):
                dest_frame.meta_data[attrib] = src_frame.meta_data[attrib]

        return dest_frame


    # _append_cols
    # Add all the cols in the src_frame to cols in the dest_frame, will overwrite existing cols in dest_frame if verify_integrity or drop_dups not set (see below)
    #  
    # INPUTS:
    #   src_frame - source of columns
    #   dest_frame - destination for columns
    #   verify_integrity (optional: False) - Raise an error if columns with the same key (in src) are attempted to be added to dest
    #   drop_dups (optional:  False) - Do not add columns from src to dest if a column with the same key already exists (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's columns added

    @staticmethod
    def _append_cols(src_frame: Type['ForecastMetaDataFrame'], dest_frame: Type['ForecastMetaDataFrame'], verify_integrity: bool = False, drop_dups: bool = False) -> Type['ForecastMetaDataFrame']:
        for col_key in src_frame.model.keys():
            dest_frame = ForecastMetaDataFrame._append_col(src_series = src_frame.model[col_key], dest_frame = dest_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)

        return dest_frame



    # _append_col
    # Add a ForecastMetaDataSeries src_series as a col in the dest_frame, will overwrite existing cols in dest_frame if verify_integrity or drop_dups not set (see below)
    #  
    # INPUTS:
    #   src_series - source of columns
    #   dest_frame - destination for columns
    #   verify_integrity (optional: False) - Raise an error if a key in the src_series already exists as a column in dest_frame
    #   drop_dups (optional:  False) - Do not add src_series if it's key already exists in dest_frame (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's columns added

    @staticmethod
    def _append_col(src_series: ForecastMetaDataSeries, dest_frame: Type['ForecastMetaDataFrame'], verify_integrity: bool = False, drop_dups: bool = False) -> Type['ForecastMetaDataFrame']:
        key = src_series.meta_data[ForecastMetaDataSeriesSchema.ID]
        
        if(drop_dups or verify_integrity):
            key_exists = key in dest_frame.model.keys()

            if(drop_dups and key_exists):
                return dest_frame
            elif(verify_integrity and key_exists):
                raise ValueError(f"*  _append_col:  col '{key}' already exists in ForecastMetaDataFrame\n\n{dest_frame}\n\n{src_series}\n")
            
        dest_frame.model[key] = src_series
        return dest_frame



    # __str__
    # Return a printable version of the class instance
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Str with printable version of instance data

    def __str__(self):
        results = super().__str__()

        # print ForecastMetaDataFrame specific meta-data
        for attrib in ForecastMetaDataFrameSchema:
            if(attrib != ForecastMetaDataFrameSchema.MODEL):
                results += f"\n{attrib} = {self.meta_data[attrib]}"

        # iterate on all columns and print their meta-data
        for col_key in self.model.keys():
            results += f"\n\nCol '{col_key}':\n{self.model[col_key]}"

        return results

    


    

    
