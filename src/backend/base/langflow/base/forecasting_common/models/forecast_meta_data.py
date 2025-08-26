#####################################################################
# forecast_meta_data.py
#
# Implements a class which holds the meta-data for creating a forecast
# model
#
#####################################################################

from typing import Type, Tuple
import nanoid
from langflow.schema.data import Data


# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
import datetime as datetime
from enum import Enum
import re as re
import numpy as np
import pandas as pd
import copy

from langflow.base.forecasting_common.models.date_utils import gen_dates, gen_pre_dates
from langflow.base.forecasting_common.constants import ForecastModelTimescale, ForecastModelInputTypes


# CONSTANTS
# =========



# ===========
# ENUMERATION
# ===========


# Enums of FORECAST_META_DATA_FRAME
# =================================

# Enum of ForecastMetaDataFrame
# The data storage attributes of this object / structure
class ForecastMetaDataFrameSchema(str, Enum):
    ID = "id"
    INPUT_TYPE = "input_type"
    TIMESCALE = "timescale"
    START_YEAR = "start_year"
    START_MONTH = "start_month"
    NUM_PERIODS = "num_periods"
    LAST_ID = "last_id" # the if of the last ForecastMetaDataSeries in the Frame (value or non-value)
    LAST_VALUE_ID = "last_value_id" # the if of the last ForecastMetaDataSeries in the Frame (value only)
    MODEL = "model" # a dict of the Series in this object.  NOTE:  This attribute name cannot be changed (I explicilty use the attribute .model elsehwere in the code for simplicity)




# Enums of FORECAST_META_DATA_SERIES
# ==================================

# Enum of ForecastMetaDataSeries
# The data storage attributes of this object / structure
class ForecastMetaDataSeriesSchema(str, Enum):
    ID = "id"
    STEP_TYPE = "step_type" # this maps to the different component types in forecasting
    ACTION = "action"
    RANGES = "ranges"   # allows for changing parameters of the action while processing a column of data
    DATA_TYPE = "data_type"
    DISPLAY_TYPE = "display_type"
    DISPLAY_NAME = "display_name"
    DATA_VALUES = "data_values"
    VALIDATION = "validation" # a list of validation directives
    PRED = "pred" # predecessors, a set of column ids necessary for the action
    ARGS = "args" # any additional values necessary for actions, or validations
    OBJS = "objs" # any additional objects which are required for this step


# Enum of ACTION
# Within in forecast step, what different actions are taken
class ForecastDataSeriesMetaDataAction(str, Enum):
    VALUES = "values" # display read-only values
    DATES = "dates" # display a set of dates
    INPUT = "input" # set-up an input row for data entry
    COPY = "copy" # copy the values from another row
    SUM = "sum" # sum up a series of col ids (in preds) or constants
    TOTAL = "total" # same as sum, but may be treated different visually
    PROD = "prod" # multiply a series of col ids (preds) or constants
    SUB = "sub"  # subtract a series of col ids (preds) or constants
    STEP_INIT = "step_init" # perform any initialization required for this step type
    YEAR_TO_MONTH = "year_to_month" # convert a yearly series to monthly
    MONTH_TO_YEAR = "month_to_year" # convert a monthly series to yearly
    SHIFT = "shift" # shift a series by a number of months (positive or negative)


# Enum of STEP_TYPE
# What are the different steps in the forecast process
class ForecastDataSeriesMetaDataStepTypes(str, Enum):
    EPIDEMIOLOGY = "epidemiology"
    POPULATION_CUT = "population_cut"
    PRICING = "pricing"
    SEGMENT = "segment"
    SUMMATION = "summation"
    TREATMENT = "treatment"
    DELAY = "delay"


# Enum of DATA_TYPE / DISPLAY_TYPE
# What are the different acceptable data types
class ForecastDataSeriesMetaDataDataType(str, Enum):
    DATE = "date"
    INT = "int"
    FLOAT = "float"
    PCT = "percent"
    CURRENCY = "currency"




# Enums of ARGS
# -------------

# Enums of STEP_INIT args

# Enum of arguments for StepInit Treatment action
class ForecastDataSeriesMetaDataArgsTreatmentStepInit(str, Enum):
    NEED_PRE_FORECAST_DATA = "need_pre_forecast_data"


# Enum of YEAR_TO_MONTH, MONTH_TO_YEAR args
# Enum of arguments for YTM and MTY actions
class ForecastDataSeriesMetaDataArgsMTYYTM(str, Enum):
    DATES = "dates"




# Enums of RANGES
# ---------------

# Enum holding the schema of the meta-data model
# The different meta-data attributes stores for each pandas data series (i.e. each column) in the forecast model
class ForecastMetaDataRangeSchema(str, Enum):
    COUNT = "count" # the number of elements in the column to apply this, if None, assume all remaining cells
    PRED = "pred" # predecessors, a set of column ids necessary for the action
    ARGS = "args" # any additional values necessary for actions, or validations
    OBJS = "objs" # any additional objects which are required for this step


# Enum holding the schema of the meta-data model
# The different meta-data attributes stores for each pandas data series (i.e. each column) in the forecast model
class ForecastMetaDataActionSchema(str, Enum):
    ACTION = "action"
    COUNT = "count" # the number of cells to apply this, if None, assume all remaining cells
    PRED = "pred" # predecessors, a set of column ids necessary for the action
    ARGS = "args" # any additional values necessary for actions, or validations
    OBJS = "objs" # any additional objects which are required for this step




# Forecast VALIDATION Enumations
# ------------------------------

# Enum of VALIDATION Schema
# The different types of data validations allowed
class ForecastDataSeriesMetaDataValidationSchema(str, Enum):
    INPUT_RESTRICTION = "input_restriction"
    VALUE_CHECK = "value_check"


# Enum of INPUT_RESTRICTION types
class ForecastDataSeriesMetaDataValidateInputRestrictions(str, Enum):
    READ_WRITE = "read_write"
    READ_ONLY = "read_only"
    TOKEN_CHECK = "token_check"

# Enum of VALUE_CHECK types
class ForecastDataSeriesMetaDataComparisonType(str, Enum):
    LT = "LT"
    LE = "LE"
    GE = "GE"
    GT = "GT"
    EQ = "EQ"
    NE = "NE"
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT_BETWEEN"






# =======
# CLASSES
# =======

# ForecastMetaDataSeriesIdGenerator
# This class encapsulates ID generation and ID parsing for all IDs generated in ForecastMetaData and DataFrame as part of
# the data-model
class ForecastMetaDataSeriesIdGenerator():
    # SAMPLE ID FORMAT:  (FULL_ID.)REL_ID():SINGLE_VALUE)(NUM_TO_SHIFT)     () = optional
    # EXAMPLE:  ABC.XYZ:2[1]        full_id = ABC, rel_id = XYZ, element = 2, shift address by left 1 time

    NANOID_CHAR_SET = "(A-Za-z0-9_-)"
    PREFIX_SEP_CHAR = "_"
    FULL_ID_SEP_CHAR = "."
    SINGLE_VALUE_SEP_CHAR = ":"

    full_match_regex = r"^\s*([\w-]+)\.(?!.*\.)"
    rel_match_regex = r"^([\w-]+)(:|\[|$)"
    shift_match_regex = r"\[(-?\d+)\]\s*$"
    single_match_regex = r"[^:+]:(-?\d+)"


    # instance variables
    # container: ForecastMetaDataFrame - the object holding this instance
    # container_id: str - the id of the object holding this instance

    def __init__(self, container: Type["ForecastMetaDataFrame"]):
        self.container = container


    # FULL ID
    # extract the full_id from the id string
    @staticmethod
    def get_full_id(id: str) -> str:
        match = re.search(ForecastMetaDataSeriesIdGenerator.full_match_regex, id)

        if(match):
            return match[1]
        else:
            return None
        
    # check if there is a full id
    @staticmethod
    def has_full_id(id: str) -> bool:
        if ForecastMetaDataSeriesIdGenerator.get_full_id(id) is None:
            return False
        else:
            return True
        
        
    # RELATIVE ID
    # extract the rel_id from the id string
    @staticmethod
    def get_rel_id(id: str) -> str:
        # check if you can find and remove the full portion of a reference first
        match = re.search(ForecastMetaDataSeriesIdGenerator.full_match_regex, id)

        if(match):
            id = id.removeprefix(match[0])

        match = re.search(ForecastMetaDataSeriesIdGenerator.rel_match_regex, id)

        if(match):
            return match[1]
        else:
            return None
        


    # has_rel_id is not provided, because all ids have to have a relative




    # SINGLE VALUE
    # extract the single_value from the id string
    @staticmethod
    def get_single_value(id: str) -> int:
        match = re.search(ForecastMetaDataSeriesIdGenerator.single_match_regex, id)

        if(match):
            return(int(match[1]))
        else:
            return None
        


    # check if ID has a single_value
    @staticmethod
    def has_single_value(id: str) -> bool:
        if ForecastMetaDataSeriesIdGenerator.get_single_value(id) is None:
            return False
        else:
            return True
        

        

    # SHIFT VALUE
    # extract the shift_value from the id string
    @staticmethod
    def get_shift_value(id: str) -> int:
        match = re.search(ForecastMetaDataSeriesIdGenerator.shift_match_regex, id)

        if match:
            return(int(match[1]))
        else:
            return None

    # check if ID has shift_value
    @staticmethod
    def has_shift_value(id: str) -> bool:
        if ForecastMetaDataSeriesIdGenerator.get_shift_value(id) is None:
            return False
        else:
            return True
        

        

    # parse_id
    # Given an id string, parse out all the different parts and return those and boolean indicators for what is there and what isn't
    #
    # INPUT:
    #   id - the id to parse
    #   default_full_id - (optional) the default full id, if provided, system will return it instead of None if no full-id is found
    #
    # OUTPUT:
    #   full_id or None
    #   rel_id
    #   single_value or None
    #   shift_value or None
    #   has_full_id - True if there was one, false if not (although default_full_id will be provided even if there isn't one)
    #   has_single_value - True if this is a single value address (i.e. XYZ:1), false if otherwise
    #   has_shift_value - True if this is a shift value address (i.e. XYZ[1]), false if otherwise
    
    @staticmethod
    def parse_id(id: str, default_full_id: str =  None) -> Tuple[str, str, int, int, bool, bool, bool]:
        has_full_id = False
        has_single_value = False
        has_shift_value = False

        full_id = None
        rel_id = None
        single_value = None
        shift_value = None

        # REL_ID
        rel_id = ForecastMetaDataSeriesIdGenerator.get_rel_id(id)

        # FULL_ID
        if(ForecastMetaDataSeriesIdGenerator.has_full_id(id)):
            has_full_id = True
            full_id = ForecastMetaDataSeriesIdGenerator.get_full_id(id)
        elif(default_full_id is not None):
            full_id = default_full_id

        # SINGLE_VALUE
        if(ForecastMetaDataSeriesIdGenerator.has_single_value(id)):
            has_single_value = True
            single_value = ForecastMetaDataSeriesIdGenerator.get_single_value(id)

        # SHIFT_VALUE
        if(ForecastMetaDataSeriesIdGenerator.has_shift_value(id)):
            has_shift_value = True
            shift_value = ForecastMetaDataSeriesIdGenerator.get_shift_value(id)

        return(full_id, rel_id, single_value, shift_value, has_full_id, has_single_value, has_shift_value)


    # get the parent container id
    def get_id(self) -> str:
        return self.container.get_id()
        

    # static_gen_rel_id
    @staticmethod
    def static_gen_rel_id(prefix: str = None, length: int = 5) -> str:
        if(prefix is None):
            return nanoid.generate(size=length)
        else:
            return f"{prefix}{ForecastMetaDataSeriesIdGenerator.PREFIX_SEP_CHAR}{nanoid.generate(size=length)}"

    # generate a relative ID
    def gen_rel_id(self, prefix: str = None, length: int = 5) -> str:
        if(prefix is None):
            return nanoid.generate(size=length)
        else:
            return f"{prefix}{ForecastMetaDataSeriesIdGenerator.PREFIX_SEP_CHAR}{nanoid.generate(size=length)}"
        
        
    # generate a full id
    def gen_full_id(self, prefix: str = None, length: int = 5) -> str:
        if(prefix is None):
            return f"{container.id}{nanoid.generate(size=length)}"
        else:
            return f"{self.get_id()}{self.FULL_ID_SEP_CHAR}{prefix}{self.PREFIX_SEP_CHAR}{nanoid.generate(size=length)}"
        

    # convert a relative id to a full id
    def rel_to_full_id(self, rel_id: str) -> str:
        return(f"{self.get_id()}{self.FULL_ID_SEP_CHAR}{rel_id}")

        # if(self.check_rel_id(rel_id)):
        #     return(f"{self.get_id()}{self.FULL_ID_SEP_CHAR}{rel_id}")
        # else:
        #     raise ValueError(f"\n*  rel_to_full_id:  Invalid relative ID provided '{rel_id}', relative id cannon contain a '{self.FULL_ID_SEP_CHAR}'.")

        
    # convert a full_id to a relative id
    @staticmethod
    def full_to_rel_id(full_id: str) -> str:
        if(ForecastMetaDataSeriesIdGenerator.has_full_id(full_id)):
            full_id_prefix = ForecastMetaDataSeriesIdGenerator.get_full_id(full_id)
            return full_id.removeprefix(full_id_prefix)
        else:
            raise ValueError(f"\n*  full_to_rel_id:  Invalid full ID provided '{full_id}'.")






# ForecastMetaDataRange
# Holds a set a specific range for the Series' action with specific parameters (reqs, args, objs).  This allows us to change the parameters (and therefore the calculations)
# over the course of an action processing a column of data (which is important for Treatment)
class ForecastMetaDataRange():

    # CLASS VARIABLES
    # ---------------


    # INSTANCE VARIABLES
    # ------------------
    _meta_data = None


    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, *args, **kwargs):
        self._meta_data = {}

        # init all meta_data attributes
        for attrib in ForecastMetaDataRangeSchema:
            if attrib in kwargs:
                self._meta_data[attrib] = kwargs.get(attrib)
            else:
                self._meta_data[attrib] = None


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
            if arg_name in ForecastMetaDataRangeSchema:
                self._meta_data[arg_name] = kwargs.get(arg_name)
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
            if key in ForecastMetaDataRangeSchema:
                self._meta_data[key] = meta_data_attribs[key]
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

        for attrib in ForecastMetaDataRangeSchema:
            meta_data_attribs[attrib] = self._meta_data[attrib]

        return meta_data_attribs
    

    # COUNT = "count" # the number of elements in the column to apply this, if None, assume all remaining cells
    # PRED = "pred" # predecessors, a set of column ids necessary for the action
    # ARGS = "args" # any additional values necessary for actions, or validations
    # OBJS = "objs" # any additional objects which are required for this step

    def has_count(self):
        if (ForecastMetaDataRangeSchema.COUNT in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataRangeSchema.COUNT] is not None):
            return True
        else:
            return False


    def get_count(self):
        if(self.has_count()):
            return self._meta_data[ForecastMetaDataRangeSchema.COUNT]


    
    def get_pred(self):
        return self._meta_data[ForecastMetaDataRangeSchema.PRED]

    

    
    def has_args(self):
        if (ForecastMetaDataRangeSchema.ARGS in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataRangeSchema.ARGS] is not None) and (len(self._meta_data[ForecastMetaDataRangeSchema.ARGS]) > 0):
            return True
        else:
            return False
    

    def get_args(self):
        if(self.has_args()):
            return self._meta_data[ForecastMetaDataRangeSchema.ARGS]


    def get_arg(self, name: str):
        if(self.has_args()):
            if(name in self._meta_data[ForecastMetaDataRangeSchema.ARGS].keys()):
                return(self._meta_data[ForecastMetaDataRangeSchema.ARGS][name])
            else:
                return None
        else:
            return None


    
    
    def has_objs(self):
        if (ForecastMetaDataRangeSchema.OBJS in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataRangeSchema.OBJS] is not None) and (len(self._meta_data[ForecastMetaDataRangeSchema.OBJS]) > 0):
            return True
        else:
            return False


    def get_objs(self):
        if(self.has_objs()):
            return self._meta_data[ForecastMetaDataRangeSchema.OBJS]


    def get_obj(self, name: str):
        if(self.has_objs()):
            if(name in self._meta_data[ForecastMetaDataRangeSchema.OBJS].keys()):
                return(self._meta_data[ForecastMetaDataRangeSchema.OBJS][name])
            else:
                return None
        else:
            return None





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

        for attrib in ForecastMetaDataRangeSchema:
            results += f"\n{attrib} = {self._meta_data[attrib]}"

        return results
    

    
    # to_json
    # Return a printable version of the class instance
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Str with printable version of instance data

    def to_json(self, indent: int = 4) -> str:
        import json
        return json.dumps(self, default=ForecastMetaDataJsonSerializer, indent=indent)




# ForecastMetaDataSeries
# Holds all the meta data for a pandas series (i.e. column) we need to render a forecast model
class ForecastMetaDataSeries():

    # CLASS VARIABLES
    # ---------------
    NON_VALUE_ACTIONS = [ForecastDataSeriesMetaDataAction.STEP_INIT]

    # INSTANCE VARIABLES
    # ------------------
    _meta_data: dict = None


    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, *args, **kwargs):
        self._meta_data: dict = {}

        # init all meta_data attributes
        for attrib in ForecastMetaDataSeriesSchema:
            if attrib in kwargs:
                self._meta_data[attrib] = kwargs.get(attrib)
            else:
                self._meta_data[attrib] = None



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
                self._meta_data[arg_name] = kwargs.get(arg_name)
            else:
                raise ValueError(f"\n*  set_forecast_meta_data:  invalid arg_name '{arg_name}'")
        

    # # set_forecast_meta_data_bulk
    # # Takes all the meta_data forecast as a set of arguments and stuffs them in the attributes of the object
    # # but in a bulk format (dict), might be easier to do when constantly copying from only pandas data series to new ones
    # # (after a concat operations, for example, which wipes out all the meta-data)
    # #  
    # # INPUTS:
    # #   Dict with name_value pairs for all the meta-data
    # # 
    # # OUTPUTS:
    # #   NA

    # def set_forecast_meta_data_bulk(self, meta_data_attribs: dict):
    #     for key in meta_data_attribs.keys():
    #         if key in ForecastMetaDataSeriesSchema:
    #             self.meta_data[key] = meta_data_attribs[key]
    #         else:
    #             raise ValueError(f"*  set_forecast_meta_data_bulk:  invalid key '{key}'")
        


    # # get_forecast_meta_data_bulk
    # # Returns a dump of all the meta-data_attributes from the pandas data series, but in a bulnk format (dict)
    # # might be easier to do when constantly copying from only pandas data series to new ones
    # # (after a concat operations, for example, which wipes out all the meta-data)
    # #  
    # # INPUTS:
    # #   NA
    # # 
    # # OUTPUTS:
    # #   Dict with name_value pairs for all the meta-data

    # def get_forecast_meta_data_bulk(self) -> dict:
    #     meta_data_attribs = {}

    #     for attrib in ForecastMetaDataSeriesSchema:
    #         meta_data_attribs[attrib] = self.meta_data[attrib]

    #     return meta_data_attribs
    


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
            results += f"\n{attrib} = {self._meta_data[attrib]}"

        return results
    

    
    # to_json
    # Return a printable version of the class instance
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Str with printable version of instance data
    def to_json(self, indent: int = 4) -> str:
        import json
        return json.dumps(self, default=ForecastMetaDataJsonSerializer, indent=indent)
    

    # get_id
    # get the id of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   the id of this Series
    def get_id(self) -> str:
        return(self._meta_data[ForecastMetaDataSeriesSchema.ID])        


    # get_action
    # get the action of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   the action of this Series
    def get_action(self) -> ForecastDataSeriesMetaDataAction:
        return(self._meta_data[ForecastMetaDataSeriesSchema.ACTION])


    # get_step_type
    # get the step_type of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   Step_type
    def get_step_type(self) -> ForecastDataSeriesMetaDataStepTypes:
        return(self._meta_data[ForecastMetaDataSeriesSchema.STEP_TYPE])
    
    # get_data_type
    # get the data_type of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   data_type
    def get_data_type(self) -> ForecastDataSeriesMetaDataDataType:
        return(self._meta_data[ForecastMetaDataSeriesSchema.DATA_TYPE])


    # get_display_type
    # get the display_type of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   display_type
    def get_display_type(self) -> ForecastDataSeriesMetaDataDataType:
        return(self._meta_data[ForecastMetaDataSeriesSchema.DISPLAY_TYPE])
    

    # display_name
    # get the display_name of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   the display_name of this Series
    def get_display_name(self) -> str:
        return(self._meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME])
    

    # has_data_values
    # Checks if there are data_values for the Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   True or False
    def has_data_values(self) -> bool:
        if (ForecastMetaDataSeriesSchema.DATA_VALUES in self._meta_data) and (self._meta_data[ForecastMetaDataSeriesSchema.DATA_VALUES] is not None) and (len(self._meta_data[ForecastMetaDataSeriesSchema.DATA_VALUES]) > 0):
            return True
        else:
            return False
        

    # get_data_values
    # get the data_values of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   the data_values of this Series
    def get_data_values(self) -> list:
        return(self._meta_data[ForecastMetaDataSeriesSchema.DATA_VALUES])


    # get_validation
    # get the validation of this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   the validation of this Series
    def get_validation(self) -> dict[ForecastDataSeriesMetaDataValidationSchema]:
        return(self._meta_data[ForecastMetaDataSeriesSchema.VALIDATION])


    # has_ranges
    # Return true if this Series has ranges (i.e. one or more entries in the ranges meta_data), False otherwise
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   True or False
    def has_ranges(self) -> bool:
        if(ForecastMetaDataSeriesSchema.RANGES not in self._meta_data.keys()) or (self._meta_data[ForecastMetaDataSeriesSchema.RANGES] is None) or (len(self._meta_data[ForecastMetaDataSeriesSchema.RANGES]) < 1):
            return False
        else:
            return True
        

    # get_ranges
    # get the ranges (a list of ForecastMetaDataRange) for this Series
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   list of ForecastMetaDataRange, or None if there are none
    def get_ranges(self) -> list[ForecastMetaDataRange]:
        if self.has_ranges():
            return self._meta_data[ForecastMetaDataSeriesSchema.RANGES]
        else:
            return None
        

    # is_value_action
    # Return true if this Series generates/has values (i.e. not a pure meta_data / command action like STEP_INIT)
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   True or False

    def is_value_action(self) -> bool:
        if self._meta_data[ForecastMetaDataSeriesSchema.ACTION] in self.NON_VALUE_ACTIONS:
            return(False)
        else:
            return(True)
        

    # has_arg
    # Return true if this Series has args (i.e. one or more entries in the args meta_data), False otherwise
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   True or False
    def has_arg(self) -> bool:
        if(ForecastMetaDataSeriesSchema.ARGS not in self._meta_data.keys()) or (self._meta_data[ForecastMetaDataSeriesSchema.ARGS] is None) or (len(self._meta_data[ForecastMetaDataSeriesSchema.ARGS]) < 1):
            return False
        else:
            return True
        
        
    # get_args
    # return all args as a dict   
    def get_args(self):
        if self.has_arg():
            return self._meta_data[ForecastMetaDataSeriesSchema.ARGS]
        else:
            return None
            
    
    # get_arg
    # get a specific arg by name    
    def get_arg(self, arg_name: str):
        if self.has_arg():
            if arg_name in self._meta_data[ForecastMetaDataSeriesSchema.ARGS].keys():
                return self._meta_data[ForecastMetaDataSeriesSchema.ARGS][arg_name]
            else:
                return None
        else:
            return None
            
    
    # has_obj
    # Return true if this Series has objs (i.e. one or more entries in the objs meta_data), False otherwise
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   True or False
    def has_obj(self) -> bool:
        if(ForecastMetaDataSeriesSchema.OBJS not in self._meta_data.keys()) or (self._meta_data[ForecastMetaDataSeriesSchema.OBJS] is None) or (len(self._meta_data[ForecastMetaDataSeriesSchema.OBJS]) < 1):
            return False
        else:
            return True
        

    # get_objs
    # return all objs as a dict
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #  dict of all objects {name of obj: obj}
    def get_objs(self):
        if(self.has_obj()):
            return self._meta_data[ForecastMetaDataSeriesSchema.OBJS]
        else:
            return None

    # get_obj
    # get a specific obj by name
    #
    # INPUTS:
    #   obj_name - the name of the object to get
    #
    # OUTPUTS:
    #   the object, or None if it doesn't exist
    def get_obj(self, obj_name: str):
        if(self.has_obj()):
            if obj_name in self._meta_data[ForecastMetaDataSeriesSchema.OBJS].keys():
                return self._meta_data[ForecastMetaDataSeriesSchema.OBJS][obj_name]
            else:
                return None
        else:
            return None


    # has_pred
    # Return true if this Series has pred (i.e. one or more entries in the ranges meta_data), False otherwise
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   True or False
    def has_preds(self) -> bool:
        if(ForecastMetaDataSeriesSchema.PRED not in self._meta_data.keys()) or (self._meta_data[ForecastMetaDataSeriesSchema.PRED] is None) or (len(self._meta_data[ForecastMetaDataSeriesSchema.PRED]) < 1):
            return False
        else:
            return True


    # get pred
    # get the preds (a list of column ids) for this Series
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   list of column ids, or None if there are none
    def get_pred(self):
        if(self.has_preds()):
            return self._meta_data[ForecastMetaDataSeriesSchema.PRED]
        else:
            return None


        







# ForecastMetaDataFrame
# Holds all the meta data for a pandas dataframe we need to render a forecast model
class ForecastMetaDataFrame():

    # INSTANCE VARIABLES
    # ------------------
    # meta_data - (dict) stores all the forecast meta-data for this instance
    # model - (dict of ForecastMetaDataSeries) stores all meta data for the specific columsn of the model (ForecastMetaDataSeries)
    # id_mgr - (ForecastMetaDataSeriesIdGenerator) can handle all id tasks (generation, conversion, information methods) 

    _meta_data: dict = None
    _model: dict[ForecastMetaDataSeries] = None


    # __init__
    # Initializing all meta-data attributes to None or to values passed in.  Initialize the model data structure
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, id_prefix: str = "ForecastMetaDataFrame", *args, **kwargs):
        self.id_prefix: str = id_prefix
        self._meta_data: dict = {}
        self._model: dict[ForecastMetaDataSeries] = {}

        # create an id_mgr and put pointer to this object as it's container
        self.id_mgr:ForecastMetaDataSeriesIdGenerator = ForecastMetaDataSeriesIdGenerator(container = self)

        # Generate a unique ID
        if(id_prefix is not None):
            self._meta_data[ForecastMetaDataFrameSchema.ID] = self.id_mgr.gen_rel_id(prefix = id_prefix)
        else:
            self._meta_data[ForecastMetaDataFrameSchema.ID] = self.id_mgr.gen_rel_id(length = 10)


        # init all meta_data attributes
        for attrib in ForecastMetaDataFrameSchema:
            if attrib in kwargs:
                if(attrib != ForecastMetaDataFrameSchema.MODEL):    # this is done because MODEL is not a meta_data schema but on object attribute
                    self._meta_data[attrib] = kwargs.get(attrib)
                else:
                    self._model: dict[ForecastMetaDataSeries] = kwargs.get(attrib)
            else:
                if(attrib != ForecastMetaDataFrameSchema.MODEL):    # this is done because MODEL is not a meta_data schema but on object attribute
                    # if there is already a value there, leave it alone, if not, create and explicitly set to null
                    if not attrib in self._meta_data.keys():
                        self._meta_data[attrib] = None


        # if no last_id was provided, calculate the last_id
        if not self.has_last_id():
            self.set_last_id(id = self._get_last_id(value_series_only = False))

        # calculate the last_value_id, last_value_id is the last_id which is of type value (not a Step_Init).  Usually that means that same
        # thing, but sometimes it can be different
        if self.has_last_id():
            if not self.has_last_value_id():
                self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = self._get_last_id(value_series_only = True)
        else:
            self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = None





    # iterator interface(s)
    # ---------------------
    def __iter__(self):
        return self._model.__iter__()


    def __next__(self) -> ForecastMetaDataSeries:
        return self._model.__next__()

    def keys(self):
        return(self._model.keys())
    
    def values(self):
        return(self._model.values())
    
    def items(self):
        return(self._model.items())
    



    # # set_col_meta_data
    # # Updates all the meta-data of a specific column (ForecastMetaDataSeries) in the model
    # #  
    # # INPUTS:
    # #   col:  Name of column (str) or position of column (0 based index)
    # #   meta_data_attribs:  A dict of all the meta-data attributes to set
    # # 
    # # OUTPUTS:
    # #   NA

    # def set_col_meta_data(self, col: int | str, meta_data_attribs: dict):
    #     if(isinstance(col, int)):
    #         col = list(self.model.keys())[col]

    #     self.model[col].set_forecast_meta_data_bulk(meta_data_attribs)


    # # get_col_meta_data
    # # Get all the meta-data of a specific column (ForecastDataSeries) in the data frame
    # #  
    # # INPUTS:
    # #   col:  Name of column (str) or position of column (0 based index)
    # # 
    # # OUTPUTS:
    # #   meta_data_attribs:  A dict of all the meta-data attributes to set

    # def get_col_meta_data(self, col: int | str) -> dict:
    #     if(isinstance(col, int)):
    #         col = list(self.model.keys())[col]

    #     return self[col].get_forecast_meta_data_bulk()
    

    # # set_all_col_meta_data
    # # Updates all the meta-data for all columns (ForecastDataSeries) in the Dataframe
    # #  
    # # INPUTS:
    # #   all_meta_data_attribs:  A list of dicts, one dict (in order) for each column in the DataFrame
    # # 
    # # OUTPUTS:
    # #   NA

    # def set_all_col_meta_data(self, all_meta_data_attribs: list[dict]):
    #     num_columns = len(self.model.keys())

    #     # make sure we have meta-data to update for each column in the data frame.  I believe this strict validation will lower errors in the long term (losing or missing meta-data due to bugs in code)
    #     if(num_columns != len(all_meta_data_attribs)):
    #         raise ValueError(f"* set_all_col_meta_data: number of columns ({len(self.model.keys())}) does not match number of meta_data_attributes provided ({len(all_meta_data_attribs)}).")

    #     # update each column with the new meta_data
    #     for i in range(len(all_meta_data_attribs)):
    #         self.set_col_meta_data(i, all_meta_data_attribs[i])

    
    # # get_all_col_meta_data
    # # Gets all the meta-data from all columns (ForecastDataSeries) in the Dataframe
    # #  
    # # INPUTS:
    # #   NA
    # # 
    # # OUTPUTS:
    # #   all_meta_data_attribs:  A list of dicts, one dict (in order) for each column in the DataFrame

    # def get_all_col_meta_data(self) -> list[dict]:
    #     num_columns = len(self.model.keys())
    #     all_meta_data_attribs = []

    #     # if there are no columns, raise an error
    #     if(num_columns < 1):
    #         raise ValueError(f"* get_all_col_meta_data:  no columns to update.")

    #     for i in range(num_columns):
    #         all_meta_data_attribs.append(self.get_col_meta_data(i))

    #     return(all_meta_data_attribs)



    # # concat_and_sum
    # # Equivalent to forecast_data_model concat_and_sum, combines all the meta_datas using the concat function and,
    # # if there is more than one data_object, adds a totals instruction line as well
    # #  
    # # INPUTS:
    # #   datas:  List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    # #   series_id:  If there ends up being a totals line, what is the unique ID to provide it
    # #   display_name:  If there ends up being a totals line, what is the display name to provide it
    # #   verify_integrity (optional: False) - Ensure that no columns have the same key (otherwise, it will write over the previous col value)
    # #   drop_dups (optional:  False) - Drops columns with the same key (if this is set, verify_integrity is ignored)
    # # 
    # # OUTPUTS:
    # #   ForecastMetaDataFrame will all the elements combined

    # @staticmethod
    # def concat_and_sum(datas: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']], 
    #                    display_name: str,
    #                    new_total_line_id: str,
    #                    new_summation_id: str = None,
    #                    is_total: bool = False,
    #                    new_total_values: pd.Series = None,
    #                    verify_integrity: bool = False,
    #                    drop_dups: bool = False,
    #                    **kwargs) -> Type['ForecastMetaDataFrame']:
        
    #     if(datas is None or len(datas) < 1):
    #         raise ValueError("*  concat_and_sum:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

    #     # if there is only 1 element provided, no need to calculate and add total, simply run the concat with deduping of rows
    #     if len(datas) < 2:
    #         # we run concat even though there is one elements, because if that elements is a Series, concat will convert to a Frame
    #         meta_data = ForecastMetaDataFrame.concat(objs = datas, verify_integrity = verify_integrity, drop_dups = drop_dups)

    #     else:
    #         # get the ids of the last rows of all the meta_data fields, they will before the predecessor input into the totals
    #         (list_of_pred_ids, list_of_forecast_series) = ForecastMetaDataFrame._get_list_of_last_ids(datas = datas)
    #         (display_type, data_type) = ForecastMetaDataFrame._get_display_data_type(list_of_forecast_series)

    #         # generate a new step for summation
    #         meta_data_step_init_series = ForecastMetaDataSeries(id = f"{new_summation_id}_Init",
    #                                                             step_type = ForecastDataSeriesMetaDataStepTypes.SUMMATION,
    #                                                             action = ForecastDataSeriesMetaDataAction.STEP_INIT,
    #                                                             data_type = data_type,
    #                                                             display_type = display_type,
    #                                                             display_name = display_name,
    #                                                             validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
    #                                                             pred = list_of_pred_ids)
            
    #         # generate the instructions for the totals row
    #         action_type = ForecastDataSeriesMetaDataAction.SUM

    #         if(is_total):
    #             action_type = ForecastDataSeriesMetaDataAction.TOTAL

    #         meta_data_sum_series = ForecastMetaDataSeries(id = new_total_line_id,
    #                                                       step_type = ForecastDataSeriesMetaDataStepTypes.SUMMATION,
    #                                                       action = action_type,
    #                                                       data_type = data_type,
    #                                                       display_type = display_type,
    #                                                       display_name = display_name,
    #                                                       data_values = new_total_values.to_list(),
    #                                                       validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
    #                                                       pred = list_of_pred_ids)
            
    #         # concat the list of data_objects provided (unpacked using '*') and the new totals line Series into one ForecastMetaDataFrame
    #         meta_data = ForecastMetaDataFrame.concat(objs = [*datas, meta_data_step_init_series, meta_data_sum_series], verify_integrity = verify_integrity, drop_dups = drop_dups)
    #         meta_data.set_last_id(new_total_line_id)

    #     return(meta_data)


    # ZIV
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
    def concat(objs: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']], 
               verify_integrity = False, 
               drop_dups = False, 
               last_id: str = None, 
               **kwargs) -> Type['ForecastMetaDataFrame']:
        results_frame = ForecastMetaDataFrame()
        read_df = False

        if(objs is None or len(objs) < 1):
            raise ValueError("*  concat:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # grab the next object off the list
        for obj in objs:

            # handle the case that the next object is a ForecastMetaDataFrame
            if isinstance(obj, ForecastMetaDataFrame):

                # if this is our first ForecastMetaDataFrame to concat, we can copy all the non_model meta_data over before merging the columns
                if (not read_df):
                    results_frame = copy.deepcopy(obj)
                    read_df = True
                
                # if we have already read a ForecastMetaDataFrame previously in the list, then we need 
                # to confirm that all meta_data in this ForecastMetaDataFrame (other than MODEL) matches
                # since we can't combine forecasts of different types
                else:
                    # make sure that all the forecast meta_data agrees, otherwise we can't merge it
                    if (not verify_integrity) or (ForecastMetaDataFrame._check_meta_data(frame1 = obj, frame2 = results_frame)):
                        #results_frame = ForecastMetaDataFrame._append_cols(src_frame = obj, dest_frame = results_frame)
                        results_frame = ForecastMetaDataFrame._append_cols(src_frame = obj, dest_frame = results_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)
                    else:
                        raise ValueError("*  concat:  error ForecastMetaDataFrames do not have the same meta-data")
                    
            # handle the case that the this object is a ForecastMetaDataSeries
            else:
                results_frame = ForecastMetaDataFrame._append_col(src_series = obj, dest_frame = results_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)

        if(last_id is not None):
            results_frame.set_last_id(last_id)

        # ZIV:  for the moment, don't let last_id be implicitly set, get everyone to explicitly set the value
        # else:
        #     results_frame._meta_data[ForecastMetaDataFrameSchema.LAST_ID] = results_frame._get_last_id()

        return(results_frame)
    
    
    # add_col_data_meta
    # Generate and add a ForecastMetaDataSeries to an ForecastMetaDataFrame

    @staticmethod
    def add_col_meta_data(frame: Type['ForecastMetaDataFrame'],
                          id: str,
                        #   display_name: str,
                        #   data_values: pd.Series,
                        #   step_type: ForecastDataSeriesMetaDataStepTypes,
                        #   action: ForecastDataSeriesMetaDataAction,
                        #   data_type: ForecastDataSeriesMetaDataDataType,
                        #   display_type: ForecastDataSeriesMetaDataDataType,
                        #   validation: List[Dict[ForecastDataSeriesMetaDataValidationSchema, Any]],
                        #   pred: List[str | int | float] = None,
                        #   args: Dict = None,
                        #   objs: List = None,
                          update_last_id = False,
                          verify_integrity: bool = True,
                          drop_dups: bool = False, 
                          **kwargs) -> Type['ForecastMetaDataFrame']:
        
        new_series = ForecastMetaDataSeries(id = id, **kwargs)
        frame._append_col(new_series, frame, verify_integrity = verify_integrity, drop_dups = drop_dups)

        if(update_last_id):
            frame.set_last_id(id = id)

        return(frame)
    

    # BUNCH OF HELPER FUNCTIONS TO QUICKLY GET THE OVERALL META-DATA FROM THE FRAME (the stuff that isn't going to change)

    # META_DATA

    # get_id
    # get the ID for the Frame
    def get_id(self) -> str:
        return(self._meta_data[ForecastMetaDataFrameSchema.ID])        


    # get_timescale
    # get the TIMESCALE of the frame
    def get_timescale(self) -> ForecastModelTimescale:
        return(self._meta_data[ForecastMetaDataFrameSchema.TIMESCALE])
        
    def set_timescale(self, value: ForecastMetaDataFrameSchema):
        self._meta_data[ForecastMetaDataFrameSchema.TIMESCALE] = value



    def get_start_year(self) -> int:
        return(self._meta_data[ForecastMetaDataFrameSchema.START_YEAR])
    
    def set_start_year(self, value: int):
        self._meta_data[ForecastMetaDataFrameSchema.START_YEAR] = value




    def get_start_month(self) -> int:
        return(self._meta_data[ForecastMetaDataFrameSchema.START_MONTH])
    
    def set_start_month(self, value: int):
        self._meta_data[ForecastMetaDataFrameSchema.START_MONTH] = value




    def get_num_periods(self) -> int:
        return(self._meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS])
    
    def set_num_periods(self, value: int):
        self._meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS] = value


    
    def get_input_type(self) -> ForecastModelInputTypes:
        return(self._meta_data[ForecastMetaDataFrameSchema.INPUT_TYPE])
    
    def set_input_type(self, value: ForecastModelInputTypes):
        self._meta_data[ForecastMetaDataFrameSchema.INPUT_TYPE] = value




    # _MODEL

    # has_series
    # Returns true if the _model has 1 or more series in it
    def has_series(self) -> bool:
        if hasattr(self, "_model") and (self._model is not None) and (len(self._model) > 0):
            return True
        else:
            return False
        

    # get_series_ids()
    def get_series_ids(self) -> list[str]:
        if self.has_series():
            return list(self._model.keys())
        else:
            return None
        

    # get_num_series
    def get_num_series(self) -> int:
        series = self.get_series_ids()

        if series is None:
            return 0
        else:
            return len(series)
        

    # get_series
    # given a key or an index into the list of actions, return the corresponding Series object
    def get_series(self, id: int | str) -> ForecastMetaDataSeries:
        # if an int is provided, it's an index, if a string, it's a key
        # convert index to key
        if isinstance(id, int):
            #id = list(self._model.keys())[id]
            id = self.get_series_ids()[id]
        
        return self._model[id]
    

    # has_series_id
    # given a series id, return true if it's among the ForecastMetaDataSeries in model, false otherwise
    def has_series_id(self, id: str) -> bool:
        if(not self.has_series()):
            return False
        else:
            if(id in self.get_series_ids()):
                return True
            else:
                return False






    # _MODEL: LAST SERIES

    # get_last_series
    # Return the series which is pointed to by LAST_ID in _meta_data
    def get_last_series(self, value_series_only = False) -> ForecastMetaDataSeries:
        return self._model[self.get_last_id(value_series_only = value_series_only)]


    # get_last_id
    # Get the id of the last column
    def get_last_id(self, value_series_only: bool = False) -> str:

        # find LAST_ID
        if not value_series_only:
            if not self.has_last_id():
                raise ValueError(f"\n* get_last_id:  No last_id.")
            
            last_id: str = self._meta_data[ForecastMetaDataFrameSchema.LAST_ID]

            # check to make sure this last_id still exists in the model
            if not self.has_series_id(last_id):
                raise ValueError(f"\n* get_last_id: invalid last_id '{last_id}' not found in model keys {self.get_series_ids()}")
            
            # if everything checks out, return it
            return(last_id)
        
        # find LAST_VALUE_ID
        else:
            if not self.has_last_value_id():
                raise ValueError(f"\n* get_last_id:  value_series_only = True, but no last_value_id.")
            
            last_value_id: str = self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID]

            # check to make sure this last_value_id still exists in the model
            if not self.has_series_id(last_value_id):
                raise ValueError(f"\n* get_last_id: value_series_only - True, invalid last_value_id '{last_value_id}' not found in model keys {self.get_series_ids()}")
            
            # if everything checks out, return it
            return(last_value_id)
    

    # convenience wrapper around get_last_id
    def get_last_value_id(self) -> str:
        last_value_id = self.get_last_id(value_series_only = True)
        return(last_value_id)


    def has_last_id(self) -> bool:
        if (ForecastMetaDataFrameSchema.LAST_ID in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataFrameSchema.LAST_ID] is not None):
            return True
        else:
            return False
        
    def has_last_value_id(self) -> bool:
        if (ForecastMetaDataFrameSchema.LAST_VALUE_ID in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] is not None):
            return True
        else:
            return False

        
    # _get_last_id
    # NOTE:  This is a special function that doesn't depend on the value of ForecastMetaDataSeriesSchema.LAST_ID or LAST_VALUE_ID,
    # it grabs all the series ids, and takes the last ID in the order (or if value_series_only is true, starts at the end and exists at the).
    # first series which is a value_action.
    #   
    # This function should only be used during the __init__ function of ForecastMetaDataFrame to set its last_id / last_value_id, if it's not explicitly set 
    # and should not be otherwise used (use get_last_id and get_last_value_id respectively)
    def _get_last_id(self, value_series_only = False) -> str:
        last_id = None

        if self.has_series():
           last_id = list(self._model.keys())[-1]
        else:
            return None

        if not value_series_only:
            return(last_id)
        else:
            curr_series: ForecastMetaDataSeries = self.get_series(last_id)
        
            if curr_series.is_value_action():
                return(last_id)

            # algorithm for value_series_only check        
            #start_index = list(self._model.keys()).index(last_id) - 1
            start_index = self.get_series_ids().index(last_id) - 1

            if(start_index > 0): 
                for i in range(start_index-1, 1, -1):
                    curr_id: str = self.get_series_ids()[i]
                    curr_series: ForecastMetaDataSeries = self.get_series(curr_id)

                    if(curr_series.is_value_action()):
                        return(curr_id)
                    
            # raise ValueError(f"\n*  get_last_series:  error, no value series found from '{last_id}' backwards {self.get_series_ids()}.")
            return None




    # set_last_id
    # NOTE:  there is NO set_last_value_id function because value ID should never be set separately from last_id.
    #        last_value_id is simply the last_id that is a value_action (series), so whenever we set a last_id, we simply
    #        check if it's a value_id and set it there, if not, we leave as is
    def set_last_id(self, id: str):

        if (id is None):
            self._meta_data[ForecastMetaDataFrameSchema.LAST_ID] = None
            self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = None
            return

        if (self.has_series_id(id)):
            self._meta_data[ForecastMetaDataFrameSchema.LAST_ID] = id

            if(self.get_series(id).is_value_action()):
                self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = id
        else:
            raise ValueError(f"\n* set_last_id:  error, id value '{id}' does not exist in {self.get_id()}: {self.get_series_ids}")


    # get_last_data_type
    def get_last_display_name(self, value_series_only = False) -> str:
        return(self.get_last_series(value_series_only = value_series_only).get_display_name())


    # get_last_data_type
    def get_last_data_type(self, value_series_only = False) -> ForecastDataSeriesMetaDataDataType:
        return(self.get_last_series(value_series_only = value_series_only).get_data_type())


    # get_last_display_type
    def get_last_display_type(self, value_series_only = False) -> ForecastDataSeriesMetaDataDataType:
        return(self.get_last_series(value_series_only = value_series_only).get_display_type())


    # get_last_step_type(self) 
    def get_last_step_type(self, value_series_only = False) -> ForecastDataSeriesMetaDataStepTypes:
        return(self.get_last_series(value_series_only = value_series_only).get_step_type())


    # get_last_values(self) 
    def get_last_values(self, value_series_only = False) -> list:
        return(self.get_last_series(value_series_only = value_series_only).get_data_values())


    # get first date in the forecast
    def get_first_date(self) -> datetime:
        start_year = self.get_start_year()
        start_month = self.get_start_month()
        timescale = self.get_timescale()
        return(gen_dates(start_year=start_year, start_month = start_month, num_years=1, time_scale = timescale)[0])


    # get last date in the forecast
    def get_last_date(self) -> datetime:
        start_year = self.get_start_year()
        start_month = self.get_start_month()
        num_periods = self.get_num_periods()
        timescale = self.get_timescale()


        if(self.get_timescale() == ForecastModelTimescale.MONTH):
            num_periods = num_periods / 12
        
        return(gen_dates(start_year=start_year, start_month = start_month, num_years=num_periods, time_scale = timescale)[-1])









    # PRIVATE HELPER FUNCTIONS
    # ------------------------



    # _get_list_of_last_ids
    # Go through 
    #  
    # INPUTS:
    #   List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    # 
    # OUTPUTS:
    #   List of IDs

    @staticmethod
    def _get_list_of_last_ids(datas: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']]) -> tuple[list[str], list[ForecastMetaDataSeries]]:
        list_of_ids = []
        list_of_forecast_series = []

        if(datas is None or len(datas) < 1):
            raise ValueError("*  _get_list_of_last_ids:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # iterate over all the data_objects in datas grabbing the last id
        for data_obj in datas:

            # handle the case that the data_obj is a ForecastMetaDataFrame
            if isinstance(data_obj, ForecastMetaDataFrame):
                # get the id of the last key in the model (the last column of ForecastMetaDataSeries to be added)
                last_id = data_obj.get_last_id()
                list_of_forecast_series.append(data_obj.get_last_series())
                list_of_ids.append(last_id)

            # handle the case that the data_obj is ForecastMetaDataSeries
            else:
                list_of_forecast_series.append(data_obj)
                list_of_ids.append(data_obj.get_id())

        return(list_of_ids, list_of_forecast_series)
    

    # _get_display_data_type
    # Go through a list of ForecastMetaDataSeries and ensure they all have the same display_type and data_type, returning the common values
    #  
    # INPUTS:
    #   list_of_forecast_series - List of ForecastMetaDataSeries to check
    # 
    # OUTPUTS:
    #   (display_type, data_type) - the common display_type and data_type found in the list

    @staticmethod
    def _get_display_data_type(list_of_forecast_series: list[ForecastMetaDataSeries]) -> tuple[ForecastDataSeriesMetaDataDataType, ForecastDataSeriesMetaDataDataType]: #(display_type, data_type) = ForecastMetaDataFrame._get_display_data_type(list_of_forecast_series)
        
        last_display_type = None
        last_data_type = None

        for series in list_of_forecast_series:
            if series.get_display_type() is not None:
                if last_display_type is None:
                    last_display_type = series.get_display_type()
                elif last_display_type != series.get_display_type():
                    raise ValueError(f"*  _get_display_data_type:  inconsistent display_types found in list_of_forecast_series, '{last_display_type}' != '{series.get_display_type()}'")
            
            if series.get_data_type() is not None:
                if last_data_type is None:
                    last_data_type = series.get_data_type()
                elif last_data_type != series.get_data_type():
                    raise ValueError(f"*  _get_display_data_type:  inconsistent data_types found in list_of_forecast_series, '{last_data_type}' != '{series.get_data_type()}'")

        # if we never found a display_type or data_type, default to FLOAT
        if last_display_type is None:
            last_display_type = ForecastDataSeriesMetaDataDataType.FLOAT
        if last_data_type is None:
            last_data_type = ForecastDataSeriesMetaDataDataType.FLOAT

        return(last_display_type, last_data_type)
    

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

        # check the meta-data
        for attrib in ForecastMetaDataFrameSchema:

            # check that all the forecast attributes match, otherwise, can't merge the two forecasts
            # ignore LAST_ID since that is not expected to match, ignore MODEL because it's not an attribute
            # TODO:  add the ability to do this for .model as well, currently cannot be done since MODEL is an attribute not a dict
            if(attrib != ForecastMetaDataFrameSchema.MODEL and 
               attrib != ForecastMetaDataFrameSchema.LAST_ID and
               attrib != ForecastMetaDataFrameSchema.LAST_VALUE_ID):
                if(frame1._meta_data[attrib] != frame2._meta_data[attrib]):
                    return False
                
        return True
    
    

    # _copy_frame_meta_data
    # Copy all the non-model meta-data from one frame to another
    #  
    # INPUTS:
    #   src_frame - source of meta-data
    #   dest_frame - destination for meta-data
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's meta-data added
    #
    # NOTE:  This function is only called ONCE as part of this object concat() function.  It's just is just to copy for _meta_data
    #        portion of the ForecastMetaDataFrame object from the src to the dest.  It does not handle the _model portion.  That
    #        part is handled by a different fuction: _append_cols
    @staticmethod
    def _copy_frame_meta_data(src_frame: Type['ForecastMetaDataFrame'], dest_frame: Type['ForecastMetaDataFrame']) -> Type['ForecastMetaDataFrame']:

        for attrib in ForecastMetaDataFrameSchema:

            # copy all the common attributes over except MODEL (which isn't an attrbute), LAST_ID and LAST_VALUE_ID
            if(attrib != ForecastMetaDataFrameSchema.MODEL and 
               attrib != ForecastMetaDataFrameSchema.LAST_ID and
               attrib != ForecastMetaDataFrameSchema.LAST_VALUE_ID):
                dest_frame._meta_data[attrib] = src_frame._meta_data[attrib]
            
            # DON'T NEED THIS
            # # ZIV:  potential BUG here... we don't have a use case where we APPEND the _model from the src to the destination we simply OVERWRITE
            # # is that the desired behavior, need to figure out what this function is used for
            # # handle MODEL differently because it's not a meta_data entry, it's a property of the object
            # elif(attrib == ForecastMetaDataFrameSchema.MODEL):
            #     if(dest_frame._model is not None):
            #         dest_frame._model = copy.deepcopy(src_frame._model)
            #     else:
            #         dest_frame._model = {}

        return dest_frame
    



    # _append_cols (PLURAL)
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
    def _append_cols(src_frame: Type['ForecastMetaDataFrame'], 
                     dest_frame: Type['ForecastMetaDataSeries'],
                     update_last_id = False,
                     verify_integrity: bool = False, 
                     drop_dups: bool = False) -> Type['ForecastMetaDataFrame']:
        
        # check if the src_frame has _model
        if(not src_frame.has_series()):
            return dest_frame
        
        # iterate over each column and add it
        for col_key in src_frame.get_series_ids():
            dest_frame = ForecastMetaDataFrame._append_col(src_series = src_frame.get_series(col_key), 
                                                           dest_frame = dest_frame, 
                                                           update_last_id = update_last_id, 
                                                           verify_integrity = verify_integrity, 
                                                           drop_dups = drop_dups)

        return dest_frame



    # _append_col (SINGLE)
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
    def _append_col(src_series: ForecastMetaDataSeries, dest_frame: Type['ForecastMetaDataFrame'], update_last_id = False, verify_integrity: bool = False, drop_dups: bool = False) -> Type['ForecastMetaDataFrame']:
        key = src_series.get_id()
        
        if(drop_dups or verify_integrity):
            key_exists  = dest_frame.has_series_id(key)

            if(drop_dups and key_exists):
                return dest_frame
            elif(verify_integrity and key_exists):
                raise ValueError(f"*  _append_col:  col '{key}' already exists in ForecastMetaDataFrame\n\n{dest_frame}\n\n{src_series}\n")
            
        dest_frame._model[key] = src_series
        
        if update_last_id:
            dest_frame.set_last_id(key)

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
            if(attrib != ForecastMetaDataFrameSchema.MODEL):    # this is done because MODEL is not a meta_data schema but on object attribute
                results += f"\n{attrib} = {self._meta_data[attrib]}"

        # iterate on all columns and print their meta-data
        if self.has_series():
            for col_key in self.get_series_ids():
                results += f"\n\nCol '{col_key}':\n{self.get_series(col_key)}"

        return results

    
    # to_json
    # Serialize this object to a JSON string
    #  
    # INPUTS:
    #   ident (optional: 4) - number of spaces to indent the JSON
    # 
    # OUTPUTS:
    #   JSON string

    def to_json(self, indent:int = 4) -> str:
        import json
        return json.dumps(self, default=ForecastMetaDataJsonSerializer, indent=indent)



    # to_Data
    # Converts this object to a Data object (with a single text column) for use in Langflow
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Data object with a single text column containing the JSON serialization of this object

    def to_Data(self) -> Data:
        import unicodedata
        import orjson

        def normalize_text(text):
            return unicodedata.normalize("NFKD", text)

        text = orjson.loads(self.to_json())

        if isinstance(text, dict):
            text = {k: normalize_text(v) if isinstance(v, str) else v for k, v in text.items()}
        elif isinstance(text, list):
            text = [normalize_text(item) if isinstance(item, str) else item for item in text]

        return Data(data=text)
    



# ForecastMetaDataJsonSerializer
def ForecastMetaDataJsonSerializer(obj):
    from langflow.schema.dataframe import DataFrame

    if isinstance(obj, ForecastMetaDataFrame):
        return ({"meta_data": obj._meta_data, "model": obj._model})
    elif isinstance(obj, ForecastMetaDataSeries):
        return obj._meta_data
    elif isinstance(obj, ForecastMetaDataRange):
        return obj._meta_data
    elif isinstance(obj, DataFrame):
        return obj.to_dict()
    elif isinstance(obj, pd.Series):
        return obj.to_list()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, Enum):
        return obj.value
    else:
        raise TypeError(f"Type {type(obj)} not serializable by ForecastMetaDataJsonSerializer")
    