# ==============
# COMMON IMPORTS
# ==============

import pandas as pd
import numpy as np

from enum import Enum
from typing import Type, Tuple, Any

#from .forecast_meta_data_serializer import FormatMetaDataObjectsJsonSerializer




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

# Enum of ACTION PRED TYPES
# Does the action take no preds, one pred max, two preds max, three or more preds max
# this is useful for determing when to add the last_id() in the factory for ForecastMetaDataContainer
class ForecastDataSeriesMetaDataActionPredTypes(str, Enum):
    NO_PREDS = "no_preds" # this action does not take any preds (i.e. DATES)
    ONE_PRED = "one_pred" # this action take ONE AND ONLY ONE pred (i.e. YEAR_TO_MONTH, MONTH_TO_YEAR)
    TWO_OR_MORE_PREDS = "two_or_more_preds" # this action takes AT LEAST TWO preds (i.e SUM, PROD, SUB, etc.)


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

FORECAST_READ_ONLY_VALIDATION = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}]
