#####################################################################
# constants.py
#
# Collection of constants that are shared in the forecasting implementation.
# 
#####################################################################


# COMMON IMPORTS
# ==============
from enum import Enum



# List of month names and values
FORECAST_COMMON_MONTH_NAMES_AND_VALUES = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

# Enum of supporting forecast types
class ForecastModelInputTypes(str, Enum):
    TIME_BASED = "Time Based Input"
    SINGLE_INPUT = "Single Input"

# # Columns in the forecasting model to create in Pandas dataframe
# FORECAST_COMMON_FORECAST_MODEL_COLUMNS = [
#     "timestamp",
#     "volume"
# ]

class ForecatModelTimescale(str, Enum):
    MONTH = "Month"
    YEAR = "Year"


# # Columns in the forecasting model to create in Pandas dataframe
# FORECAST_MODEL_DATAFRAME_COLUMNS = [
#     "timestamp",
#     "volume"
# ]
