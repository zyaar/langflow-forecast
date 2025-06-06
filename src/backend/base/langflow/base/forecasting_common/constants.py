#####################################################################
# constants.py
#
# Collection of constants that are shared in the forecasting implementation.
# 
#####################################################################


# COMMON IMPORTS
# ==============
from enum import Enum

# min year to start forecast (this is done because most timestamp values can't go further back than 1970s)
FORECAST_MIN_FORECAST_START_YEAR = 2000

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

FORECAST_INT_TO_SHORT_MONTH_NAME = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}

# Enum of supporting forecast types
class ForecastModelInputTypes(str, Enum):
    TIME_BASED = "Time Based Input"
    SINGLE_INPUT = "Single Input"


class ForecastModelTimescale(str, Enum):
    MONTH = "Month"
    YEAR = "Year"