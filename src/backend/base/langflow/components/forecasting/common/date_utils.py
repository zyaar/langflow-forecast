#####################################################################
# date_utils
#
# Collection of date functions that may be used in multiple classes:
# 
#####################################################################

# IMPORTS
# =======

from typing import List
import pandas as pd
from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecatModelTimescale


# FUNCTIONS
# =========

# gen_dates
#
# Generates a list of dates based on a from date (year and month) and a number of years out.  All dates are returned either
# year-end or month-end.
#
# INPUTS:
#   start_year = start year of the forecast
#   num_years: number of years out to set the list
#   start_month (optional): set the start month, used to supported fiscal years which do not start on a calendar year, default is: January
#   timescale (optional): set the granularity of the time series (monthly, yearly), default is: Yearly
# OUTPUTS:
#   List of pd.Timestamps with the year end of month end dates in the forecast
def gen_dates(start_year: int, num_years: int, start_month: int=1, timescale: ForecatModelTimescale = ForecatModelTimescale.YEAR)-> List[pd.Timestamp]:
    start_date = f"{start_year:04}-{start_month:02}-01"
    time_series = None

    # set the number of time periods to create to the number of years to add (we'll adjust later based on the timescale)
    num_periods = num_years

    # set the frequency (timescale) of the dates
    if(timescale == ForecatModelTimescale.MONTH):
        num_periods = num_periods * 12
        time_series = pd.date_range(start_date, periods=num_periods, freq="ME")

    # default to years
    else:
        time_series = pd.date_range(start_date, periods=num_periods, freq="YE")

    return(time_series)
        

    

    

    