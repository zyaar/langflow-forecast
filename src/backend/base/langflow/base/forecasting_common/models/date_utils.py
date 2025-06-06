#####################################################################
# date_utils
#
# Collection of date functions that may be used in multiple classes:
# 
#####################################################################

# IMPORTS
# =======

from typing import List
import datetime as datetime
import pandas as pd
from langflow.base.forecasting_common.constants import ForecastModelTimescale


# FUNCTIONS
# =========

# gen_dates
#
# Generates a list of dates based on a from date (year and month) and a number of years out.  All dates are returned either
# fiscal year-end (assumes the month is the start of fiscal year) or month-end.
#
# INPUTS:
#   start_year = start year of the forecast
#   num_years: number of years out to set the list
#   start_month (optional): set the start month, used to supported fiscal years which do not start on a calendar year, default is: January
#   timescale (optional): set the granularity of the time series (monthly, yearly), default is: Yearly
# OUTPUTS:
#   List of pd.Timestamps with the year end of month end dates in the forecast
def gen_dates(start_year: int, num_years: int, start_month: int=1, time_scale: ForecastModelTimescale = ForecastModelTimescale.YEAR)-> List[datetime.datetime]:
    time_series = None

    # set the start date to be the last day BEFORE the start of the fiscal year (i.e. subtract one day from the start_date)
    # we do this because we want all of our forecasting numbers to be done in terms of the dates-end a time period
    # not the date start
    start_date = pd.to_datetime(f"{start_year:04}-{start_month:02}-01") - pd.Timedelta(days=1)
 
    # set the number of time periods (either num_years, or if TIMESCALE is monthly, then num_years * 12 months/year)
    num_periods = num_years if time_scale == ForecastModelTimescale.YEAR else num_years * 12

    # generate the time series
    # NOTE:  below, we set the periods fields to one more than our num_preiods.  This is to fix the fact
    # that when we subtracted one from the start_date to get the correct end_date for the forecasting,
    # we run into a problme where date_range assumes the the first period we want is that same as the start_date
    # we gave it (because it matches to the closes Month-End date).
    # Since that date happens ONE day BEFORE the start of the forecast, we don't want it, we want to first month-end
    # date the happens AFTER the time period (either 1 month or 12 months).  So we set "inclusive" = "neither", which
    # tells it to ignore the first date if that date matches the start_date.  This doesn't completely fix the problem,
    # since date_range simply tosses out the first date in the range (i.e. the one matching the start_date), but that
    # results in it return ONE LESS period that we need!
    # So, the account for that, we ask date_range to generate one MORE period that we need, knowing that we it
    # tosses the first period (the one matching start_date), we'll get exactly the right number of periods.
    if(time_scale == ForecastModelTimescale.MONTH):
        time_series = pd.date_range(start = start_date, periods=num_periods+1, freq="ME", inclusive = "neither")
    else:
        time_series = pd.date_range(start = start_date, periods=num_periods+1, freq="12ME", inclusive = "neither")

    return(time_series)
