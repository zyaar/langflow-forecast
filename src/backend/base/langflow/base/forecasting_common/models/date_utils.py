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
from dateutil.relativedelta import relativedelta
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



# conv_dates_monthly_to_yearly
#
# Given a forecast series of end-of-month dates, return the equivalent end-of-year dates 
#
# INPUTS:
#   start_year = start year of the forecast
#   num_years: number of years out to set the list
#   start_month (optional): set the start month, used to supported fiscal years which do not start on a calendar year, default is: January
#   timescale (optional): set the granularity of the time series (monthly, yearly), default is: Yearly
# OUTPUTS:
#   List of pd.Timestamps with the year end of month end dates in the forecast

def conv_dates_monthly_to_yearly(data: List[datetime.datetime] | pd.DatetimeIndex)-> List[pd.Timestamp]:
    # convert to list type if needed
    if(not isinstance(data, list)):
        data = data.to_list()

    # make sure that the total number of months is divisible by 12, otherwise throw an error
    if(len(data) % 12 != 0):
        raise ValueError(f"*   conv_dates_monthly_to_yearly:  Invalid data provided.  Number of elements in data must be a factor of 12, however, data has {len(data)} elements.")

    # if everthing is good, grab the 12th element in the list (lists are 0 indexed, so it's #11), and then grab every 12th element from that point forward to get the yearly dates
    new_dates = data[11::12]
    return(new_dates)



# conv_dates_yearly_to_monthly
#
# Given a forecast series of end-of-year dates, return the equivalent end_of_month dates 
#
# INPUTS:
#   start_year = start year of the forecast
#   num_years: number of years out to set the list
#   start_month (optional): set the start month, used to supported fiscal years which do not start on a calendar year, default is: January
#   timescale (optional): set the granularity of the time series (monthly, yearly), default is: Yearly
# OUTPUTS:
#   List of pd.Timestamps with the year end of month end dates in the forecast

def conv_dates_yearly_to_monthly(data: List[datetime.datetime] | pd.DatetimeIndex)-> List[datetime.datetime]:
    
    # convert to list type if needed
    if(not isinstance(data, list)):
        data = data.to_list()

    # Calculate the first month-end date based on the first year_end date
    min_date = min(data)
    max_date = max(data)
    num_years = len(data)

    # since we use year END dates, the min value needs to be dragged back 12 months and then add 1 day
    new_min_date = min_date - relativedelta(months=12)
    new_dates = pd.date_range(start = new_min_date, periods=(num_years*12)+1, freq="ME", inclusive = "neither").to_list()
    
    
    # if the new end_date is not the same as the old_end date, then we were probably given dates that were not evenly spaced by 12 months, so throw an error
    # if(max(new_dates) != max_date):
    #     raise ValueError(f"*   conv_dates_yearly_to_monthly:  Invalid data provided.  The end-of-year dates must be evenly spaced one year apart, this data is not: {data}")

    return(new_dates)



