from typing import List
import pandas as pd
from langflow.schema.data import Data
from langflow.schema.dataframe import DataFrame

from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.date_utils import gen_dates


# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastDataModel
# A single static class which centralizes all functions necessary to ensure that a Langflow DataFrame (which inherits from Pandas Dataframe) has everything necessary
# to work as our ForecastDataModel
class ForecastDataModel(DataFrame):
      RESERVED_COLUMN_INDEX_NAME = "dates" # name of the dates column for the forecasting model.  This must be a unique column
      REQ_FORECAST_MODEL_ATTR_NAMES = ["start_year", "num_years", "input_type", "start_month", "timescale"]
      REQ_FORECAST_MODEL_ATTR_TYPES = [int, int, ForecastModelInputTypes, int, ForecastModelTimescale]
      REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES = ["Start Year", "Number of Years", "Input Type", "Start Month", "Time-scale"]



      # FUNCTIONS
      # =========
      
      # gen_forecast_dates
      # Creates a series of dates which or compatible with the Forecast Data Model given all the standard inputs.  Added this function
      # to centralize date creation in this static class
      #  
      # INPUTS:
      #     start_year - the start year for the forecast
      #     num_years - number of years for the forecast
      #     start_month - the month of the start of a fiscal year
      #     timescale - wether to each date covers a year or a month 
      # 
      # OUTPUTS:
      #   List of pd.Timestamps with the correct dates for the requested Forecast Data model

      @staticmethod
      def gen_forecast_dates(
            start_year: int, 
            num_years: int, 
            start_month: int=1, 
            timescale: ForecastModelTimescale = ForecastModelTimescale.YEAR,
      ) -> List[pd.Timestamp]:
            return(gen_dates(start_year=start_year, num_years=num_years, start_month=start_month, time_scale=timescale))


      # init_forecast_data_model
      # The simplest way to create a Data Forecast Model.  Give it one series od data (ints or floats)Given a list of ints or floats for the first series, creates a Forecast Data Model compliant dataframe by generating the dates, adding the 
      # specific meta-data attributes and data structures to work as the basis of the forecast
      #  
      # INPUTS:
      #     data - a list of int or floats (first series)
      #     start_year - the start year for the forecast
      #     num_years - number of years for the forecast
      #     input_type - is the forecast type a 'Time Based Input' or 'Single Input' (may be an issue if there are both of them)
      #     start_month - 
      # 
      # OUTPUTS:
      #   DataFrame df_out

      @staticmethod
      def init_forecast_data_model_single_series(
            data: List[int|float],
            start_year: int,
            num_years: int,
            input_type: 'ForecastModelInputTypes',
            start_month: int,
            timescale: ForecastModelTimescale,
            series_name: str="",
      ) -> DataFrame:
            
            # create a dates for this time series based on input values
            time_series_dates = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = num_years, timescale = ForecastModelTimescale.YEAR)

            # bundle it and the series into a dictionary of series and create a DataFrame
            forecast_model = DataFrame({ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_dates, series_name: data})

            # return with the Forecast variables added
            return ForecastDataModel.init_forecast_data_model(forecast_model,
                                                              start_year=start_year,
                                                              num_years=num_years,
                                                              input_type=input_type,
                                                              start_month=start_month,
                                                              timescale=timescale)


      # init_forecast_data_model
      # Add the ForecastDataModel specific meta-data attributes and data structures to a generic DataFrame in order for it to work as the basis of the forecast
      #  
      # INPUTS:
      #     data - the dataframe which will used as the basis
      #     start_year - the start year for the forecast
      #     num_years - number of years for the forecast
      #     input_type - is the forecast type a 'Time Based Input' or 'Single Input' (may be an issue if there are both of them)
      #     start_month - 
      # 
      # OUTPUTS:
      #   DataFrame df_out

      @staticmethod
      def init_forecast_data_model(
            data: DataFrame,
            start_year: int,
            num_years: int,
            input_type: 'ForecastModelInputTypes',
            start_month: int,
            timescale: ForecastModelTimescale,
      ) -> DataFrame:
            
            # if it's not already a dataframe, then create a dataframe
            if(not isinstance(data, DataFrame)):
                  df_out = DataFrame(data=data)
            else:
                  df_out = data
            
            # if there is no legitimate index then set it to the RESERVED_COLUMN_INDEX
            if(type(df_out.index) is not pd.DatetimeIndex):
                  if((ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in df_out.columns) and (type(df_out[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME]) is not pd.DatetimeIndex)):
                        df_out.set_index(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, inplace=True)
                  else:
                        raise ValueError(f"Index is not a DatetimeIndex and missing DatetimeIndex column called '{ForecastDataModel.RESERVED_COLUMN_INDEX_NAME}'")            

            # set-up the additional forecasting-specific attributes inside pandas attributes (where they should be preserved)
            df_out.attrs["start_year"] = start_year
            df_out.attrs["num_years"] = num_years
            df_out.attrs["input_type"] = input_type
            df_out.attrs["start_month"] = start_month
            df_out.attrs["timescale"] = timescale

            return(df_out)

      

      # recalculate_forecast_data_model
      # Recalculate the fields that can be updated, especially after data transformations. 
      #  
      # INPUTS:
      #     DataFrame data
      # 
      # OUTPUTS:
      #     DataFrame df_out

      @staticmethod
      def recalculate_forecast_data_model_attributes(data: DataFrame) -> DataFrame:
            # make a shallow copy of df, may be revisited later      
            df_out = data

            # recalcuate whatever fields can be recalculated based on existing data
            # start_year
            df_out.attrs["start_year"] = ForecastDataModel.gen_start_year(df_out)  # start_year
            df_out.attrs["num_years"] = ForecastDataModel.gen_num_years(df_out)    # num_years
            df_out.attrs["start_month"] = ForecastDataModel.start_month(df_out)    # start_month

            return(df_out)

            
      # recalculate_forecast_data_model
      # Recalculate the fields that can be updated, especially after data transformations. 
      #  
      # INPUTS:
      #     DataFrame data
      # 
      # OUTPUTS:
      #     DataFrame df_out

      @staticmethod
      def is_valid_forecast_data_model(data) -> bool:
            # check that it's the right class
            if(not isinstance(data, DataFrame)):
                  return(False)
            
            # check it has an index that is a DatetimeIndex
            if(type(data.index) is not pd.DatetimeIndex):
                  return(False)
            
            # for each required attribute
            for i in range(len(ForecastDataModel.REQ_FORECAST_MODEL_ATTR_NAMES)):
                  # check that an attribute with that name exists in the pandas attributes dictionary
                  if(ForecastDataModel.REQ_FORECAST_MODEL_ATTR_NAMES[i] not in data.attrs):
                        return(False)
                  
                  # check that the attribute is the right type
                  if(not isinstance(data.attrs[ForecastDataModel.REQ_FORECAST_MODEL_ATTR_NAMES[i]], ForecastDataModel.REQ_FORECAST_MODEL_ATTR_TYPES[i])):
                        return(False)
                  
            return(True)

            
      # upsample_year_to_month
      # Convert a DateFrame set at a yearly frequency to one set at
      # a monthly frequency.  Divide all yearly values evenly across the months
      #  
      # INPUTS:
      #     DataFrame data
      # 
      # OUTPUTS:
      #   DataFrame df_out
      @staticmethod
      def upsample_yearly_to_monthly(data: DataFrame) -> DataFrame:
            
            # if this is already a MONTHLY timescale then return it
            if(data.attrs["timescale"] == ForecastModelTimescale.MONTH):
                  return(data)
            
            # since all dates are YEAR-END, before we resample we have to add a new row with zero values and a date 
            # for the year-end before the earliest year-end date in the current dataset.
            # We do this, because, when resampling, pandas always stars with the earliest dates IN THE DATASET
            # and samples from there.  So if we have a YEAR-END date, we need it to create the 12 months PRIOR to the
            # first date... which means it needs an earlier date to start from (i.e. the previous year end date).
            # If we don't do this, pandas creates month end dates starting with our first year-end.
            # (someone who knows pandas better might have a better way to do this)
            
            # get the previous year end date, create a new row with all zeros and that date, and append to dataframe
            new_min_date = min(data.index) - pd.DateOffset(years=1)
            new_row = pd.DataFrame(data = [0] * len(data.columns), index=pd.DatetimeIndex([new_min_date]))
            new_row.columns = data.columns
            new_row = DataFrame(data=new_row)
            df_concat = pd.concat([data, new_row])
            
            # resample for ME, backfill all the NA values being generated with the annual number, and then divide by 12 (number months in a year)
            # this will spread the YEAR-END number evenly across all previous months in that year
            df_out = df_concat.resample("ME").bfill().div(12)
            
            # finally get rid of the previous year end date (it's not part of the results, and it has a zero value in it)
            df_out = df_out.loc[df_out.index > min(df_out.index)]
            df_out.attrs["timescale"] = ForecastModelTimescale.YEAR

            return(df_out)
      
      
      # downsample_monthly_to_yearly
      # Convert a ForecastDataModel set at a monthly frequency to one set at
      # a yearly frequency.  Sums all the monthly values at a yearly level
      # 
      # INPUTS:
      #   DataFrame set to 
      #
      # OUTPUTS:
      #   ForecastDataModel set to yearly frequency
      @staticmethod
      def downsample_monthly_to_yearly(data: DataFrame) -> DataFrame:
            end_of_month = data.attrs["start_month"]-1 if data.attrs["start_month"] != 1 else 12
            period = str("YE-"+ FORECAST_INT_TO_SHORT_MONTH_NAME[end_of_month])
            df_out = data.resample(period).sum()
            return(df_out)
    


      # combine_forecast_model
      # Takes a list of DataFrames and combines them into a single dataframe with a
      # total which is a sum of all the incoming ones.  Two rules which are applied:
      # 1. Timescale - If there are multiple timescale granualarities, always go for the greatest one (i.e. MONTHLY over YEARLY)
      # 2. 
      # 
      # INPUTS:
      #   List of ForecastDataModel
      #
      # OUTPUTS:
      #   ForecastDataModel
      @staticmethod
      def combine_forecast_models(datas: List[DataFrame], agg_col_funct="", agg_col_name="Total_", use_as_prefix=True) -> DataFrame:
            multiple_start_year = False
            multiple_num_year = False
            multiple_input_type = False
            multiple_start_month = False
            multiple_timescale = False

            df_all_totals_col = pd.DataFrame()

            # if only one forecast model provided, return that one
            if(len(datas) == 1):
                  return(datas[0])
            
            # validate that all DataFrames are valid for using as forecast data models
            for i in range(len(datas)):
                  if(not ForecastDataModel.is_valid_forecast_data_model(datas[i])):
                        raise ValueError(f"Unable to combine forecast models.  In the list of inputs, #{i} is not a valid forecast model.")
                  
                  if(i != 0):
                        if(datas[i].attrs["start_year"] != start_year):
                              multiple_start_year = True
                        
                        if(datas[i].attrs["num_years"] != num_years):
                              multiple_num_year = True

                        if(datas[i].attrs["input_type"] != input_type):
                              multiple_input_type = True

                        if(datas[i].attrs["start_month"] != start_month):
                              multiple_start_month = True

                        if(datas[i].attrs["timescale"] != timescale):
                              multiple_timescale = True

                  # copy the current batch of values into our variables to compare to the
                  # next batch
                  start_year = datas[i].attrs["start_year"]
                  num_years = datas[i].attrs["num_years"]
                  input_type = datas[i].attrs["input_type"]
                  start_month = datas[i].attrs["start_month"]
                  timescale = datas[i].attrs["timescale"]

                  # grab the last rightmost column (defined as the totals column) and put in a common dataframe
                  df_all_totals_col = pd.concat([df_all_totals_col, datas[i][datas[i].columns[-1]]], axis=1)


            # Check if there are any issues with multiples.  We have to rase an error for anything OTHER
            # than multiple timescales (which we can address by changing everything to months)
            errMsg = ""

            if(multiple_start_year):
                  errMsg += f"\nDifferent values for {ForecastDataModel.REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES[0]}."

            if(multiple_num_year):
                  errMsg += f"\nDifferent values for {ForecastDataModel.REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES[1]}."
                              
            if(multiple_input_type):
                  errMsg += f"\nDifferent values for {ForecastDataModel.REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES[2]}."
                              
            if(multiple_start_month):
                  errMsg += f"\nDifferent values for {ForecastDataModel.REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES[3]}."
                              
            if(multiple_timescale):
                  errMsg += f"\nDifferent values for {ForecastDataModel.REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES[4]}."

            if(errMsg != ""):
                  raise ValueError(f"Error merging multiple Forecast Models: {errMsg}")
         
            # combine the dataframes
            combined_pd = pd.concat(datas, axis=1)

            # run an (optional) aggregation function on all the "Total" columns of the Forcast Data Models provided
            # (defined as the rightmost column in each of the DataFrames)
            if(agg_col_funct != ""):
                  if(agg_col_name == ""):
                        raise ValueError(f"Agg_col_funct '{agg_col_funct}' provided, but invalid agg_call_name '{agg_col_name}'.")
                  
                  match agg_col_funct:
                        case "sum":
                              df_new_agg_col = df_all_totals_col.sum(axis=1)
                        case _:
                              raise ValueError(f"Invalid aggregation function called when merging multiple Forecast Models: {agg_col_funct}")
                        
                  # set-up the agg_col_name, if it's be used as a prefix, generate a unique name by combining all the totals column names
                  # and separating with a "-"
                  if(use_as_prefix):
                        agg_col_name = str(agg_col_name + "-".join(df_all_totals_col.columns))
                  
                  # before aggregating, need to convert it into a DataFrame with the correct Forecast Build Model, so that the concat won't wipe this out
                  df_new_agg_col = DataFrame(data=df_new_agg_col.to_frame(name=agg_col_name).set_index(combined_pd.index))
                  df_new_agg_col = ForecastDataModel.init_forecast_data_model(df_new_agg_col,
                                                                              start_year = combined_pd.attrs["start_year"],
                                                                              num_years = combined_pd.attrs["num_years"],
                                                                              input_type = combined_pd.attrs["input_type"],
                                                                              start_month = combined_pd.attrs["start_month"],
                                                                              timescale = combined_pd.attrs["timescale"])
                  combined_pd = pd.concat([combined_pd, df_new_agg_col], axis=1)


            # validate that this is a legitimate forecast
            if(not ForecastDataModel.is_valid_forecast_data_model(combined_pd)):
                  raise ValueError(f"Error merging multiple Forecast Models.  Resulting model is not a valid forecast model.")
            
            return(combined_pd)



      # HELPER FUNCTIONS
      # ================
      
      # start_year
      @staticmethod
      def gen_start_year(df: DataFrame) -> int:
           # figure out start_year
            start_year = min(df.index).dt.year
            return(start_year)
      
      # gen_num_years
      @staticmethod
      def gen_num_years(df: DataFrame) -> int:
            # figure out the num_years
            num_years = max(df.index).dt.year - min(df.index).dt.year
            return(num_years)

      # gen_start_month
      @staticmethod
      def gen_start_month(df: DataFrame) -> int:
            # figure out the start_month
            start_month = min(df.index).dt.month + 1
            return(start_month)