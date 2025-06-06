from typing import List
import pandas as pd
import numpy as np
from uuid import UUID
from langflow.schema.dataframe import DataFrame, Data

from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.date_utils import gen_dates


# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
#from datetime import datetime
import numpy as np
import pandas as pd


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


      # EDITABLE_VALUES_TOKEN
      # ---------------------
      # This is used in the ForecastDataModel to indicate that a value in a cell (pandas dataframe) is editable by the person filling out the model, versus
      # the person creating the model.  The model player should recognize anytime they come across this token to make the particular cell editable.
      # NOTE VERY IMPORTANT:  In order for the code to work, it is important that any value used, is one that always reverts back to the TOKEN, when
      # being multiplied by other numbers.  For example:
      #     Zero can be the TOKEN because:  x * 0 = 0
      #     Nan can be the TOKEN because:  x * Nan = Nan
      # This simplified the development of the system because we could if I added an editable value to a non-editable value, I would always end up with an editable value

      #EDITABLE_VALUES_TOKEN = np.NAN # the value to enter in the data model for cells which will be updated after the model build
      EDITABLE_VALUES_TOKEN = 0.0 # the value to enter in the data model for cells which will be updated after the model build



      # FUNCTIONS
      # =========

      # generate_empty_forecast_data_model
      # Creates an "empty" forecast data model with just one row with the correct dates for the forecast.
      #  
      # INPUTS:
      #     start_year - the start year for the forecast
      #     num_years - number of years for the forecast
      #     input_type - is the forecast type a 'Time Based Input' or 'Single Input' (may be an issue if there are both of them)
      #     start_month - "The first month of the Fiscal year (if it's not January, otherwise January)"
      #     timescale - "Month" or "Year" resolution
      # 
      # OUTPUTS:
      #   DataFrame

      @staticmethod
      def generate_empty_forecast_data_model(
            start_year: int,
            num_years: int,
            start_month: int,
            timescale: ForecastModelTimescale.MONTH,                  
      ) -> DataFrame:
            
            # create a dates for this time series based on input values
            time_series_dates = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = num_years, timescale = timescale)
            return DataFrame(pd.DataFrame(data={ForecastDataModel.EDITABLE_VALUES_TOKEN: time_series_dates}))
      

      # init_forecast_data_model_single_series
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
      #   DataFrame

      @staticmethod
      def init_forecast_data_model_single_series(
            data: List[int|float],
            start_year: int,
            num_years: int,
            start_month: int,
            timescale: ForecastModelTimescale,
            series_name: str="",
      ) -> DataFrame:
            
            # create a dates for this time series based on input values
            time_series_dates = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = num_years, timescale = timescale)

            # bundle it and the series into a dictionary of series and create a DataFrame
            return DataFrame(data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_dates, series_name: data})
            

      # add_col_to_model
      # Add a new column to the data_model
      #  
      # INPUTS:
      #     data - the dataframe which will used as the basis
      #     new_col_values - list of floating points
      #     new_col_prefix - prefix to add to the unique id to indicate the function
      # 
      # OUTPUTS:
      #   DataFrame df which is Forecast Model compliant

      @staticmethod
      def add_col_to_model(data: DataFrame, new_col_values: List[float], new_col_name = "col_") -> DataFrame:
            df_new = pd.concat([data, pd.Series(data=new_col_values, name=new_col_name)], axis=1)
            return DataFrame(data=df_new)
      


      # concat_and_sum
      # Merge all the dataframes together (unique columns only) and add a new column to the data_model
      # with the sum of all the totals from all the dataframes
      #  
      # INPUTS:
      #     data - list of dataframe whose values will be added together
      #     new_col_name - the unique name for the column
      #     skip_total_if_one - boolean value, if true, do NOT create a totals column if only one dataframe in the list
      # 
      # OUTPUTS:
      #   DataFrame df which is Forecast Model compliant

      @staticmethod
      def concat_and_sum(datas: List[DataFrame], new_col_name = "Total", skip_total_if_one = True) -> DataFrame:
            # array holding all the totals columns that need to be added up
            total_cols = None

            # run the validation loop against all data sets to ensure they are valid, and grab the ids from
            # the last (i.e. total line) of each one
            for i in range(len(datas)):

                  # grab the last rightmost column (defined as the totals column) and put in a common dataframe
                  # if the rightmost column is the 'dates' column, it means the dataset is empty and can be ignored
                  if(datas[i].iloc[:, -1].name != ForecastDataModel.RESERVED_COLUMN_INDEX_NAME):
                        if(total_cols is None):
                              total_cols = np.array([datas[i].iloc[:, -1]])
                        else:
                              total_cols = np.concatenate([total_cols, [datas[i].iloc[:, -1]]], axis=0)
                  
                  # if second or later dataset, concat with first, but only add columns not found in first
                  if(i == 0):
                        combined_df = datas[i].copy()
                  else:
                        new_cols = list(set(datas[i].columns).difference(combined_df.columns))
                        combined_df = pd.concat([combined_df, datas[i][new_cols]], axis=1)

            # If 2 or more totals to add up, create a totals column, otherwise, skip it
            if(total_cols is None or np.shape(total_cols)[0] < 2):
                  pass
            else:
                  combined_df = ForecastDataModel.add_col_to_model(data = combined_df,
                                                                  new_col_values = np.sum(total_cols, axis=0, dtype = np.float64).flatten().tolist(),
                                                                  new_col_name = new_col_name)
            return(combined_df)



      # HELPER FUNCTIONS
      # ================
      
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
      


      # astype_first_all_cols
      # Helper function:  given either a DataFrame or List[dict] (provided by a TableInput), first convert the DataFrame, and then
      # set the types correct which is usually the first column is the Date column (type: datetime64), and the rest are all float columns
      #  
      # INPUTS:
      #     in_df - the input to be converted and typed
      #     first_col_type - the type of the first column (usually the Date column)
      #     rest_col_type - the types of all remaining columns (usually floats)
      # 
      # OUTPUTS:
      #   DataFrame with the columns correctly typed

      @staticmethod
      def astype_first_all_cols(in_df: DataFrame | List[dict], first_col_type: str = "datetime64[ns]", rest_col_type: str = "float") -> DataFrame:
            if not isinstance(in_df, DataFrame):
                  in_df = DataFrame(in_df)
            
            out_df = in_df.astype(dict.fromkeys(list(in_df.columns)[1:], rest_col_type))
            out_df = out_df.astype({out_df.columns[0]: first_col_type})
            return(out_df)
      