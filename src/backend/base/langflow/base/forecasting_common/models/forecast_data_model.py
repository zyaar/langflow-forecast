from typing import List, Tuple
import pandas as pd
import numpy as np
import nanoid
import copy
from langflow.schema.dataframe import DataFrame, Data

from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeries,
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema,
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)
from langflow.base.forecasting_common.models.date_utils import gen_dates, gen_pre_dates, conv_dates_monthly_to_yearly, conv_dates_yearly_to_monthly




# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
import re
import numpy as np
import pandas as pd
import datetime as datetime



# CONSTANTS
# =========



# CLASSES
# =======

# ForecastDataModel
# A single static class which centralizes all functions necessary to ensure that a Langflow DataFrame (which inherits from Pandas Dataframe) has everything necessary
# to work as our ForecastDataModel
class ForecastDataModel(DataFrame):

      # Model DataFrame column names
      RESERVED_COLUMN_INDEX_NAME = "dates" # name of the dates column for the forecasting model.  This must be a unique column
      PATIENT_PROGRESSION_COLUMN_NAME = "patient_progression" # name of the column which holds patient progression for a treatment

      # Forecast attributes
      REQ_FORECAST_MODEL_ATTR_NAMES = ["start_year", "num_years", "input_type", "start_month", "timescale"]
      REQ_FORECAST_MODEL_ATTR_TYPES = [int, int, ForecastModelInputTypes, int, ForecastModelTimescale]
      REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES = ["Start Year", "Number of Years", "Input Type", "Start Month", "Time-scale"]


      # TREATMENT attributes
      TREATMENT_PAT_TOTAL_BY_MONTH = "total_patients_on_treatment_by_treatment_month"
      TREATMENT_PAT_LEAVING_BY_MONTH = "total_patients_leaving_treatment_by_treatment_month"

      TREATMENT_PRODUCT_RX_BY_MONTH = "total_product_rxs_by_treatment_month"



      # EDITABLE_VALUES_TOKEN
      # ---------------------
      # This is used in the ForecastDataModel to indicate that a value in a cell (pandas dataframe) is editable by the person filling out the model in the model player, versus
      # the person creating the model (using this tool).  The model player should recognize anytime they come across this token to make the particular cell editable.
      #
      # NOTE VERY IMPORTANT:  In order for the code to work, it is important that any value used, is one that always reverts back to the TOKEN, when
      # combined arithmetically with any other values (artithmetically = +, -, *, /).  For example:
      #     Nan can be the TOKEN because:  x * Nan = Nan, x + Nan = Nan, etc.
      #     pd.NA can be a TOKEN because: x * pd.NA = pd.NA, x / pd.NA = pd.NA, etc.
      #     pd.NAT can be a TOKEN (datetime only) because:  x * pd.NAT = pd.NAT, etc.
      #
      # This simplified the development of the system because the TOKEN gives us a free implementation of a "dirty bit", meaning it automatically provides the system with an
      # understanding with what cells in the model player cannot be calculated ahead of time (i.e. dirty) because they are dependent on variables that will be entered by the 
      # user of the model player, and what cells can be calculated and will be read-only and unchangeable having been set by the modeler in the langflow tool (i.e. not dirty) instead
      # of us having to create and track dirty bits in the code manually.
      #
      # IT IS RECOMMENDED THAT THIS VALUE BE SET TO pd.NA (which is pandas placeholder for missing values but also provide protection for preserving different data types, 
      # but this has not been tested yet)


      #EDITABLE_VALUES_TOKEN = pd.NA # the value to enter in the data model for cells which will be updated after the model build
      EDITABLE_VALUES_TOKEN = 0.0 # TODO:  kept as 0.0 for now to keep dev going, but need to go back and change to pd.NA and then rerun to make sure this works as expected



      # FUNCTIONS
      # =========


      # =====================
      # CREATE FORECAST MODEL
      # =====================

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
            return DataFrame(pd.DataFrame(data = {ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_dates}))
      

      

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
            series_name: str="") -> DataFrame:
            
            # create a dates for this time series based on input values
            time_series_dates = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = num_years, timescale = timescale)

            # bundle it and the series into a dictionary of series and create a DataFrame
            return DataFrame(data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_dates, series_name: data})
      
            

      # ==========
      # CONVERSION
      # ==========


      # to_pandas
      # Convenience function to convert a Langflow DataFrame to a Pandas DataFrame checking first
      @staticmethod
      def to_pandas(df : DataFrame | pd.DataFrame) -> pd.DataFrame:
            if(isinstance(df, DataFrame)):
                  return pd.DataFrame(df)
            else:
                  return df


      # to_langflow
      # Convenience function to convert a Pandas DataFrame to a Langflow DataFrame checking first
      @staticmethod
      def to_langflow(df : DataFrame | pd.DataFrame) -> DataFrame:
            if(isinstance(df, DataFrame)):
                  return df
            
            return DataFrame(df)



      # ======================
      # DATAFRAME MANIPULATION
      # ======================

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



      # concat
      # Merge all the dataframes together (unique columns only)
      #  
      # INPUTS:
      #     data - list of dataframe whose values will be added together
      # 
      # OUTPUTS:
      #   DataFrame df which is Forecast Model compliant

      @staticmethod
      def concat(datas: List[DataFrame]) -> DataFrame:
            if(len(datas) < 1):
                  raise ValueError(f"*  concat:  error, empty list of datasets provided.")

            # if we only have one dataset and the flag to skip generating total col if only one is True,
            # just return the existing dataset
            if(len(datas) == 1):
                  return(datas[0])

            for i in range(len(datas)):
                  # if second or later dataset, concat with first, but only add columns not found in first
                  if(i == 0):
                        combined_df = datas[i].copy()
                  else:
                        new_cols = [colname for colname in datas[i].columns if colname not in combined_df.columns]
                        combined_df = pd.concat([combined_df, datas[i][new_cols]], axis=1)             
            return(combined_df)



      # concat_and_sum
      # Merge all the dataframes together (unique columns only) and add a new column to the data_model
      # with the sum of all the totals from all the dataframes
      #  
      # INPUTS:
      #     data - list of dataframe whose values will be added together
      #     drop_dups - drop duplicate column names
      #     skip_total_if_one - boolean value, if true, do NOT create a totals column if only one dataframe in the list
      # 
      # OUTPUTS:
      #   total_line_create = True if a new total line was created, false if not
      #   DataFrame df which is Forecast Model compliant
      #   If a new total line was created, the ID of the SUMMATION, to be used as part of Meta_Data setup
      #   The id of the actual total calculation line created (usually:  SUMMATION_ID+"_Total")

      @staticmethod
      def concat_and_sum(datas: List[DataFrame], drop_dups: bool = True, skip_total_if_one: bool = True) -> tuple[bool, DataFrame, str, str]:

            if(len(datas) < 1):
                  raise ValueError(f"*  concat_and_sum:  error, empty list of datasets provided.")

            # if we only have one dataset and the flag to skip generating total col if only one is True,
            # just return the existing dataset
            if(len(datas) == 1 and skip_total_if_one):
                  last_col_id = datas[0].columns[-1]
                  return(False, datas[0], None, last_col_id)
            
            # if more than one datas provided, we will need to create a totals line here, and a summation step
            # in the meta data (both Init and Sum), so the make things easier, create a custom ID name that has
            # SUMMATION in it
            new_col_id = f"SummationTB_{nanoid.generate(size=5)}"

            # array holding all the totals columns that need to be added up
            total_cols = None
            total_cols_names = []
            new_total_col_id = f"{new_col_id}_Total"

            # run the validation loop against all data sets to ensure they are valid, and grab the ids from
            # the last (i.e. total line) of each one
            for i in range(len(datas)):
                  total_col_name = datas[i].columns[-1]
                  total_col_values = datas[i][total_col_name].to_numpy()

                  # grab the last rightmost column (defined as the totals column) and put in a common dataframe
                  # if the rightmost column is the 'dates' column, it means the dataset is empty and can be ignored
                  if(total_col_name != ForecastDataModel.RESERVED_COLUMN_INDEX_NAME):

                        # if this is the first time we're adding a column to total_cols, then add it in
                        if(total_cols is None):
                              total_cols = total_col_values
                              total_cols_names.append(total_col_name)

                        # if this is NOT the first time, then append (concat) to the the rest of the totals
                        # columns
                        else:
                              # WEIRD EDGE CASE TO HANDLE:  if the name of the totals column for one of the dataframes being
                              # concatenated already exists in the df_combined, raise an error and stop (we may change
                              # this in the future if there is some good reason to allow this)
                              if(total_col_name in total_cols_names):
                                    raise ValueError(f"*  concat_and_sum:  error, duplicate total_column names, trying to add: '{total_col_name}', to: {total_cols_names}")

                              total_cols = total_cols + total_col_values
                  
                  # if second or later dataset, concat with first, but only add columns not found in first
                  if(i == 0):
                        combined_df = datas[i].copy()
                  else:
                        new_cols = [colname for colname in datas[i].columns if colname not in combined_df.columns]
                        combined_df = pd.concat([combined_df, datas[i][new_cols]], axis=1)

            # append Totals column to dataframe
            combined_df = ForecastDataModel.add_col_to_model(data = combined_df,
                                                                    new_col_values = total_cols.tolist(),
                                                                    new_col_name = new_total_col_id)
            
            return(True, combined_df, new_col_id, new_total_col_id)
      


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
      



      # =================
      # DATE MANIPULATION
      # =================
      
      

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
      ) -> List[datetime.datetime]:
            return(gen_dates(start_year = start_year, num_years = num_years, start_month = start_month, time_scale = timescale))
      


      # conv_dates_monthly_to_yearly
      #
      # PASS THROUGH FROM DATE_UTILS:  Given a forecast series of end-of-month dates, return the equivalent end-of-year dates 
      #
      # INPUTS:
      #   start_year = start year of the forecast
      #   num_years: number of years out to set the list
      #   start_month (optional): set the start month, used to supported fiscal years which do not start on a calendar year, default is: January
      #   timescale (optional): set the granularity of the time series (monthly, yearly), default is: Yearly
      # OUTPUTS:
      #   List of pd.Timestamps with the year end of month end dates in the forecast

      @staticmethod
      def conv_forecast_dates_monthly_to_yearly(data: List[datetime.datetime] | pd.DatetimeIndex)-> List[pd.Timestamp]:
            return conv_dates_monthly_to_yearly(data = data)

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

      @staticmethod
      def conv_forecast_dates_yearly_to_monthly(data: List[datetime.datetime] | pd.DatetimeIndex)-> List[datetime.datetime]:
            return conv_dates_yearly_to_monthly(data = data)


      # gen_pre_dates
      #
      # Generate a list of dates given a first date, a number of periods to go BACK from that date, and a definition of the length of a period (YEAR or MONTH)
      #
      # INPUTS:
      #   first_forecase date = earliest forecast date in the forecast date series
      #   num_periods: number of periods to go back in generating pre-forecast-dates
      #   time_scale (optional): the length of a time period (monthly or yearly), default is: Yearly
      # OUTPUTS:
      #   List of datetimes with the year end of month end dates prior to the forecast

      @ staticmethod
      def gen_pre_dates(
            first_forecast_date:  datetime.datetime, 
            num_periods: int, 
            time_scale: ForecastModelTimescale = ForecastModelTimescale.YEAR
      ) -> List[datetime.datetime]:
            return(gen_pre_dates(first_forecast_date = first_forecast_date, num_periods = num_periods, time_scale = time_scale))



      # yearly_to_monthly
      # Helper function:  given a pd.Series which is assumed to be YEARLY, convert it to monthly time series
      # by taking the annual values and dividing them by twelve and spreading that over over 12 columns
      #  
      # INPUTS:
      #     pd.Series of Yearly numbers (only supports integers and floats)
      # OUTPUTS:
      #   pd.Series - a series with the MONTHLY numbers that align to those same yearly numbers

      @staticmethod
      def yearly_to_monthly(data: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
            # pd.Series version:
            # simplest way to do this which is not dependent on having a datetime for an index is simply repeat
            # the data series 12 times (number of months in a year) and then divide all the values by 12
            if(isinstance(data, pd.Series)):
                  # make sure these are integer or float columns, otherwise throw an error
                  if(not pd.api.types.is_integer_dtype(data) and not pd.api.types.is_float_dtype(data)):
                        raise ValueError(f"*  yearly_to_monthly:  Invalid dtype for pd.Series = {data.dtype}.  Only integer and float supported.")

                  data_out = data.repeat(12)/12

            # pd.DateFrame version:
            # same as above, however, has to handle multiple integer and/or float columns.  Has to also handle the one Date column
            # we can have (at the very beginning)
            else:
                  has_date_col = False

                  # if there is a date column, remove it, handle it separately, ahead of the expansion to monthly
                  if(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in data.columns):
                        has_date_col = True
                        new_date_col = conv_dates_yearly_to_monthly(data = data[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME])
                        data = data.drop(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, axis=1)

                  data_out = data.iloc[np.repeat(np.arange(len(data)), 12)]/12

                  # if there was a date col, add the newly handled dates back in
                  if(has_date_col):
                        data_out.insert(0, ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, new_date_col)

            # finally, regardless of pd.Series or pd.Dataframe, we regenerate the index the same way
            data_out.index = list(range(len(data_out)))
            return(data_out)
      


      # monthly_to_yearly
      # Helper function:  given a pd.Series or pd.DataFrame which is assumed to be MONTHLY, convert it to yearly time series
      # by summing up every 12 months into a single value
      #  
      # INPUTS:
      #     pd.Series or pd.DataFrame of Monthly numbers (no Date index needed)
      # OUTPUTS:
      #   pd.Series - a series with the Yearly numbers that align to those same monthly numbers

      @staticmethod
      def monthly_to_yearly(data: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
            has_date_col = False

            

            # we don't deal with datetime indexes in this function, but we do work with 1 index instead of 0 index (for months and years)
            data.index = list(range(0, len(data)))

            # Special code for a DataFrame to handle the Date column
            if(isinstance(data, pd.DataFrame) and ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in data.columns):
                  has_date_col = True
                  new_date_col = conv_dates_monthly_to_yearly(data = data[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME])
                  data = data.drop(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, axis=1)

            # simplest way to do this is to group by every 12 units of index and sum up the values.
            # however, if the index does not start at zero, need to make the index zero to line up the months with the years
            data_out = data.groupby((data.index-min(data.index)) // 12).sum()
            data_out.index = list(range(len(data_out)))

            # Special code if it is a DataFrame WITH the Date column, to add it back in
            if(isinstance(data, pd.DataFrame) and has_date_col):
                  data_out.insert(0, ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, new_date_col)

            return(data_out)


      @staticmethod
      def convert_timescale(data_model: pd.DataFrame, 
                            meta_data: ForecastMetaDataFrame, 
                            target = ForecastModelTimescale, 
                            step_type: ForecastDataSeriesMetaDataStepTypes = None) -> tuple[pd.DataFrame, ForecastMetaDataFrame, str]:
            
            # if timescale is already same as target, throw an error
            if meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE] == target:
                  raise ValueError(f"\n*  convert_monthly_to_yearly:  error, current data_model is already {target}.")

            data_model = copy.deepcopy(data_model)
            meta_data = copy.deepcopy(meta_data)

            # determine the number of num_years for date generation:
            # if the target is YEARLY, that means we are monthly, so take num_periods /12 for num_years
            if(target == ForecastModelTimescale.YEAR):
                  new_dates = conv_dates_monthly_to_yearly(data = data_model[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME])

            # else num_years = num_periods
            else:
                  new_dates = conv_dates_yearly_to_monthly(data = data_model[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME])


            # setup
            last_id = data_model.columns[-1]
            last_display_name = meta_data.model[last_id].meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]


            if step_type is None:
                  step_type = meta_data.get_last_step_type()
                        
            # remove the old date column names
            if(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in data_model.columns):
                  data_model = data_model.drop(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, axis = 1)


            # do the conversion
            if(target == ForecastModelTimescale.MONTH):
                  new_id = f"{last_id}_Yearly_to_Monthly"
                  new_display_name = f"From '{last_display_name}' (converted from years to months)"
                  data_model = ForecastDataModel.yearly_to_monthly(data_model)
                  action = ForecastDataSeriesMetaDataAction.YEAR_TO_MONTH
            else:
                  new_id = f"{last_id}_Monthly_to_Yearly"
                  new_display_name = f"From '{last_display_name}' (converted from months to years)"
                  data_model = ForecastDataModel.monthly_to_yearly(data_model)
                  action = ForecastDataSeriesMetaDataAction.MONTH_TO_YEAR

            data_model[new_id] = data_model[last_id]
            
            meta_data = meta_data.add_col_meta_data(frame = meta_data,
                                                    id = new_id,
                                                    step_type = step_type,
                                                    action = action,
                                                    data_type = meta_data.get_last_data_type(),
                                                    display_type = meta_data.get_last_display_type(),
                                                    display_name = new_display_name,
                                                    data_values = data_model[new_id].to_list(),
                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                    pred = [last_id],
                                                    args = {"dates": new_dates},
                                                    verify_integrity=True,
                                                    drop_dups = False)
            
            # add dates           
            data_model.insert(0, column = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, value = new_dates)

            # update the overall meta data since we now have a new timescale, the start year and date will be different, and the number of periods
            meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE] = target
            meta_data.meta_data[ForecastMetaDataFrameSchema.START_YEAR] = new_dates[0].year
            meta_data.meta_data[ForecastMetaDataFrameSchema.START_MONTH] = new_dates[0].month
            meta_data.meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS] = len(new_dates)
            
            return(data_model, meta_data, new_id)
