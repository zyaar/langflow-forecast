from typing import List, Tuple
import pandas as pd
import numpy as np
from uuid import UUID
from langflow.schema.dataframe import DataFrame, Data

from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.date_utils import gen_dates, conv_dates_monthly_to_yearly, conv_dates_yearly_to_monthly


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

      # Model DataFrame column names
      RESERVED_COLUMN_INDEX_NAME = "dates" # name of the dates column for the forecasting model.  This must be a unique column
      PATIENT_PROGRESSION_COLUMN_NAME = "patient_progression" # name of the column which holds patient progression for a treatment

      # Forecast attributes
      REQ_FORECAST_MODEL_ATTR_NAMES = ["start_year", "num_years", "input_type", "start_month", "timescale"]
      REQ_FORECAST_MODEL_ATTR_TYPES = [int, int, ForecastModelInputTypes, int, ForecastModelTimescale]
      REQ_FORECAST_MODEL_ATTR_DISPLAY_NAMES = ["Start Year", "Number of Years", "Input Type", "Start Month", "Time-scale"]


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
            series_name: str="") -> DataFrame:
            
            # create a dates for this time series based on input values
            time_series_dates = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = num_years, timescale = timescale)

            # bundle it and the series into a dictionary of series and create a DataFrame
            return DataFrame(data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_dates, series_name: data})
      
            



      # =====================
      # FORECAST CALCULATIONS
      # =====================

      # calc_treatment_pat_forecast
      # Calculates a forecast for total number of patients expected at every every month of the forecast divided by what month in their own treatment progression are they in
      #
      # INPUT:
      #     col_prefix:  Unique name to add as prefix to each column name generated
      #     forecast_in = The current forecast model, NOTE:  must have the total patients in the last line
      #     treatment_details: the table which has the length of the treatment, the patient progression curve, and which products and how many Rxs are prescribed to a patient at each month of their treatment
      #     forecast_timescale: whether the current forecast is MONTHLY or YEARLY
      #     patient_progression_colname (optional): the name of the column in treatment_details which holds the progression curve for the treatment
      #     product_prefix_colname (optional): the prefix in every column name in treatment_details which indicates this column details number of Rx for product during each month of the treatment
      #     pc_initial_state (optional): a list of numbers which can set the first month in the forecast, the number of patients in each month of the treatment progress
      #                                  the default is to set all treatment months after month 1 to zero and set month 1 to the number of New To Therapy (NTP) patients to the first forecast number
      #     keep_granular (optional): if True, keep the results in the granular timescale (monthly)
      #
      # OUTPUT:
      #     forecast_total_patients_and_total_by_treatment_month: a dataframe which provides a value for each MONTH of a forecast (even if the forecast is set to YEARLY), the total number of patients in that therapy,
      #                                                           as well as the total number of patients by each MONTH of therapy (used to calculate total product Rx in the next step)
      #     

      def calc_treatment_pat_forecast(col_prefix: str,
                                      forecast_in: DataFrame | List[str],
                                      treatment_details: DataFrame | List[str],
                                      forecast_timescale: ForecastModelTimescale = ForecastModelTimescale.MONTH,
                                      patient_progression_colname: str = PATIENT_PROGRESSION_COLUMN_NAME,
                                      product_prefix_colname = "product_",
                                      pc_initial_state: List = None,
                                      keep_granular: bool = True) -> Tuple[DataFrame, DataFrame]:
            # TREATMENT DURATION
            # get treatment duration (in months)
            treatment_duration = len(treatment_details)

            if(treatment_duration < 1):
                  raise ValueError(f"* calc_treatment_forecast:  invalid treatment duration value: {treatment_duration}.  Treatment duration must be 1 month or greater.")
            
            # TREATMENT DETAILS
            # convert treatment_details to a dataframe if it isn't one already
            if(not isinstance(treatment_details, DataFrame)):
                  treatment_details = DataFrame(data=treatment_details)
                        
            # PATIENT PROGRESSION CURVE
            progression_curve = treatment_details[patient_progression_colname].values

            # PRODUCT LIST & NUM_PRODUCTS
            # get the product list and product count
            treatment_product_colnames = list(filter(lambda col_name: col_name.startswith(product_prefix_colname), treatment_details.columns.to_list()))

            if(len(treatment_product_colnames) < 1):
                  raise ValueError(f"* calc_treatment_forecast:  treatment_details does not have any product columns (columns that begin with prefix '{product_prefix_colname}').  Total list of column names:  {treatment_details.columns.to_list()})")

            num_products = len(treatment_product_colnames)

            if(num_products < 1):
                  raise ValueError(f"* calc_treatment_forecast:  num_products is {num_products}, must be > 0")

            # CURRENT FORECAST
            # convert forecast_in to a dataframe if it isn't one already
            if(not isinstance(forecast_in, DataFrame)):
                  forecast_in = DataFrame(data=forecast_in)
            
            # to be extra safe, convert all columns in the DataFrame but the first one in the forecast to floats, and the first one to dates
            forecast_in = ForecastDataModel.astype_first_all_cols(in_df=forecast_in)

            # # NEW TO THERAPY (NTP) PATIENTS
            # # get the total number of patients coming into the system each time_period     
            # num_NTP_per = forecast_in[forecast_in.columns.to_list()[-1]]
            
            # if the forecast timescale is not at the same timescale as MONTHLY, then expand it to be monthly by dividing out the annual
            # New To Therapy (NTP)
            if(forecast_timescale != ForecastModelTimescale.MONTH):
                  forecast_in = ForecastDataModel.yearly_to_monthly(forecast_in)

            # NEW TO THERAPY (NTP) PATIENTS
            # get the total number of patients coming into the system each time_period     
            num_NTP_per = forecast_in[forecast_in.columns.to_list()[-1]]

            # since the gen_forecast_pat_by_treatment_month doesn't use dates, need to remove the dates column
            # and add it back later
            forecast_dates = None
            if(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in forecast_in.columns):
                  forecast_dates = forecast_in[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME].values

            # GENERATE TOTAL PATIENTS BY PROGRESSION MONTH
            pat_by_treatment_month, pat_leaving_by_treatment_month = ForecastDataModel.gen_forecast_pat_by_treatment_month(treatment_name = col_prefix,
                                                                                                                           num_NTP_per = num_NTP_per, 
                                                                                                                           treatment_duration = treatment_duration, 
                                                                                                                           progression_curve = progression_curve, 
                                                                                                                           pc_initial_state = pc_initial_state)
            # if the dates column was held, add it back here
            if(forecast_dates is not None):
                  pat_by_treatment_month.insert(loc = 0, column = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, value = forecast_dates)
                  pat_leaving_by_treatment_month.insert(loc = 0, column = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, value = forecast_dates)

            
            # if we don't want to keep the results as granular as possible, then revert to (YEARLY) timescale
            if(forecast_timescale != ForecastModelTimescale.MONTH and not keep_granular):
                  pat_by_treatment_month = ForecastDataModel.monthly_to_yearly(pat_by_treatment_month)
                  pat_leaving_by_treatment_month = ForecastDataModel.monthly_to_yearly(pat_leaving_by_treatment_month)


            return(DataFrame(data=pat_by_treatment_month), DataFrame(data=pat_leaving_by_treatment_month))
      



      # calc_treatment_rx_forecast_for_product
      # Return the total number of Rx's for a specific product from a specific treatment for the forecast at monthly granularity,
      # as well as separated out by number of Rx's as well as the total number of Rx's for each month of the forecast each cohort of patients
      # in the same month of their treatment
      #
      # INPUT:
      #     forecast_in = The current forecast model, NOTE:  must have the total patients in the last line
      # TODO:  finish this section
      #     

      def calc_treatment_rx_forecast_for_product(product_name: str,
                                                 col_prefix: str,
                                                 forecast_in: DataFrame | List[str],
                                                 treatment_details: DataFrame | List[str],
                                                 forecast_timescale: ForecastModelTimescale = ForecastModelTimescale.MONTH,
                                                 patient_progression_colname: str = PATIENT_PROGRESSION_COLUMN_NAME,
                                                 product_prefix_colname = "product_",
                                                 pc_initial_state: List = None,
                                                 convert_timescale: ForecastModelTimescale = None) -> DataFrame:
            
            # TREATMENT DETAILS
            # convert treatment_details to a dataframe if it isn't one already (this is repetative with what we'll do in 'calc_treatment_pat_forecast')
            # but we need that for later steps
            if(not isinstance(treatment_details, DataFrame)):
                  treatment_details = DataFrame(data=treatment_details)
            
            # Calculates a forecast for total number of patients expected at every every month of the forecast divided by what month in their own treatment progression are they in
            pat_by_treatment_month, pat_leaving_by_treatment_month = ForecastDataModel.calc_treatment_pat_forecast(col_prefix = col_prefix,
                                                                                                                   forecast_in = forecast_in,
                                                                                                                   treatment_details = treatment_details,
                                                                                                                   forecast_timescale = forecast_timescale,
                                                                                                                   patient_progression_colname = patient_progression_colname,
                                                                                                                   product_prefix_colname = product_prefix_colname,
                                                                                                                   pc_initial_state = pc_initial_state)
            
            # product_use_in_treatment_by_month: create this by removing JUST the product rx data from treatment_details
            product_use_colnames = [colname for colname in treatment_details.columns.to_list() if colname.startswith(product_prefix_colname)]
            product_use_in_treatment_by_month = treatment_details[product_use_colnames]


            # using the forecast data, calculate the number of Rx's for the product_name provided
            products_rxs_by_treatment_month  = ForecastDataModel.gen_forecast_product_rx_by_prog_month(product_name = product_name,
                                                                                                       treatment_name = col_prefix,
                                                                                                       treatment_pat_by_month_forecast = pat_by_treatment_month, 
                                                                                                       product_use_in_treatment_by_month = product_use_in_treatment_by_month)
            
            # SINCE gen_forecast_product_rx_by_prog_month() ALWAYS RETURNS A MONTHLY TIMESCALE,
            # NEED TO CHECK IF WE WANT TO CONVERT IT TO YEARLY
            
            # check if we want to force a conversion to a different timescale (this is a convenience feature, since this is often done)
            if(convert_timescale is not None):
                  if(convert_timescale == ForecastModelTimescale.MONTH):
                        products_rxs_by_treatment = products_rxs_by_treatment_month
                  else:
                        products_rxs_by_treatment = ForecastDataModel.monthly_to_yearly(products_rxs_by_treatment_month)

            # if we don't want to force timescale conversion, check if the original timescale was monthly
            # if yes, keep as is, if not, convert to yearly
            elif(forecast_timescale == ForecastModelTimescale.MONTH):
                  products_rxs_by_treatment = products_rxs_by_treatment_month
            else:
                  products_rxs_by_treatment = ForecastDataModel.monthly_to_yearly(products_rxs_by_treatment_month)
                  
            return(DataFrame(data=products_rxs_by_treatment))



      # gen_forecast_pat_by_treatment_month
      # Helper function:  generate a forecast for total number of patients for a specific treatment, separated out by which MONTH OF TREATMENT they are in
      #  
      # INPUTS:
      #     treatment_name - the name of the treatment to use as prefix for the columns of the result
      #     num_NTP_per - the number of New to Therapy Patients (NTP) entering each month into the system
      #     treatment_duration - the length of the treatment in months
      #     progression_curve - a list for each month of the treatment for the expected % of patients (from the original NTP number) who are still in the treatment
      #     month_prefix (optional) - the prefix to add (after treatname_name) to each month of treatment progression column
      #     total_prefix (optional) - the prefix to add (after treatment_name) to the totals columns
      #     pc_initial_state (optional) - A list defining the number of patients by months in treatment at the first month of the forecast (this is the inital state of the forecast) 
      #                                   (NOTE:  if pc_initial_state is used, it will OVERRIDE the first month of new to therapy patients from num_NTP_per)
      # 
      # OUTPUTS:
      #   DataFrame with columns for the totals by month of treatment and a total column as well

      @staticmethod
      def gen_forecast_pat_by_treatment_month(treatment_name: str,
                                              num_NTP_per: np.array | List, 
                                              treatment_duration: int, 
                                              progression_curve: np.array | List,
                                              month_prefix: str = "month_",
                                              total_postfix: str = "total",
                                              pc_initial_state: List = None ) -> Tuple[pd.DataFrame, pd.DataFrame]:
            # calc forecast_length
            forecast_length = len(num_NTP_per)

            if(isinstance(progression_curve, List)):
                  progression_curve = np.array(progression_curve)

            # recalculate the relative progression curve which calculates the % of the previous months which is retained (vs. % of NTP)
            rel_pc = list(progression_curve[1:] / progression_curve[:-1])

            if(isinstance(num_NTP_per, List)):
                  num_NTP_per = np.array(num_NTP_per)

            # generate the empty forecast of num patients by treatment month
            blank_treatment_months_col = [0.0] * treatment_duration
            blank_for_forecast = [blank_treatment_months_col] * len(num_NTP_per)

            colnames = list(map(lambda i: treatment_name + month_prefix + str(i), list(range(1,treatment_duration+1))))
            pat_by_treatment_month = pd.DataFrame(data=blank_for_forecast, 
                                                  columns=colnames,
                                                  index = list(range(0,forecast_length)))
                                                  #index = list(range(1,forecast_length+1)))
            
            pat_leaving_by_treatment_month = pd.DataFrame(data=blank_for_forecast,
                                                          columns=colnames, 
                                                          index = list(range(forecast_length)))
                                                          #index = list(range(1,forecast_length+1)))
            
            # if pc_initial_states_state is provided, then use that
            if pc_initial_state is not None:
                  if len(pc_initial_state) != treatment_duration:
                        raise ValueError(f"* gen_pat_by_treatment_month:  invalid pc_initial_state provided, pc_initial_state length = {len(pc_initial_state)}, it must be same as treatment duration: {treatment_duration}")
            
            # otherwise, create an initial_state for treatment duration using the first NTP number from the forecast, and zeros for all other months in patient_progression
            else:
                  pc_initial_state = [num_NTP_per[0]] + [0] * (treatment_duration-1)

            # set month 1 of treatment to be the same as the number of NTP patients
            pat_by_treatment_month[pat_by_treatment_month.columns[0]] = np.array(num_NTP_per).astype(float)

            # add the initial state (which if provided, may override the first number in the month 1 treatment progression)
            pat_by_treatment_month.iloc[0] = np.array(pc_initial_state).astype(float)

            # calculate the remaining months by multipling the first month by the attrition in the progression curve
            # NOTE:  progression curve HAS TO be set up as a % of total from the original number
            for row in range(1, forecast_length):
                  for col in range(1, treatment_duration):
                        pat_by_treatment_month.iloc[row, col] = pat_by_treatment_month.iloc[row-1, col-1] * rel_pc[col-1] # note: we use col-1 instead of col to index into rel_pc, because rel_pc is the DELTA betwween two stages in progression_curve, and therefore 1 element shorter than progression_curve
                        pat_leaving_by_treatment_month.iloc[row, col] = pat_by_treatment_month.iloc[row-1, col-1] - pat_by_treatment_month.iloc[row, col]

            # calculate a totals column for both dataframes
            total_pat = pd.Series(pat_by_treatment_month.sum(axis=1), name=treatment_name + total_postfix)
            pat_by_treatment_month = pd.concat([pat_by_treatment_month, total_pat], axis=1)

            total_pat_leave = pd.Series(pat_leaving_by_treatment_month.sum(axis=1), name=treatment_name + total_postfix)
            pat_leaving_by_treatment_month = pd.concat([pat_leaving_by_treatment_month, total_pat_leave], axis=1)

            return(pat_by_treatment_month, pat_leaving_by_treatment_month)
      


      # gen_forecast_product_rx_by_prog_month
      # Helper function:  generate the 
      #  
      # INPUTS:
      #     product_name - name of the product to prefix all columns
      #     treatment_pat_by_month_forecast - the number of patients for each month of the forecast both total and divided by month of patient_progression of treatmnt
      #     product_use_in_treatment_by_month - from the therapy_details TableInput, the list of products and the amount of Rxs (SKUs?) given each month to a patient for for each month of the treatment (patient progression)
      # 
      # OUTPUTS:
      #   DataFrame the number of Rxs (SKUs?) for one product both total for each month of the forecast and by month of treatment 

      @staticmethod
      def gen_forecast_product_rx_by_prog_month(product_name: str,
                                                treatment_name: str,
                                                treatment_pat_by_month_forecast: DataFrame, 
                                                product_use_in_treatment_by_month: DataFrame,
                                                month_name_prefix: str = None,
                                                total_name_postfix: str = "total") -> pd.DataFrame:
            
            # check that the product_name exists as a column in the product_use_in_treatment_by_month
            if(product_name in product_use_in_treatment_by_month.columns):
                  # get the treatment details for the product_name
                  product_rx_per_treatment_month = product_use_in_treatment_by_month[product_name].values
            else:
                  raise ValueError(f"* gen_forecast_product_rx_by_prog_month:  invalid product_name '{product_name}' provided, is not found in product_use_in_treatment_by_month columns:  '{product_use_in_treatment_by_month.columns.to_list()}'")
            
            # if NO col_name_prefix was provided, use the product_name with an additional "_" as the prefix for all col_names
            if(month_name_prefix is None):
                  month_name_prefix = product_name + "_"

            # check if there is a dates column
            if(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in treatment_pat_by_month_forecast.columns):
                  hasDates = True
            else:
                  hasDates = False

            # if we have dates, remove them for now, we'll add back at the end
            if(hasDates):
                  dates_col = treatment_pat_by_month_forecast[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME].values
                  treatment_pat_by_month_forecast = treatment_pat_by_month_forecast.drop(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, axis=1)


            # make a copy of treatment_pat_by_month_forecast (except for the "total_" column because it 
            # has the same structure as we are going to create for the product use
            forecast_product_rx_by_prog_month = treatment_pat_by_month_forecast.iloc[:, :-1].copy()

            for i in range(len(forecast_product_rx_by_prog_month.columns)):
                  forecast_product_rx_by_prog_month.iloc[:, i] = forecast_product_rx_by_prog_month.iloc[:, i] * product_rx_per_treatment_month[i]
            
            # calculate a totals column
            totals = [forecast_product_rx_by_prog_month.iloc[i,:].sum() for i in range(0, len(forecast_product_rx_by_prog_month))]
            total_products = pd.Series(totals, index=forecast_product_rx_by_prog_month.index, name=f"{treatment_name}{total_name_postfix}")
            forecast_product_rx_by_prog_month = pd.concat([forecast_product_rx_by_prog_month, total_products], axis=1)

            # finally, add product_name to each column in the results
            colnames = [month_name_prefix + col for col in forecast_product_rx_by_prog_month]
            forecast_product_rx_by_prog_month.columns = colnames

            # if we have dates, add them back now
            if(hasDates):
                  forecast_product_rx_by_prog_month.insert(loc=0, column = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, value = dates_col)

            return(forecast_product_rx_by_prog_month)



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
      ) -> List[pd.Timestamp]:
            return(gen_dates(start_year=start_year, num_years=num_years, start_month=start_month, time_scale=timescale))
      


      # yearly_to_monthly
      # Helper function:  given a pd.Series which is assumed to be YEARLY, convert it to monthly time series
      # by taking the annual values and dividing them by twelve and spreading that over over 12 columns
      #  
      # INPUTS:
      #     pd.Series of Yearly numbers (no Date index needed)
      # OUTPUTS:
      #   pd.Series - a series with the MONTHLY numbers that align to those same yearly numbers

      @staticmethod
      def yearly_to_monthly(data: pd.Series | pd.DataFrame) -> pd.Series | pd.DataFrame:
            num_years = len(data.index)
            data.index = list(range(1, num_years+1))

            # simplest way to do this which is not dependent on having a datetime for an index is simply repeat
            # the data series 12 times (number of months in a year) and then divide all the values by 12
            if(isinstance(data, pd.Series)):
                  data_out = data.repeat(12)/12

            # if dataframe, have to check if Date columns is there
            else:
                  has_date_col = False

                  # code to handle if there is a date column
                  if(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in data.columns):
                        has_date_col = True
                        new_date_col = conv_dates_yearly_to_monthly(data = data[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME])
                        data = data.drop(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, axis=1)

                  data_out = data.iloc[np.repeat(np.arange(len(data)), 12)]/12

                  # if we had a date col, add the new dates back in
                  if(has_date_col):
                        data_out.insert(0, ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, new_date_col)

            data_out.index = list(range(num_years*12))
            #data_out.index = list(range(1, (num_years*12)+1))
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
            data.index = list(range(1, len(data)+1))

            # Special code for a DataFrame to handle the Date column
            if(isinstance(data, pd.DataFrame) and ForecastDataModel.RESERVED_COLUMN_INDEX_NAME in data.columns):
                  has_date_col = True
                  new_date_col = conv_dates_monthly_to_yearly(data = data[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME])
                  data = data.drop(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, axis=1)

            # simplest way to do this is to group by every 12 units of index and sum up the values.
            # however, if the index does not start at zero, need to make the index zero to line up the months with the years
            data_out = data.groupby((data.index-min(data.index)) // 12).sum()
            data_out.index = list(range(len(data_out)))
            #data_out.index = list(range(1, len(data_out) + 1))


            # Special code if it is a DataFrame WITH the Date column, to add it back in
            if(isinstance(data, pd.DataFrame) and has_date_col):
                  data_out.insert(0, ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, new_date_col)

            return(data_out)
      



