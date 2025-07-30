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
from langflow.base.forecasting_common.models.date_utils import gen_dates, conv_dates_monthly_to_yearly, conv_dates_yearly_to_monthly




# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
import re
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
      
            



      # =====================
      # FORECAST CALCULATIONS
      # =====================

      # calc_treatment_pat_forecast
      # Calculates a forecast for total number of patients at every every month of the forecast broken out by the total patients by each month of their own treatment journey
      # also calculates the total number of patients leaving their treatment journey, also broken out by which month of their own treatment journey they are leaving
      #
      # INPUT:
      #     component_id:  The treatment component's id, as a prefix to each column name generated
      #     pred_col_id:
      #     updated_model:  The current forecast model, NOTE:  must have the total patients in the last line
      #     updated_meta_data:  The current forecast meta_data, NOTE:  must have the total patients in the last line
      #     treatment_table_col_prefix:
      #     treatment_display_name:
      #     treatment_details_model: the data for the table which has the length of the treatment, the patient progression curve, and which products and how many Rxs are prescribed to a patient at each month of their treatment
      #     treatment_details_meta_data:  the meta_data for the table which has the length of the treatment, the patient progression curve, and which products and how many Rxs are prescribed to a patient at each month of their treatment
      #     forecast_timescale: whether the current forecast is MONTHLY or YEARLY
      #     patient_progression_colname (optional): the name of the column in treatment_details which holds the progression curve for the treatment
      #     pc_initial_state (optional): a list of numbers which can set the first month in the forecast, the number of patients in each month of the treatment progress
      #                                  the default is to set all treatment months after month 1 to zero and set month 1 to the number of New To Therapy (NTP) patients to the first forecast number
      #     month_label_postfix:  the string to append to the end of an ID to indicate this is a total values by month in a treatment journey
      #     total_label_postfix:  the string to append to the end of an ID to indicate this is a total values forecast month
      #     keep_granular (optional): if True, keep the results in the granular timescale (monthly)
      #
      # OUTPUT:
      #     forecast_total_patients_and_total_by_treatment_month: a dataframe which provides a value for each MONTH of a forecast (even if the forecast is set to YEARLY), the total number of patients in that therapy,
      #                                                           as well as the total number of patients by each MONTH of therapy (used to calculate total product Rx in the next step)

      def calc_treatment_pat_forecast(component_id: str,
                                      pred_col_id: str,
                                      updated_model: DataFrame | List[str],
                                      updated_meta_data: ForecastMetaDataFrame,
                                      treatment_table_col_prefix: str,
                                      treatment_display_name: str,
                                      treatment_details_model: DataFrame | List[str],
                                      treatment_details_meta_data: ForecastMetaDataFrame,
                                      pc_initial_state: List | pd.Series,
                                      forecast_timescale: ForecastModelTimescale = ForecastModelTimescale.MONTH,
                                      patient_progression_colname: str = PATIENT_PROGRESSION_COLUMN_NAME,
                                      month_label_postfix: str = "month",
                                      total_label_postfix: str = "Total",
                                      keep_granular: bool = True) -> Tuple[DataFrame, DataFrame, ForecastMetaDataFrame]:
            

            # UPDATED_MODEL
            if(not isinstance(updated_model, DataFrame)):
                  updated_model = DataFrame(data = updated_model)

            updated_model = ForecastDataModel.astype_first_all_cols(in_df = updated_model)


            # TREATMENT DETAIL TABLE
            treatment_name = component_id

            if(not isinstance(treatment_details_model, DataFrame)):
                  treatment_details_model = DataFrame(data = treatment_details_model)

            # TREATMENT DURATION
            treatment_duration = len(treatment_details_model)

            # PC INITIAL STATE
            if(not isinstance(pc_initial_state, pd.Series)):
                  pc_initial_state = pd.Series(data = pc_initial_state, name = "pc initial state")

            # PATIENT PROGRESSION CURVE (PER MONTH)
            patient_progression_col_name = f"{treatment_table_col_prefix}_{patient_progression_colname}"
            patient_progression_curve_full_id = treatment_details_meta_data.id_mgr.gen_full_id(patient_progression_col_name)
            progression_curve = treatment_details_model[patient_progression_col_name]

            # PATIENTS NEW TO THERAPY (input data from previous component)
            data_model = updated_model[pred_col_id].copy().to_frame()

            # CONVERT PATIENT 'NEW TO THERAPY' TO MONTHLY (IF NEEDED)
            if(updated_meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE] != ForecastModelTimescale.MONTH):  # if the forecast timescale is not at the same timescale as MONTHLY, then expand it to be monthly by dividing out the annual
                  (data_model, updated_meta_data, pred_col_id) = ForecastDataModel.convert_timescale(data_model, updated_meta_data, target = ForecastModelTimescale.MONTH, step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)

                  print(f"updated_meta_data={updated_meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE]}")

            #pred_col_id = data_model.columns[-1]
            num_NTP_per = data_model[pred_col_id]


            # ------------------
            #      pred_values_id = f"{pred_col_id}_Yearly_to_Monthly"
            #      data_model = ForecastDataModel.yearly_to_monthly(data_model)
            #      data_model[pred_values_id] = data_model[pred_col_id]
            #      num_NTP_per = data_model[pred_values_id]
            #
            #      pred_display_name = updated_meta_data.model[pred_col_id].meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]
            #      updated_meta_data = updated_meta_data.add_col_meta_data(frame = updated_meta_data,
                                                                        #   id = pred_values_id,
                                                                        #   step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                        #   action = ForecastDataSeriesMetaDataAction.YEAR_TO_MONTH,
                                                                        #   data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                        #   display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                        #   display_name = f"From '{pred_display_name}' (converted from years to months)",
                                                                        #   data_values = num_NTP_per.to_list(),
                                                                        #   validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                        #   pred = [pred_col_id],
                                                                        #   verify_integrity=True,
                                                                        #   drop_dups = False)
            # ---------------------
            #pred_col_id = pred_values_id

            # SETUP data and meta_data for BY TREATMENT MONTH and LEAVING_BY_TREATMENT_MONTH
            pat_by_treatment_month_data = copy.deepcopy(data_model)
            pat_by_treatment_month_meta_data = copy.deepcopy(updated_meta_data)
                        
            pat_leaving_by_treatment_month_data = copy.deepcopy(data_model)
            pat_leaving_by_treatment_month_meta_data = copy.deepcopy(updated_meta_data)

            prev_month_id = num_NTP_per.name

            list_of_on_treatment_ids = []
            list_of_leaving_treatment_ids = []

            # print()
            # print()
            # print(treatment_display_name)

            # CALCULATE BY TREATMENT MONTH and LEAVING BY TREATMENT MONTH FOR EACH TREATMENT MONTH
            for i in range(treatment_duration):
                  
                  # BY TREATMENT MONTH
                  col_id = f"{treatment_name}_{ForecastDataModel.TREATMENT_PAT_TOTAL_BY_MONTH}_{month_label_postfix}_{i+1}"
                  pat_by_treatment_month_data[col_id] = (num_NTP_per * progression_curve[i]).shift(periods = i, fill_value = ForecastDataModel.EDITABLE_VALUES_TOKEN)

                  # add pc_initial conditions if needed
                  if(i > 0):
                        pat_by_treatment_month_data.loc[0:(i-1), col_id] = (pc_initial_state * progression_curve[i]).tail(i).to_list()

                  pat_by_treatment_month_meta_data = pat_by_treatment_month_meta_data.add_col_meta_data(frame = pat_by_treatment_month_meta_data,
                                                                                                        id = col_id,
                                                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                        action = ForecastDataSeriesMetaDataAction.PROD,
                                                                                                        data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                        display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                        display_name = f"# of patients in '{treatment_display_name}' Month {i+1}",
                                                                                                        data_values = pat_by_treatment_month_data[col_id].to_list(),
                                                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
#                                                                                                        pred = [pred_values_id, patient_progression_curve_full_id],
                                                                                                        pred = [pred_col_id, patient_progression_curve_full_id],
                                                                                                        args = {ForecastDataSeriesMetaDataAction.PROD: {"shift": i, "init_vals": pc_initial_state}},
                                                                                                        verify_integrity=True,
                                                                                                        drop_dups = False)
                  list_of_on_treatment_ids.append(col_id) # add to list of preds to be used for totals calculation

                  # new_line = ["{:.0f}".format(val) for val in pat_by_treatment_month_data[col_id].to_list()[:8]]
                  # print(new_line)

                  
                  # LEAVING BY TREATMENT MONTH
                  col_leaving_id = f"{treatment_name}_{ForecastDataModel.TREATMENT_PAT_LEAVING_BY_MONTH}_{month_label_postfix}_{i+1}"
                  # ZIV
                  if(i > 0):
                        pat_leaving_by_treatment_month_data[col_leaving_id] = 0
                  else:
                        pat_leaving_by_treatment_month_data[col_leaving_id] = 0

                  pat_leaving_by_treatment_month_meta_data = pat_leaving_by_treatment_month_meta_data.add_col_meta_data(frame = pat_leaving_by_treatment_month_meta_data,
                                                                                                                        id = col_leaving_id,
                                                                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                                        action = ForecastDataSeriesMetaDataAction.SUB,
                                                                                                                        data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                                        display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                                        display_name = f"# of patients leaving '{treatment_display_name}' in Month {i+1}",
                                                                                                                        data_values = pat_leaving_by_treatment_month_data[col_leaving_id].to_list(),
                                                                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                                        pred = [prev_month_id, col_id],
                                                                                                                        args = {ForecastDataSeriesMetaDataAction.SUB: {"shift_minuend": i, "init_vals": pc_initial_state}},
                                                                                                                        verify_integrity=True,
                                                                                                                        drop_dups = False)
                  
                  list_of_leaving_treatment_ids.append(col_leaving_id) # add to list of preds to be used for totals calculation

                  # save current values as prev values for next loop
                  prev_month_id = col_id
                  #prev_month_values = pat_by_treatment_month_data[col_id]


            # GENERATE TOTALS PATIENTS BY MONTH
            # PATIENTS ON TREATMENT
            pat_by_treatment_month_data_total_id = f"{treatment_name}_{ForecastDataModel.TREATMENT_PAT_TOTAL_BY_MONTH}_{total_label_postfix}"
            pat_by_treatment_month_data[pat_by_treatment_month_data_total_id] = pat_by_treatment_month_data[list_of_on_treatment_ids].sum(axis = 1)

            pat_by_treatment_month_meta_data = pat_by_treatment_month_meta_data.add_col_meta_data(frame = pat_by_treatment_month_meta_data,
                                                                                                  id = pat_by_treatment_month_data_total_id,
                                                                                                  step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                  action = ForecastDataSeriesMetaDataAction.SUM,
                                                                                                  data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                  display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                  display_name = f"Total # patients ON-THERAPY",
                                                                                                  data_values = pat_by_treatment_month_data[pat_by_treatment_month_data_total_id].to_list(),
                                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                  pred = list_of_on_treatment_ids,
                                                                                                  verify_integrity=True,
                                                                                                  drop_dups = False)


            # PATIENTS LEAVING TREATMENT
            pat_leaving_by_treatment_month_data_total_id = f"{treatment_name}_{ForecastDataModel.TREATMENT_PAT_LEAVING_BY_MONTH}_{total_label_postfix}"
            pat_leaving_by_treatment_month_data[pat_leaving_by_treatment_month_data_total_id] = pat_leaving_by_treatment_month_data[list_of_leaving_treatment_ids].sum(axis = 1)

            pat_leaving_by_treatment_month_meta_data = pat_leaving_by_treatment_month_meta_data.add_col_meta_data(frame = pat_leaving_by_treatment_month_meta_data,
                                                                                                                  id = pat_leaving_by_treatment_month_data_total_id,
                                                                                                                  step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                                  action = ForecastDataSeriesMetaDataAction.SUB,
                                                                                                                  data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                                  display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                                  display_name = f"Total # patients LEAVING",
                                                                                                                  data_values = pat_leaving_by_treatment_month_data[pat_leaving_by_treatment_month_data_total_id].to_list(),
                                                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                                  pred = [prev_month_id, col_id],
                                                                                                                  args = {ForecastDataSeriesMetaDataAction.SUB: {"shift_minuend": -i, "init_vals": pc_initial_state}},
                                                                                                                  verify_integrity = True,
                                                                                                                  drop_dups = False)
            
            # CONVERT MONTHLY TO YEARLY (if needed)
            if((forecast_timescale != ForecastModelTimescale.MONTH) and (not keep_granular)):
                  print(f"pat_by_treatment_month_meta_data={pat_by_treatment_month_meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE]}")
                  print(f"pat_leaving_by_treatment_month_meta_data={pat_leaving_by_treatment_month_meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE]}")
                  (pat_by_treatment_month_data, pat_by_treatment_month_meta_data, pred_col_id_on_treatment) = ForecastDataModel.convert_timescale(data_model = pat_by_treatment_month_data, 
                                                                                                                                                  meta_data = pat_by_treatment_month_meta_data,
                                                                                                                                                  target = ForecastModelTimescale.YEAR,
                                                                                                                                                  step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)
                  
                  (pat_leaving_by_treatment_month_data, pat_leaving_by_treatment_month_meta_data, pred_col_id_leaving_treatment) = ForecastDataModel.convert_timescale(data_model = pat_leaving_by_treatment_month_data, 
                                                                                                                                                                       meta_data = pat_leaving_by_treatment_month_meta_data,
                                                                                                                                                                       target = ForecastModelTimescale.YEAR, 
                                                                                                                                                                       step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)

                  ## PATIENTS ON TREATMENT
                  #last_series = pat_by_treatment_month_meta_data.get_last_series()
                  #last_id = pat_by_treatment_month_meta_data.get_last_id()
                  #new_id = f"{last_id}_Monthly_to_Yearly"


                  #pat_by_treatment_month_data = ForecastDataModel.monthly_to_yearly(pat_by_treatment_month_data)
                  #pat_by_treatment_month_data[new_id] = pat_by_treatment_month_data[last_id]
                  #data_values = pat_by_treatment_month_data[new_id]

                  #pred_display_name = last_series.meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]
                  # pat_by_treatment_month_meta_data = pat_by_treatment_month_meta_data.add_col_meta_data(frame = pat_by_treatment_month_meta_data,
                  #                                                                                       id = new_id,
                  #                                                                                       step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                  #                                                                                       action = ForecastDataSeriesMetaDataAction.MONTH_TO_YEAR,
                  #                                                                                       data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                  #                                                                                       display_type = ForecastDataSeriesMetaDataDataType.INT,
                  #                                                                                       display_name = f"From '{pred_display_name}' (converted from months to years)",
                  #                                                                                       data_values = data_values.to_list(),
                  #                                                                                       validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                  #                                                                                       pred = [last_id],
                  #                                                                                       verify_integrity=True,
                  #                                                                                       drop_dups = False)

                  # # PATIENTS LEAVING TREATMENT
                  # last_leaving_series = pat_leaving_by_treatment_month_meta_data.get_last_series()
                  # last_leaving_id = pat_leaving_by_treatment_month_meta_data.get_last_id()
                  # new_leaving_id = f"{last_leaving_id}_Monthly_to_Yearly"

                  # pat_leaving_by_treatment_month_data = ForecastDataModel.yearly_to_monthly(pat_leaving_by_treatment_month_data)
                  # pat_leaving_by_treatment_month_data[new_leaving_id] = pat_leaving_by_treatment_month_data[last_leaving_id]
                  # data_leaving_values = pat_leaving_by_treatment_month_data[new_leaving_id]

                  # pred_leaving_display_name = last_leaving_series.meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]
                  # pat_leaving_by_treatment_month_meta_data = pat_leaving_by_treatment_month_meta_data.add_col_meta_data(frame = pat_leaving_by_treatment_month_meta_data,
                  #                                                                                                       id = new_leaving_id,
                  #                                                                                                       step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                  #                                                                                                       action = ForecastDataSeriesMetaDataAction.MONTH_TO_YEAR,
                  #                                                                                                       data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                  #                                                                                                       display_type = ForecastDataSeriesMetaDataDataType.INT,
                  #                                                                                                       display_name = f"From '{pred_leaving_display_name}' (converted from months to years)",
                  #                                                                                                       data_values = data_leaving_values.to_list(),
                  #                                                                                                       validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                  #                                                                                                       pred = [last_leaving_id],
                  #                                                                                                       verify_integrity=True,
                  #                                                                                                       drop_dups = False)

            # RETURN RESULTS
            return(DataFrame(data=pat_by_treatment_month_data), 
                   DataFrame(data=pat_leaving_by_treatment_month_data), 
                   pat_by_treatment_month_meta_data,
                   pat_leaving_by_treatment_month_meta_data)






      # calc_treatment_rx_forecast_for_product
      # Calculates a forecast for total number of Rxs for a product expected at every every month of the forecast divided
      #
      # INPUT:
      #     treatment_id:  The treatment component's id, as a prefix to each column name generated
      #     treatment_name:
      #     product_id:
      #     treatment_details_product_full_id:
      #     product_model:
      #     product_meta_data:
      #     pat_on_treatment_data:  from treatment details table, the number of Rxs of product to provide at each month in a treatment journey
      #     pat_on_treatment_meta_data:  from treatment details table, meta_data that goes along with the above data
      #     forecast_timescale:  whether the current forecast is MONTHLY or YEARLY
      #     target_timescale:  whether the returned forecast should be MONTHLY or YEARLY
      #     month_label_postfix:  the string to append to the end of an ID to indicate this is a total values by month in a treatment journey
      #     total_label_postfix:  the string to append to the end of an ID to indicate this is a total values forecast month
      #
      # OUTPUT:
      #     

      def calc_treatment_rx_forecast_for_product(treatment_id: str,
                                                 treatment_name: str,
                                                 product_id: str,
                                                 product_display_name: str,
                                                 treatment_details_product_full_id: str,
                                                 product_model: pd.Series,
                                                 product_meta_data: ForecastMetaDataSeries,
                                                 pat_on_treatment_data: pd.DataFrame,
                                                 pat_on_treatment_meta_data: ForecastMetaDataFrame,
                                                 forecast_timescale = ForecastModelTimescale.MONTH,
                                                 target_timescale: ForecastModelTimescale = None,
                                                 month_label_postfix: str = "month",
                                                 total_label_postfix: str = "Total") -> Tuple[DataFrame, ForecastMetaDataFrame]:
            
           # SETUP data and meta_data for BY TREATMENT MONTH
            product_rx_per_treatment_month = product_model.values
            treatment_duration = len(product_rx_per_treatment_month)
            #product_name = product_meta_data.meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]

            # get just the total patients per treatent month columns, we do this by dropping the last row ("_Total") and
            # getting the last X rows before it where X = the total treatment duration:
            forecast_product_rx_by_prog_month = pat_on_treatment_data.iloc[:, -(treatment_duration+1):-1].copy()
            
            # set-up list of references to store for _Total Rx calc at the end
            list_of_by_treatment_month_rx = []

            # print(product_display_name)

            # CALCULATE BY TREATMENT MONTH
            # calculate the number of RXs by multipying to total number of patients in each month of their treatment (for every month of the forecast)
            # by the total Rxs provided for a patient at that month in the program
            for i in range(len(forecast_product_rx_by_prog_month.columns)):
                  # setup
                  pred_col_name = forecast_product_rx_by_prog_month.columns[i]
                  num_rx_col_id = f"{product_id}_{ForecastDataModel.TREATMENT_PRODUCT_RX_BY_MONTH}_{month_label_postfix}_{i+1}"

                  # calculate totals
                  forecast_product_rx_by_prog_month.rename(columns = {forecast_product_rx_by_prog_month.columns[i]: num_rx_col_id}, inplace = True)
                  forecast_product_rx_by_prog_month.iloc[:, i] = forecast_product_rx_by_prog_month.iloc[:, i] * product_rx_per_treatment_month[i]
                  const_full_id = f"{treatment_details_product_full_id}:{i}"

                  # store meta_data
                  pat_on_treatment_meta_data = pat_on_treatment_meta_data.add_col_meta_data(frame = pat_on_treatment_meta_data,
                                                                                            id = num_rx_col_id,
                                                                                            step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                            action = ForecastDataSeriesMetaDataAction.PROD,
                                                                                            data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                            display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                            display_name = f"# of '{product_display_name}' Rx for patients in Month {i+1} of '{treatment_name}'",
                                                                                            data_values = forecast_product_rx_by_prog_month[num_rx_col_id].to_list(),
                                                                                            validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                            #pred = [treatment_details_product_full_id, pred_col_name],
                                                                                            pred = [const_full_id, pred_col_name],
                                                                                            verify_integrity=True,
                                                                                            drop_dups = False)
                  #list_of_by_treatment_month_rx.append(num_rx_col_id)
                  #print(const_full_id, pred_col_name)

                  # new_line = ["{:.0f}".format(val) for val in forecast_product_rx_by_prog_month[num_rx_col_id].to_list()[:8]]
                  # print(new_line)



            # GENERATE TOTALS RX BY FORECAST MONTH
            totals_col_id = f"{product_id}_{ForecastDataModel.TREATMENT_PRODUCT_RX_BY_MONTH}_{total_label_postfix}"
            forecast_product_rx_by_prog_month[totals_col_id] = forecast_product_rx_by_prog_month[list_of_by_treatment_month_rx].sum(axis = 1)
            total_values = forecast_product_rx_by_prog_month[totals_col_id]

            pat_on_treatment_meta_data = pat_on_treatment_meta_data.add_col_meta_data(frame = pat_on_treatment_meta_data,
                                                                                      id = totals_col_id,
                                                                                      step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                      action = ForecastDataSeriesMetaDataAction.SUM,
                                                                                      data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                      display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                      display_name = f"Total # of '{product_display_name}' Rx for patients in '{treatment_name}'",
                                                                                      data_values = total_values.to_list(),
                                                                                      validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                      pred = list_of_by_treatment_month_rx,
                                                                                      verify_integrity=True,
                                                                                      drop_dups = False)


            # CONVERT MONTHLY TO YEARLY (if needed)
            # At this point, our timescale is MONTHLY... check if we want to force a conversion to a different timescale or not
            # (this is a convenience feature, since this is often done)
            if(target_timescale is not None):
                  if(target_timescale == ForecastModelTimescale.MONTH):
                        doConvert = False
                  else:
                        doConvert = True
            
            # if we don't want to force timescale conversion, check if the original timescale was monthly
            # if yes, keep as is, if not, convert to yearly
            elif(forecast_timescale == ForecastModelTimescale.MONTH):
                  doConvert = False
            else:
                  doConvert = True


            if(doConvert):
                  (forecast_product_rx_by_prog_month, pat_on_treatment_meta_data, new_id) = ForecastDataModel.convert_timescale(data_model = forecast_product_rx_by_prog_month, meta_data = pat_on_treatment_meta_data, target = ForecastModelTimescale.YEAR)

                  # last_id = pat_on_treatment_meta_data.get_last_id()
                  # last_id_display_name = pat_on_treatment_meta_data.get_last_series().meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]
                  # new_id = f"{last_id}_Monthly_to_Yearly"
                  # forecast_product_rx_by_prog_month = ForecastDataModel.yearly_to_monthly(forecast_product_rx_by_prog_month)
                  # forecast_product_rx_by_prog_month[new_id] = forecast_product_rx_by_prog_month[last_id]
                  # new_id_values = forecast_product_rx_by_prog_month[new_id]

                  # pat_on_treatment_meta_data = pat_on_treatment_meta_data.add_col_meta_data(frame = pat_on_treatment_meta_data,
                  #                                                                           id = new_id,
                  #                                                                           step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                  #                                                                           action = ForecastDataSeriesMetaDataAction.MONTH_TO_YEAR,
                  #                                                                           data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                  #                                                                           display_type = ForecastDataSeriesMetaDataDataType.INT,
                  #                                                                           display_name = f"From '{last_id_display_name}' (converted from months to years)",
                  #                                                                           data_values = new_id_values.to_list(),
                  #                                                                           validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                  #                                                                           pred = [last_id],
                  #                                                                           verify_integrity=True,
                  #                                                                           drop_dups = False)

            return(DataFrame(data=forecast_product_rx_by_prog_month), pat_on_treatment_meta_data)




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
                  #print(datas[i])
                  #print()
                  #print()
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
      ) -> List[pd.Timestamp]:
            return(gen_dates(start_year=start_year, num_years=num_years, start_month=start_month, time_scale=timescale))
      


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
                  if(not pd.api.types.is_integer_dtype(data) and not  pd.api.types.is_float_dtype(data)):
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
            
            # if timescale is already MONTHLY, throw an error
            if meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE] == target:
                  raise ValueError(f"\n*  convert_monthly_to_yearly:  error, current data_model is already {target}.")

            data_model = copy.deepcopy(data_model)
            meta_data = copy.deepcopy(meta_data)

            # setup
            last_id = data_model.columns[-1]
            last_display_name = meta_data.model[last_id].meta_data[ForecastMetaDataSeriesSchema.DISPLAY_NAME]

            new_dates = gen_dates(start_year = meta_data.meta_data[ForecastMetaDataFrameSchema.START_YEAR],
                                  num_years = meta_data.meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS],
                                  start_month = meta_data.meta_data[ForecastMetaDataFrameSchema.START_MONTH],
                                  time_scale = target)
            
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
                                                    verify_integrity=True,
                                                    drop_dups = False)
            
            # add dates, update TIMESCALE           
            data_model.insert(0, column = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME, value = new_dates)
            meta_data.meta_data[ForecastMetaDataFrameSchema.TIMESCALE] = target
            
            return(data_model, meta_data, new_id)




