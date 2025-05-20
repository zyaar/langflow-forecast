#####################################################################
# forecast_epidemiology.py
#
# Implements the epidemiology component of the forecasting.  The epidemiology
# component sets up a forecast based on some date/time assumptions
# and an incident rate of patients per a timeperiod (monthly, yearly).
# 
# INPUTS:  None
# OUTPUTS:  ForecastDataModel
#
#####################################################################

from langflow.base.data.utils import TEXT_FILE_TYPES, parallel_load_data, parse_text_file_to_data, retrieve_file_paths
from langflow.custom import Component
from langflow.io import DropdownInput, IntInput, FloatInput, TableInput
from langflow.schema import DataFrame
from langflow.schema.table import EditMode
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from typing import cast
from langflow.components.forecasting.common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.components.forecasting.common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc



# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime
from typing import List
import numpy as np


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastEpidemiology
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastEpidemiology(Component):

    # COMPONENT META-DATA
    # -------------------
    display_name = "Epidemiology"
    description = "Build an epidemiology stream of patients based on assumptions."
    icon = "Globe"
    name = "Epidemiology"

    # COMPONENT INPUTS
    # ----------------
    inputs = [

        # Number of Years in Forecast
        IntInput(
            name="num_years",
            display_name="# of Years to Forecast",
            info="The number of years to include in the forecast.",
            value=5,
            required=True,
            real_time_refresh = True,
        ),

        # Start Year
        IntInput(
            name="start_year",
            display_name="Start Year",
            info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
            value=datetime.now().year + 1,
            required=True,
            real_time_refresh = True,
        ),

        # Time Scale
        DropdownInput(
            name = "time_scale",
            display_name = "Time-Scale",
            info = "The granularity of the time scale for the forecast.",
            options = [op.value for op in ForecastModelTimescale],
            value = "",
            required = True,
            show = True,
            real_time_refresh = True,
        ),

        # Month Start of Fiscal Year
        DropdownInput(
            name="month_start_of_fiscal_year",
            display_name="Month Start of Fiscal Year",
            info="For fiscal years which do not start in January, allows you the option of specifying the start month.",
            options=list(FORECAST_COMMON_MONTH_NAMES_AND_VALUES.keys()),
            value=list(FORECAST_COMMON_MONTH_NAMES_AND_VALUES.keys())[0],
            required = True,
            show = True,
            real_time_refresh = True,
        ),

        # Input Type
        DropdownInput(
            name = "input_type",
            display_name = "Input Type",
            info = "Determines the type of forecast to generate.  'Time Based Input' generates which allows for individual values to be entered at the time-scale chosen.  'Single Input' uses a base value and growth/shrink rate at the time-scale chosen.",
            options = [op.value for op in ForecastModelInputTypes],
            value = "",
            required = True,
            show = True,
            real_time_refresh = True,
        ),

        # Patient Count
        IntInput(
            name="patient_count",
            display_name="Patient Count",
            info="The count of new patients entering the model for each time-scale period in the forecast.",
            value=0,
            required=False,
            show=False,
            dynamic=True,
        ),

        # Growth Rate
        FloatInput(
            name="growth_rate",
            display_name="Growth Rate (% in the time-scale)",
            info="The rate of growth of number of patients entering the forecast year over year.  Enter the decimal value for the percentage (i.e. for 5%% enter 0.05, 10%% enter 0.1, etc.)",
            value=0.0,
            required=False,
            show=False,
            dynamic=True,
        ),

        # Patient Count Table
        TableInput(
            name="patient_count_table",
            display_name="Patient Counts (by Time-Scale)",
            info="For each time-scale period, enter the expected number of patients entering the forecast.",
            required=False,
            show=False,
            dynamic=True,
            table_schema=[
                {
                    "name": "date",
                    "display_name": "Date",
                    "type": "date",
                    "description": "Date of patient count",
                    #"edit_mode": EditMode.INLINE,
                    "disable_edit": True,
                },
                {
                    "name": "patient_count_table",
                    "display_name": "Patient Count",
                    "type": "int",
                    "description": "Patient count",
                    #"edit_mode": EditMode.INLINE,
                },
            ],
            value=[],
        ),
    ]


    # COMPONENT OUTPUTS
    # -----------------
    outputs = [
        Output(display_name="Epidemiology Patient Flow", name="epi_forecast_model", method="generate_forecast_model"),
    ]


    # FORM UPDATE RULES
    # -----------------
    form_update_rules = {
        "input_type": {
            ForecastModelInputTypes.TIME_BASED: {"show_required": ["patient_count_table"], "hide": ["growth_rate", "patient_count"]},
            ForecastModelInputTypes.SINGLE_INPUT: {"show_required": ["growth_rate", "patient_count"], "hide": ["patient_count_table"]},
        },
    }

    form_trigger_rules = {
       "patient_count_table": ("generate_table_values", ["num_years", "start_year", "input_type", "time_scale", "month_start_of_fiscal_year"]),
    }
    


    # UPDATE_BUILD_CONFIG (update dynamically changing fields in the form of the component)
    # -------------------
    def update_build_config(self, build_config, field_value, field_name = None):

        # update the fields in the form to show/hide, based on the field updated
        forecastFormUpdater = ForecastFormUpdater()
        build_config = forecastFormUpdater.forecast_update_fields(build_config, 
                                                                  self.form_update_rules,
                                                                  field_value = field_value,
                                                                  field_name = field_name,
                                                                  only_shown_fields=True)
        
        # update the calculated values of fields in the form based on the field updated        
        forecastFormTriggerCalc = ForecastFormTriggerCalc()
        build_config = forecastFormTriggerCalc.forecast_update_fields(build_config,
                                                                      self.form_trigger_rules,
                                                                      field_value=field_value,
                                                                      field_name=field_name,

                                                                      # list of all the updater functions for calculated fields
                                                                      generate_table_values=self.generate_table_values)

        
        # return updated config         
        return(build_config)



    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # COMMON VALUE CHECKS
        if(not hasattr(self, "num_years")):
            raise ValueError("Missing num_years")

        if(not hasattr(self, "start_year")):
            raise ValueError("Missing start_year")

        if(not hasattr(self, "time_scale")):
            raise ValueError("Missing time_scale")

        if(not hasattr(self, "input_type")):
            raise ValueError("Missing input_type")

        # Number of Years in Forecast
        if(self.num_years < 1):
            msg += f"\n'{self._inputs["num_years"].display_name}:' must have a positive value."

        # Start Year
        if(self.start_year < FORECAST_MIN_FORECAST_START_YEAR):
            msg += f"\n'{self._inputs["start_year"].display_name}:' cannot be earlier than {FORECAST_MIN_FORECAST_START_YEAR}."

        # Time-Scale
        if(self.time_scale == ""):
            msg += f"\n'{self._inputs["time_scale"].display_name}:' no valid value selected."

        # Input Type
        if(self.input_type == ForecastModelInputTypes.TIME_BASED):
            # do nothing
            True

        # SINGLE_INPUT CHECKS
        elif(self.input_type == ForecastModelInputTypes.SINGLE_INPUT):
            # Patient Count
            if(self.patient_count < 1):
                msg += f"\n'{self._inputs["patient_count"].display_name}' must be a positive number"
            
            # Growth Rate
            if(self.growth_rate > 0 and self.num_years < 2):
                msg += f"Positive growth rate will never be applied if '{self._inputs["num_years"].display_name}' is set to 1."
        

        else:
           msg += f"\n'{self._inputs["input_type"].display_name}:' no valid value selected."


        # TIME_BASED CHECKS        
        # NO VALIDATION NEEDED
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
        
    # generate_forecast_model
    # Output function epi_forecast_model end-point
    # 
    # INPUTS:
    # OUTPUTS:
    #   ForecastDataModel (which is automatically cast down to DataFrame)
    def generate_forecast_model(self) -> DataFrame:
        self.validate_inputs()
        patient_series = self.generate_epi_series()
        patient_dataframe = DataFrame(data = patient_series)
 
        return ForecastDataModel.init_forecast_data_model(patient_dataframe,
                                                          start_year = self.start_year,
                                                          num_years = self.num_years,
                                                          input_type=self.input_type,
                                                          start_month=FORECAST_COMMON_MONTH_NAMES_AND_VALUES[self.month_start_of_fiscal_year],
                                                          timescale=self.time_scale)
    

    # generate_epi_series
    # Generate the patient counts for each year of the forecast period using the epi assumptions
    # 
    # INPUTS:
    # OUTPUTS:
    #   List of patient counts for each year of the forecast
    def generate_epi_series(self) -> DataFrame:

        # generate SINGLE_INPUT epi patient series
        if(self.input_type == ForecastModelInputTypes.SINGLE_INPUT):
            forecast_model = self.generate_single_input()

        # generate TIME_BASED epi patient series
        else:
            forecast_model = self.generate_time_based()

        return(forecast_model)
    
    
    # generate_time_based
    # Generate the epi patient counts using the TIME_BASED assumptions
    # 
    # INPUTS:
    # OUTPUTS:
    #   List of patient counts for each year of the forecast
    # add another comment
    def generate_time_based(self) -> DataFrame:
        return(DataFrame(self.patient_count_table))


    # generate_single_input
    # Generate the epi patient counts using the SINGLE_INPUT assumptions
    # 
    # INPUTS:
    # OUTPUTS:
    #   List of patient counts for each year of the forecast
    def generate_single_input(self) -> DataFrame:
        # generate dates
        time_series = ForecastDataModel.gen_forecast_dates(start_year = self.start_year,
                                                           num_years = self.num_years,
                                                           start_month = FORECAST_COMMON_MONTH_NAMES_AND_VALUES[self.month_start_of_fiscal_year],
                                                           timescale = self.time_scale)
        
        # determine the number of periods to return 
        # (either the total years of the forecast, or the total years * 12 months per year of the forecast)
        num_periods = self.num_years if self.time_scale == ForecastModelTimescale.YEAR else self.num_years * 12

        # if there is no growth rate, return the patient count for each time period
        if(self.growth_rate == 0):
            epi_series = [self.patient_count] * num_periods

        # if we have a growth rate, start with the initial patient count and add a growth rate to each subsequent
        # time period
        else:
            epi_series = [0] * self.num_years if self.time_scale == ForecastModelTimescale.YEAR else [0] * self.num_years * 12
            curr_patient_value = self.patient_count

            for i in range(num_periods):
                # epi_series[i] = int(np.floor(curr_patient_value))
                epi_series[i] = curr_patient_value
                curr_patient_value = curr_patient_value * (1+self.growth_rate)
        
        # return as a dataframe
        forecast_model = DataFrame({ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series, self.name: epi_series})
        return(forecast_model)
    

    # generate_table_values
    # Generate the default values for the patient_count_table (dates and zeros for counts)
    # 
    # INPUTS:
    # OUTPUTS:
    #   List of dictionaries / one dictionary per row, looking like this:
    #  value=[{"date": "2010-01-01", "patient_count_table": 10,},
    #         {"date": "2010-02-01", "patient_count_table": 20,}],

    def generate_table_values(self) -> List[dict]:
        if((self.month_start_of_fiscal_year not in FORECAST_COMMON_MONTH_NAMES_AND_VALUES.keys()) or (self.num_years < 1)):
            return([])

        time_series = ForecastDataModel.gen_forecast_dates(start_year = self.start_year,
                                                           num_years = self.num_years,
                                                           start_month = FORECAST_COMMON_MONTH_NAMES_AND_VALUES[self.month_start_of_fiscal_year],
                                                           timescale = self.time_scale)
        return([{ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series[i], "patient_count_table": 0} for i in range(len(time_series))])