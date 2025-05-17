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
from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.components.forecasting.common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc



# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime
from typing import List
import numpy as np
from langflow.components.forecasting.common.date_utils import gen_dates


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

        # Input Type
        DropdownInput(
            name="input_type",
            display_name="Input Type",
            info="Determines the type of forecast to generate.  'Time Based Input' generates a forecast broken down to a fixed time units (i.e. monthly).  'Single Input' generates a forecast with no breakdown.",
            options=[op.value for op in ForecastModelInputTypes],
            value="",
            required = True,
            real_time_refresh = True,
        ),

        # Patient Count
        IntInput(
            name = "patient_count",
            display_name = "Patient Count",
            info = "The annual count of patients entering the model for the forecast.",
            value = 0,
            required = False,
            show = False,
            dynamic = True,
        ),

        # Growth Rate
        FloatInput(
            name = "growth_rate",
            display_name = "Growth Rate (percent annual)",
            info = "The rate of growth of number of patients entering the forecast year over year.  Enter the decimal value for the percentage (i.e. for 5%% enter 0.05, 10%% enter 0.1, etc.)",
            value = 0.0,
            required = False,
            show = False,
            dynamic = True,
        ),

        # Time Scale
        DropdownInput(
            name = "time_scale",
            display_name = "Time-Scale",
            info = "The granularity of the time scale for the forecast.",
            options = [op.value for op in ForecastModelTimescale],
            value = "",
            required = False,
            show = False,
            dynamic = True,
            real_time_refresh = True,
        ),

        # Month Start of Fiscal Year
        DropdownInput(
            name="month_start_of_fiscal_year",
            display_name="Month Start of Fiscal Year",
            info="For fiscal years which do not start in January, allows you the option of specifying the start month.",
            options=list(FORECAST_COMMON_MONTH_NAMES_AND_VALUES.keys()),
            value=list(FORECAST_COMMON_MONTH_NAMES_AND_VALUES.keys())[0],
            required = False,
            show = False,
            dynamic = True,
            real_time_refresh = True,
        ),

        # Patient Count Table
        TableInput(
            name="patient_count_table",
            display_name="Patient Counts (by Month)",
            info="For each time period, enter the expected number of patients entering the forecast.",
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
        Output(display_name="Epidemiology Forecast Model", name="epi_forecast_model", method="generate_forecast_model"),
    ]


    # FORM UPDATE RULES
    # -----------------
    form_update_rules = {
        "input_type": {
            ForecastModelInputTypes.TIME_BASED: {"show_required": ["time_scale", "patient_count_table"], "hide": ["growth_rate", "patient_count"]},
            ForecastModelInputTypes.SINGLE_INPUT: {"show_required": ["growth_rate", "patient_count"], "hide": ["time_scale", "month_start_of_fiscal_year", "patient_count_table"]},
        },
        "time_scale": {
           ForecastModelTimescale.MONTH: {"show_required": ["month_start_of_fiscal_year"]},
           ForecastModelTimescale.YEAR: {"hide": ["month_start_of_fiscal_year"]}
        } 
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

        # CHECK ALL REQUIRED INPUTS
        # input_type
        if(self.input_type == ""):
            msg += f'\nNo input_type selected.'

        # COMMON VALUE CHECKS
        # Number of Years in Forecast
        if(self.num_years < 1):
            msg += f"'\n{self.inputs[0].display_name}' must have a positive value"

        # Start Year
        if(self.start_year < 1):
            msg += f"'\n{self.inputs[1].display_name}' must have a positive value"

        # TIME_BASED CHECKS
        if(self.input_type == ForecastModelInputTypes.TIME_BASED):
            # Time Scale
            if(self.time_scale == ""):
                msg += f'\nNo time_scale selected.'

        # SINGLE_INPUT CHECKS
        elif(self.input_type == ForecastModelInputTypes.SINGLE_INPUT):
            # Patient Count
            if(self.patient_count < 1):
                msg += f"'\n{self.inputs[3].display_name}' most be a positive number"

            # Growth Rate
            if(self.growth_rate > 0 and self.num_years < 2):
                msg += f"'\n{self.inputs[0].display_name}' must be > 1, to have a {self.inputs[4].display_name} > 0"

        # TIME_BASED CHECKS        
        # NO VALIDATION NEEDED
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
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
 
        return ForecastDataModel(
            start_year = self.start_year,
            num_years = self.num_years,
            start_month=FORECAST_COMMON_MONTH_NAMES_AND_VALUES[self.month_start_of_fiscal_year],
            data=patient_series,
        )
    

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
        time_series = gen_dates(start_year = self.start_year,
                                num_years = self.num_years, 
                                timescale = ForecastModelTimescale.YEAR)

        # if forecast is only for 1 year, return a list with just one element, the patient count
        if(self.num_years == 1):
            epi_series = [self.patient_count]

        # if forecast is for multiple years, but no growth rate, return num_years elements, with the patient count duplicated
        elif(self.growth_rate == 0):
            epi_series = [self.patient_count] * self.num_years
        
        # otherwise, we have multple years and a growth rate, so calculate the compounding growth from year 2 to the end
        else:
            epi_series = [0] * self.num_years
            curr_patient_value = self.patient_count
            epi_series[0] = curr_patient_value

            for i in range(1, self.num_years):
                curr_value = int(np.floor(epi_series[i-1] * (1+self.growth_rate)))
                epi_series[i] = curr_value

        forecast_model = DataFrame({ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series,
                                    self.name: epi_series})
        
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

        time_series = gen_dates(self.start_year, self.num_years, FORECAST_COMMON_MONTH_NAMES_AND_VALUES[self.month_start_of_fiscal_year], self.time_scale)
        return([{"date": time_series[i], "patient_count_table": 0} for i in range(len(time_series))])