from langflow.base.data.utils import TEXT_FILE_TYPES, parallel_load_data, parse_text_file_to_data, retrieve_file_paths
from langflow.custom import Component
from langflow.io import DropdownInput, IntInput, FloatInput
from langflow.schema import Data
from langflow.schema.dataframe import DataFrame
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecatModelTimescale, FORECAST_COMMON_TIME_SCALE, FORECAST_MODEL_DATAFRAME_COLUMNS


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime


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
            value=1,
            required=True,
        ),

        # Start Year
        IntInput(
            name="start_year",
            display_name="Start Year",
            info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
            value=datetime.now().year + 1,
            required=True,
        ),

        # Input Type
        DropdownInput(
            name="input_type",
            display_name="Input Type",
            info="Determines the type of forecast to generate.  'Time Based Input' generates a forecast broken down to a fixed time units (i.e. monthly).  'Single Input' generates a forecast with no breakdown.",
            options=[op.value for op in ForecastModelInputTypes],
            value=[],
            required = True,
            real_time_refresh = True,
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
            options = [op.value for op in ForecatModelTimescale],
            value = [],
            required = False,
            show = False,
            dynamic = True,
            real_time_refresh = True,
        ),

        # Patient Count
        IntInput(
            name = "patient_count",
            display_name = "Patient Count",
            info = "The initial count of patients entering the model for the forecast.",
            value = 0,
            required = False,
            show = False,
            dynamic = True,
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
        ),
    ]



    # COMPONENT OUTPUTS
    # -----------------
    outputs = [
        Output(display_name="DataFrame", name="dataframe", method="generate_forecast_model"),
    ]



    # UPDATE_BUILD_CONFIG (update dynamically changing fields in the form of the component)
    # -------------------
    def update_build_config(self, build_config, field_value, field_name = None):

        # Changed field:  Input Type
        if(field_name == "input_type"):

            # Time Based Input
            if(field_value == ForecastModelInputTypes.TIME_BASED):
                # growth_rate (OFF)
                build_config["growth_rate"]["show"] = False
                build_config["growth_rate"]["required"] = False

                # time_scale (ON)
                build_config["time_scale"]["show"] = True
                build_config["time_scale"]["required"] = True

                # patient_count (ON)
                build_config["patient_count"]["show"] = True
                build_config["patient_count"]["required"] = True

                # month_start_of_fiscal_year (if time_scale is month ON, else OFF)
                if(self.time_scale == ForecatModelTimescale.MONTH):
                    build_config["month_start_of_fiscal_year"]["show"] = True
                    build_config["month_start_of_fiscal_year"]["required"] = True
                else:
                    build_config["month_start_of_fiscal_year"]["show"] = False
                    build_config["month_start_of_fiscal_year"]["required"] = False
            
            # Single Input
            elif(field_value == ForecastModelInputTypes.SINGLE_INPUT):
                # growth_rate (ON)
                build_config["growth_rate"]["show"] = True
                build_config["growth_rate"]["required"] = True

                # time_scale (OFF)
                build_config["time_scale"]["show"] = False
                build_config["time_scale"]["required"] = False
                
                # patient_count (ON)
                build_config["patient_count"]["show"] = True
                build_config["patient_count"]["required"] = True

                # month_start_of_fiscal_year (OFF)
                build_config["month_start_of_fiscal_year"]["show"] = False
                build_config["month_start_of_fiscal_year"]["required"] = False
                
        
        # Changed field:  Time-Scale
        elif(field_name == "time_scale"):
            if(field_value == ForecatModelTimescale.MONTH):
                build_config["month_start_of_fiscal_year"]["show"] = True
                build_config["month_start_of_fiscal_year"]["required"] = True

            else:
                build_config["month_start_of_fiscal_year"]["show"] = False
                build_config["month_start_of_fiscal_year"]["required"] = False

                
        # return updated config         
        return(build_config)



    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # Number of Years in Forecast
        if(self.num_years < 1):
            msg += f"'\n{self.inputs[0].display_name}' must have a positive value"

        # Start Year
        if(self.start_year < 1):
            msg += f"'\n{self.inputs[1].display_name}' must have a positive value"

        
        # Patient Count
        if(self.patient_count < 0):
            msg += f"'\n{self.inputs[2].display_name}' cannot be negative"
        
        # Input Type
        # NO VALIDATION NEEDED
        
        # Month Start of Fiscal Year
        # NO VALIDATION NEEDED
        
        # Growth Rate
        # NO VALIDATION NEEDED
        
        # Time Scale
        # NO VALIDATION NEEDED

        if(msg != ""):
            self.status = msg
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
    def generate_forecast_model(self) -> DataFrame:
        self.validate_inputs()

        # determine how many time-units (rows) we will need
        #if((self.input_type == ForecastEpidemiologyInputTypes.SINGLE_INPUT) or (() and ()))
        
        # return the dataframe
        return DataFrame(columns=FORECAST_MODEL_DATAFRAME_COLUMNS)
    