#####################################################################
# forecast_forecast.py
#
# Implements a forecast setup.  Takes the key parameters for a
# forecast and sets-up the initial DateFrame with the forecast
# selection parameters and passes to an object.
# This component MUST BE the starting point for all components
# in a forecasting model in Langflow.
# 
# INPUTS:  None
# OUTPUTS:  DataFrame (ForecastDataModel format)
#####################################################################

from langflow.custom import Component
from langflow.io import DropdownInput, IntInput, StrInput
from langflow.schema import DataFrame
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc

from langflow.services.deps import get_variable_service, session_scope
from langflow.services.variable.constants import CREDENTIAL_TYPE, GENERIC_TYPE


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastEpidemiology
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastForecast(Component):

    # COMPONENT META-DATA
    # -------------------
    display_name: str = "Forecast"
    description: str = "Sets up a forecast for all later components to use."
    icon = "DollarSign"
    name = "Forecast"


    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    form_trigger_rules = []
    # form_trigger_rules = [
    #    (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_shared_vars", ["num_years", "start_year", "input_type", "timescale", "start_month"])),
    # ]



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
        StrInput(
            name="start_year",
            display_name="Start Year",
            info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
#            value=datetime.now().year + 1,
            required=True,
            real_time_refresh = True,
        ),

        # Time Scale
        DropdownInput(
            name = "timescale",
            display_name = "Time-Scale",
            info = "The granularity of the time scale for the forecast.",
            options = [op.value for op in ForecastModelTimescale],
            value = ForecastModelTimescale.YEAR,
            required = True,
            show = True,
            real_time_refresh = True,
        ),

        # Month Start of Fiscal Year
        DropdownInput(
            name="start_month",
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
            value = ForecastModelInputTypes.SINGLE_INPUT,
            required = True,
            show = True,
            real_time_refresh = True,
        ),
    ]


    # COMPONENT OUTPUTS
    # -----------------
    outputs = [
        Output(display_name="Initial Forecast Model", name="forecast_model", method="generate_forecast_model"),
    ]


    def __init__(self, **kwargs) -> None:
        # run parent constructor
        super().__init__(**kwargs)




    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing fields whenever an update happens from a dynamic field
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
        build_config = forecastFormTriggerCalc.execute_trigger(build_config,
                                                               self.form_trigger_rules,
                                                               field_value=field_value,
                                                               field_name=field_name)        

        # return updated config         
        return(build_config)



    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        print(f"start_year = {self.start_year}")
        self.start_year = int(self.start_year)

        # COMMON VALUE CHECKS
        if(not hasattr(self, "num_years")):
            raise ValueError("\n* Missing num_years")

        if(not hasattr(self, "start_year")):
            raise ValueError("\n* Missing start_year")

        if(not hasattr(self, "timescale")):
            raise ValueError("\n* Missing timescale")

        if(not hasattr(self, "input_type")):
            raise ValueError("\n* Missing input_type")

        # Number of Years in Forecast
        if(self.num_years < 1):
            msg += f"\n'{self._inputs["num_years"].display_name}:' must have a positive value."

        # Start Year
        if(self.start_year < FORECAST_MIN_FORECAST_START_YEAR):
            msg += f"\n* '{self._inputs["start_year"].display_name}:' cannot be earlier than {FORECAST_MIN_FORECAST_START_YEAR}."

        # Time-Scale
        if(self.timescale == ""):
            msg += f"\n* '{self._inputs["timescale"].display_name}:' no valid value selected."
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
        
    # generate_forecast_model
    # Generate an empty forecast model with just the dates set-up
    # 
    # INPUTS:
    #   None
    # OUTPUTS:
    #   DataFrame
    async def generate_forecast_model(self) -> DataFrame:
        self.validate_inputs()

        print("key names:")
        print(self.list_key_names())

        forecast_data_model = ForecastDataModel.generate_empty_forecast_data_model(start_year=self.start_year,
                                                                                   num_years=self.num_years,
                                                                                   input_type=self.input_type,
                                                                                   start_month=FORECAST_COMMON_MONTH_NAMES_AND_VALUES[self.start_month],
                                                                                   timescale=self.timescale)
        
        return(forecast_data_model)
    

    # # update_shared_var
    # # forecastFormTriggerCalc Trigger handler:  On a field update of one of the forecast variables, update it in the shared variables context
    # # 
    # # INPUTS:
    # #   build_config - not needed but required by passed in by ForecastFormTriggerCalc
    # #   field_value - the value of the field getting updated
    # #   field_name - the name of the field getting updated
    # #
    # # OUTPUTS:
    # #   build_config - required by ForecastFormTriggerCalc
    # def update_shared_var(self, build_config, field_value, field_name):
    #     if(field_name in ForecastDataModel.REQ_FORECAST_MODEL_ATTR_NAMES):
    #         print(f"forecast update_shared_var:  attempting to update '{field_name}' = '{field_value}'")

    #         # check if we have a shared context, and if so, update it
    #         if(self.shared_context is not None):
    #             self.shared_context.attr[field_name] = field_value
    #             print(f"forecast update_shared_var:  updated shared context {self.shared_context.attr}")
    #         else:
    #             print(f"forecast update_shared_var:  No shared context available for update")

    #     return(build_config)
    