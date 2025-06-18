#####################################################################
# forecast_summation_TB.py
#
# Implements the a summation component.  It's already implemented everywhere
# this just makes it explicit (for visual presentation purposes)
# 
# INPUTS:  DataFrame
# OUTPUTS:  DataFrame
#
#####################################################################

from langflow.custom import Component
from langflow.io import StrInput, DataFrameInput, IntInput, TableInput
from langflow.schema import DataFrame
from langflow.schema.table import EditMode
from langflow.template import Output
from langflow.field_typing.range_spec import RangeSpec

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List


# CLASSES
# =======

# ForecastPricingTB
# This class represents converting a series of Product Rx / SKU orders in to a revenue stream by applying price
class ForecastSummationTB(Component):

    # CONSTANTS
    # =========
    MAX_SEGMENTS = 1
    SEGMENT_COL_PREFIX = "total_"

    # COMPONENT META-DATA
    # ===================
    display_name: str = "Summation TB"
    description: str = "Sum up all the inputs provided and create a new totals line in the output."
    icon = "Sigma"
    name: str = "SummationTB"


    # COMPONENT INPUTS
    # ================
    inputs = [
        # Number of Years in Forecast
        StrInput(
            name="num_years",
            display_name="# of Years to Forecast",
            info="The number of years to include in the forecast.",
            required=True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Start Year
        StrInput(
            name="start_year",
            display_name="Start Year",
            info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
            required=True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Time Scale
        StrInput(
            name = "timescale",
            display_name = "Time-Scale",
            info = "The granularity of the time scale for the forecast.",
            required = True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Month Start of Fiscal Year
        StrInput(
            name="start_month",
            display_name="Month Start of Fiscal Year",
            info="For fiscal years which do not start in January, allows you the option of specifying the start month.",
            required = True,
            show = True,
            #dynamic = True,
            #real_time_refresh = True,
            advanced=True,
        ),

        # Input Type
        StrInput(
            name = "input_type",
            display_name = "Input Type",
            info = "Determines the type of forecast to generate.  'Time Based Input' generates which allows for individual values to be entered at the time-scale chosen.  'Single Input' uses a base value and growth/shrink rate at the time-scale chosen.",
            required = True,
            show = True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),


        # dataframes in List[DataFrame]
        DataFrameInput(
            name="forecasts_in",
            display_name="Forecast(s)",
            info="Time Based forecast(s) DataFrame(s)",
            dynamic=True,
            real_time_refresh=True,
            is_list = True,
        ),
    ]


    # COMPONENT OUTPUTS
    # =================
    outputs = [
        Output(display_name="Total", name="total", method="update_forecast_model"),
    ]



    # FORM UPDATE RULES
    # =================
    form_update_rules = {}
    form_trigger_rules = []



    # update_build_config
    # Updates real_time_refreshing INPUTS fields whenever an update happens from a dynamic field
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
        build_config = forecastFormTriggerCalc.execute_trigger(build_config=build_config,
                                                               form_trigger_rules=self.form_trigger_rules,
                                                               field_value=field_value,
                                                               field_name=field_name,)

        # return updated config         
        return(build_config)
    
    

    # OUTPUT FUNCTIONS
    # ================

    # update_forecast_model
    # Return the summation of all the input streams
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model(self) -> DataFrame:
        # run input validation
        self.validate_inputs()

        # sum up all the inputs to create a single total line and add it to the output model
        updated_model = self.check_and_combine_forecasts()
        return(updated_model)
    


    # INPUT VALIDATION
    # ================
    def validate_inputs(self):
        msg = ""

        # TODO:  ADD COMPONENT SPECIFIC CODE HERE
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # HELPER FUNCTIONS
    # ================

    # check_and_combine_forecasts
    # Consolidate all the value checking and dataframe concat into one function
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def check_and_combine_forecasts(self) -> DataFrame:
        updated_model = ForecastDataModel.concat_and_sum(datas=self.forecasts_in, new_col_name = str("Total_"+self._id), skip_total_if_one=True)
        return updated_model
