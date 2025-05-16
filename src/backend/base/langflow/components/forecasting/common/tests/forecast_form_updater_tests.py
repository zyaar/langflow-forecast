# from datetime import datetime
# from typing import cast, List

# from langflow.custom import Component
# from langflow.io import DropdownInput, IntInput, FloatInput, TableInput

# from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecatModelTimescale



# class ForecastEpidemiology(Component):
#     # COMPONENT META-DATA
#     # -------------------
#     display_name = "Epidemiology"
#     description = "Build an epidemiology stream of patients based on assumptions."
#     icon = "Globe"
#     name = "Epidemiology"

#     # COMPONENT INPUTS
#     # ----------------
#     inputs = [
        
#         # Number of Years in Forecast
#         IntInput(
#             name="num_years",
#             display_name="# of Years to Forecast",
#             info="The number of years to include in the forecast.",
#             value=5,
#             required=True,
#         ),

#         # Start Year
#         IntInput(
#             name="start_year",
#             display_name="Start Year",
#             info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
#             value=datetime.now().year + 1,
#             required=True,
#         ),

#         # Input Type
#         DropdownInput(
#             name="input_type",
#             display_name="Input Type",
#             info="Determines the type of forecast to generate.  'Time Based Input' generates a forecast broken down to a fixed time units (i.e. monthly).  'Single Input' generates a forecast with no breakdown.",
#             options=[op.value for op in ForecastModelInputTypes],
#             value=[],
#             required = True,
#             real_time_refresh = True,
#         ),

#         # Patient Count
#         IntInput(
#             name = "patient_count",
#             display_name = "Patient Count",
#             info = "The annual count of patients entering the model for the forecast.",
#             value = 0,
#             required = False,
#             show = False,
#             dynamic = True,
#         ),

#     ]
#     output = []

from langflow.components.forecasting.common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.forms.forecast_form_updater import ForecastFormUpdater

# simple function to dump the build config state
def print_build_config(config_to_print):
    for i in config_to_print.keys():
        print(f'{i}: {config_to_print[i]}')
    
    print("\n")


###################
# MAIN PROGRAM LOOP
# Test showing required and hiding no longer required based on rules
###################
def main():
    # create fake build_config
    build_config = {
        "num_years": {
            "value": 5,
            "show": True,
            "required": True
        },
        "start_year": {
            "value": "2026",
            "show": True,
            "required": True
        },
        "input_type": {
            "value": "",
            "show": True,
            "required": True
        },
        "patient_count": {
            "value": 0,
            "show": False,
            "required": False
        },
        "growth_rate": {
            "value": 0,
            "show": False,
            "required": False
        },
        "time_scale": {
            "value": ForecastModelTimescale.MONTH,
            "show": False,
            "required": False
        },
        "month_start_of_fiscal_year": {
            "value": "January",
            "show": False,
            "required": False
        },
        "patient_count_table": {
            "value": [],
            "show": False,
            "required": False
        },
    }

    form_update_rules = {
        "input_type": {
            ForecastModelInputTypes.TIME_BASED: {"show": [], "toggle": [], "show_required": ["time_scale", "patient_count_table"], "show_optional": [], "hide": ["growth_rate", "patient_count"], "trigger": []},
            ForecastModelInputTypes.SINGLE_INPUT: {"show": [], "toggle": [], "show_required": ["growth_rate", "patient_count"], "show_optional": [], "hide": ["time_scale", "patient_count_table", "month_start_of_fiscal_year"], "trigger": []},
        },
        "time_scale": {
            ForecastModelTimescale.MONTH: {"show_required": ["month_start_of_fiscal_year"]},
            ForecastModelTimescale.YEAR: {"hide": ["month_start_of_fiscal_year"]}
        } 
    }

    forecastFormUpdater = ForecastFormUpdater()

    # print the initial config
    print("Initial state")
    forecastFormUpdater.forecast_update_fields(build_config, form_update_rules, only_shown_fields=True)
    print_build_config(build_config)

    # set input_type to TIME_BASED
    print("Updated input_type to TIME_BASED")
    build_config["input_type"]["value"] = ForecastModelInputTypes.TIME_BASED
    forecastFormUpdater.forecast_update_fields(build_config, form_update_rules)
    print_build_config(build_config)

    # set input_type to SINGLE_INPUT
    print("Updated input_type to SINGLE_INPUT")
    build_config["input_type"]["value"] = ForecastModelInputTypes.SINGLE_INPUT
    forecastFormUpdater.forecast_update_fields(build_config, form_update_rules)
    print_build_config(build_config)



if __name__ == "__main__":
    main()