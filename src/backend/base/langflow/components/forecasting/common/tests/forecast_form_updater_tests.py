from langflow.components.forecasting.common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.forms.forecast_form_updater import ForecastFormUpdater

# simple function to dump the build config state
def print_build_config(config_to_print):
    for i in config_to_print.keys():
        print(f'{i}: {config_to_print[i]}')
    
    print("\n")

# simple function to test the trigger_value_update action
def generate_table_values():
    return("hello")

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
            ForecastModelInputTypes.TIME_BASED: {"show": [], "toggle": [], "show_required": ["time_scale", "patient_count_table"], "show_optional": [], "hide": ["growth_rate", "patient_count"]},
            ForecastModelInputTypes.SINGLE_INPUT: {"show": [], "toggle": [], "show_required": ["growth_rate", "patient_count"], "show_optional": [], "hide": ["time_scale", "patient_count_table", "month_start_of_fiscal_year"]},
        },
        "time_scale": {
            ForecastModelTimescale.MONTH: {"show_required": ["month_start_of_fiscal_year"]},
            ForecastModelTimescale.YEAR: {"hide": ["month_start_of_fiscal_year"]}
        } 
    }

    forecastFormUpdater = ForecastFormUpdater()

    # print the initial config
    print("Initial state")
    print_build_config(build_config)

    # set input_type to TIME_BASED
    print("Updated input_type to TIME_BASED")
    build_config["input_type"]["value"] = ForecastModelInputTypes.TIME_BASED
    build_config = forecastFormUpdater.forecast_update_fields(build_config, 
                                                              form_update_rules, 
                                                              field_value=ForecastModelInputTypes.TIME_BASED, 
                                                              field_name="input_type", 
                                                              only_shown_fields=True)
    print_build_config(build_config)

    # set input_type to SINGLE_INPUT
    print("Updated input_type to SINGLE_INPUT")
    build_config["input_type"]["value"] = ForecastModelInputTypes.SINGLE_INPUT
    build_config = forecastFormUpdater.forecast_update_fields(build_config,
                                                              form_update_rules, 
                                                              field_value=ForecastModelInputTypes.SINGLE_INPUT, 
                                                              field_name="input_type",
                                                              only_shown_fields=True)
    print_build_config(build_config)



if __name__ == "__main__":
    main()