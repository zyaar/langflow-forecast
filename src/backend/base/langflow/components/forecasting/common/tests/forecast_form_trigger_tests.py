from langflow.components.forecasting.common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc

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

    form_trigger_rules = {
        "patient_count_table": ("generate_table_values", ["num_years", "start_year", "input_type", "time_scale", "month_start_of_fiscal_year"]),
    }
    
    forecastFormTriggerCalc = ForecastFormTriggerCalc()

    # print the initial config
    print("Initial state")
    print_build_config(build_config)

    # update input_type to be TIME_BASED
    build_config["input_type"]["value"] = ForecastModelInputTypes.TIME_BASED
    build_config = forecastFormTriggerCalc.forecast_update_fields(build_config,
                                                                  form_trigger_rules, 
                                                                  field_value=ForecastModelInputTypes.TIME_BASED,
                                                                  field_name="input_type",
                                                                  
                                                                  # list of all the updater functions for calculated fields
                                                                  generate_table_values=generate_table_values)
    print_build_config(build_config)



if __name__ == "__main__":
    main()