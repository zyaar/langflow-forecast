from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc

# simple function to dump the build config state
def print_build_config(config_to_print):
    for i in config_to_print.keys():
        print(f'{i}: {config_to_print[i]}')
    
    print("\n")

class FakeComponent():
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

    # create fake biz rules
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("patient_count_table", "generate_table_values", ["num_years", "start_year", "input_type", "time_scale", "month_start_of_fiscal_year"])),
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("add_new_fields", ["input_type"])),
    ]


    def __init__(self):
        self.label_to_use = "hello"
        self.forecastFormTriggerCalc = ForecastFormTriggerCalc()

    def update_build_config(self, build_config, field_value, field_name):
        build_config = self.forecastFormTriggerCalc.execute_trigger(build_config=build_config,
                                                                    form_trigger_rules=self.form_trigger_rules, 
                                                                    field_value=field_value,
                                                                    field_name=field_name,
                                                           
                                                                    # list of all the updater functions for calculated fields
                                                                    generate_table_values=self.generate_table_values,
                                                                    add_new_fields=self.add_new_fields)
        return(build_config)


    # simple function to test ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE
    def generate_table_values(self, field_value: str, field_name: str) -> str:
        return(self.label_to_use)

    # simple function to test ForecastFormTriggerCalc.TriggerType.RUN_FUNCT
    def add_new_fields(self, build_config, field_value, field_name):
        build_config["new_entry1"] = {"value": 50, "show": False, "required": True}
        build_config["new_entry2"] = {"value": self.label_to_use, "show": True, "required": False}
        build_config["new_entry3"] = {"value": ["red", "yellow", "green"], "show": True, "required": True}
        return(build_config)





###################
# MAIN PROGRAM LOOP
# Test showing required and hiding no longer required based on rules
###################
def main():    
    # print the initial config
    print("Initial state")
    testComponent = FakeComponent()
    print_build_config(testComponent.build_config)

    # update input_type to TIME_BASED and run some rules
    print("Update input_type to TIME_BASED")
    testComponent.build_config["input_type"]["value"] = ForecastModelInputTypes.TIME_BASED
    build_config = testComponent.update_build_config(testComponent.build_config, field_value=ForecastModelInputTypes.TIME_BASED, field_name="input_type")
    print_build_config(build_config)

if __name__ == "__main__":
    main()