#####################################################################
# ForecastFormUpdate
#
# Series of helper functions to make code cleaner in the Compoents
# when running business rules around what fields to show, hide, make
# required, make optional, etc.
#
#####################################################################

# COMMON IMPORTS
# ==============
from enum import Enum



class ForecastFormUpdater():
    # "show": [], "toggle": [], "show_required": [], "show_optional": [], "hide": [], "trigger": []
    # Enum of supporting forecast types
    class RULE_TYPES(str, Enum):
        SHOW = "show"
        TOGGLE = "toggle"
        SHOW_REQUIRED = "show_required"
        SHOW_OPTIONAL = "show_optional"
        HIDE = "hide"
        TRIGGER = "trigger"

    # forecast_show_fields
    # Given a list of variables to show , iterate over each
    # one in the build_config updating the "show" to true
    # 
    # INPUTS:
    #   build_config: Pydantic build_config structure
    #   list_of_vars: List of strings, each string a variable name to update
    #
    # OUTPUTS:
    #   build_config
    def forecast_show_fields(self, build_config, list_of_vars):

        for curr_var in list_of_vars:
            build_config[curr_var]["show"] = True
        
        return(build_config)


    # forecast_toggle_show_fields
    # Given a list of variables, iterate over each
    # one in the build_config and flip the value to the "show"
    # attribute
    # 
    # INPUTS:
    #   build_config: Pydantic build_config structure
    #   list_of_vars: List of strings, each string a variable name to update
    #
    # OUTPUTS:
    #   build_config
    def forecast_toggle_show_fields(self, build_config, list_of_vars):

        for curr_var in list_of_vars:
            build_config[curr_var]["show"] = not build_config[curr_var]["show"]
        
        return(build_config)


    # forecast_show_required_fields
    # Given a list of variables to show and make required, iterate over each
    # one in the build_config updating the "show" and "required" attributes
    # to true
    # 
    # INPUTS:
    #   build_config: Pydantic build_config structure
    #   list_of_vars: List of strings, each string a variable name to update
    #
    # OUTPUTS:
    #   build_config
    def forecast_show_required_fields(self, build_config, list_of_vars):

        for curr_var in list_of_vars:
            build_config[curr_var]["show"] = True
            build_config[curr_var]["required"] = True
        
        return(build_config)


    # forecast_show_optional_fields
    # Given a list of variables to show and make optional, iterate over each
    # one in the build_config updating the "show" to true and the "required"
    # attribute to false
    # 
    # INPUTS:
    #   build_config: Pydantic build_config structure
    #   list_of_vars: List of strings, each string a variable name to update
    #
    # OUTPUTS:
    #   build_config
    def forecast_show_optional_fields(self, build_config, list_of_vars):

        for curr_var in list_of_vars:
            build_config[curr_var]["show"] = True
            build_config[curr_var]["required"] = False
        
        return(build_config)


    # forecast_hide_fields
    # Given a list of variables to hide, iterate over each
    # one in the build_config updating the "show" and "required" attributes
    # to false
    # 
    # INPUTS:
    #   build_config: Pydantic build_config structure
    #   list_of_vars: List of strings, each string a variable name to update
    #
    # OUTPUTS:
    #   build_config
    def forecast_hide_fields(self, build_config, list_of_vars):

        for curr_var in list_of_vars:
            build_config[curr_var]["show"] = False
            build_config[curr_var]["required"] = False
        
        return(build_config)



    # forecast_update_component_fields
    # Given a set of business rules in a structure shown below, run through the structure
    # and execute the fields per the rules.  This should reduce the amount of business logic
    # specific code written in each and every component to a simple structure that can be configured
    # 
    # INPUTS:
    #   build_config: Pydantic build_config structure
    #   biz_rules:  The structure shown below for executing the business rules on the form in the component
    #
    # OUTPUTS:
    #   build_config
    #
    # biz_rules structure:
    # {
    #     "var_name1": {
    #         "var_value1": {"action1": [], "action2": [], "action3": [], "action4": [], "action5": [], "action6": []},
    #         "var_value2": {"action1": [], "action2": [], "action3": [], "action4": [], "action5": [], "action6": []},
    #     },
    #     "input_type": {
    #         ForecastModelInputTypes.TIME_BASED: {"show": ["time_scale",], "toggle": [], "show_required": [], "show_optional": [], "hide": ["growth_rate", "patient_count"], "trigger": []},
    #         ForecastModelInputTypes.SINGLE_INPUT: {"show": ["growth_rate", "patient_count"], "toggle": [], "show_required": [], "show_optional": [], "hide": ["time_scale", "patient_count_table", "month_start_of_fiscal_year"], "trigger": []},
    #     },    
    # }

    def forecast_update_fields(self, build_config, biz_rules, only_shown_fields = False):
        # if there are no keys in the biz_rules, raise an error
        if(len(biz_rules.keys()) < 1):
            raise ValueError(f'Unable to run forecast_update_fields:  no biz_rules provided.')

        # iterate over all the 'var_name's in the biz rules structure
        for var_name in biz_rules.keys(): # var_name1, var_name2, etc. etc.

            # check if the current 'var_name' exists in build_config (if not, throw an error, because that shouldn't happen, unless there was a misconfiguration/typo)
            if var_name not in build_config.keys():
                raise ValueError(f'Unable to run forecast_update_fields: {var_name} does not exist in the build_config.')
            
            # if 'only_shown_fields' flag is on, then only check fields who are currently set-up for as "show"=True
            if(build_config[var_name]["show"] == False):
                continue
            
            # if it's a legitimate field, grab the value (var_value) of the field in build_config and
            # find out what business rules (if any) are assigned to that var_value
            var_value = build_config[var_name]["value"]

            # check if the value is covered by one of the existing rules, if not, we can skip ahead to the next
            # for-iteration
            if var_value not in biz_rules[var_name].keys():
                continue

            # get the rule to execute
            var_value_actions = biz_rules[var_name][var_value]

            # if there are no keys, skip this step
            if(len(var_value_actions) == 0):
                continue

            # for each action rule, get the list of fields affected and then dispatch to the correct
            # handler function.  If there is no match for the action in our dispatchers, throw an error
            for action in var_value_actions.keys():
                action_fields = biz_rules[var_name][var_value][action]

                if(len(action_fields) == 0):
                    continue

                match action:
                    case self.RULE_TYPES.SHOW:
                        build_config = self.forecast_show_fields(build_config, action_fields)
                    case self.RULE_TYPES.TOGGLE:
                        build_config = self.forecast_toggle_show_fields(build_config, action_fields)
                    case self.RULE_TYPES.SHOW_REQUIRED:
                        build_config = self.forecast_show_required_fields(build_config, action_fields)
                    case self.RULE_TYPES.SHOW_OPTIONAL:
                        build_config = self.forecast_show_optional_fields(build_config, action_fields)
                    case self.RULE_TYPES.HIDE:
                        build_config = self.forecast_hide_fields(build_config, action_fields)
                    case _:
                        raise ValueError(f'Uanble to run forecast_update_fields: for {var_name} invalid rule {action}.')
                
        return(build_config)
