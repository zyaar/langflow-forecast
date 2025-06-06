#####################################################################
# ForecastFormTriggerCalc
#
# This class allows for a consistent way to manage the business rules triggered by dynamic update to the
# Inputs in the template via configuation structure instead of IF/MATCH statements in the component itself.
# 
# NOTE:  This is not meant to handle the business rules around the actual execution of data processing for the
# component (i.e. "run()" function of CustomComponent).
# 
# The concept is that business rules are triggered during the 'update_build_config()' method call, which occurs
# whenever an Input field flagged as 'real_time_refresh=True" changes it's value.  The field name and new value
# are passed into 'update_build_config" which then triggers this object. 
# 
# This uses a dictionary called a 'reverse_config' to see if the field name has one or more rules associated with it
# and if it does, executes of of those rules (each rule as called 'action', and the list of rules assocated with that field
# is called an 'action_chain')
# rules assocated with it, and runs all the rules which are there.
# 
# Each action is classified by an action_type which represent a differen type of rule.  The current action_types supported are:
#
#   UPDATE_VALUE - Update the value of a specific Input field based on the results of a function called
#   RUN_FUCT - Run a function and allow it to manipulate the build_config
#   
# To support each action_type, do dispatcher handler functions are written:
# 1. parse_ACTION_TYPE - parses the business rules in the configuration for that action type, set-up the reverse configuation for that action_type
# 2. exec_ACTION_TYPE - take the entry for this action_type from the reverse configuration and execute it accordingly
# 
# Business rules configuration:
# biz_rules = [
#     (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("field_to_update", "funct_to_run", ["trigger_1", "trigger_2", "trigger_3"])),
#     (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("funct_to_run", ["trigger_1", "trigger_2", "trigger_3"])),
# ]
#
# REAL EXAMPLE from tests:
# # create fake biz rules
# form_trigger_rules = [
#     (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("patient_count_table", "generate_table_values", ["num_years", "start_year", "input_type", "time_scale", "month_start_of_fiscal_year"])),
#     (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("add_new_fields", ["input_type"])),
# ]
#
#
#####################################################################

# COMMON IMPORTS
# ==============
from enum import Enum
from typing import Tuple

class ForecastFormTriggerCalc():
    class TriggerType(str, Enum):
        UPDATE_VALUE = "Update Value"
        UPDATE_FORM = "Update Form"
        RUN_FUNCT = "Run Funct"
        
    # update_rev_config
    # Reads in the config file and creates a new rev_config.  The reverse config holds a dictionary of all the fields which can trigger 
    # actions, and the series of actions that need to be triggered for each one (called an action chaoin)
    # that need to be triggered.
    # 
    # INPUTS:
    #   biz_rules:  The structure shown above for executing the business rules on the form in the component (see file header for examples and information)
    # 
    # OUTPUTS:
    #   NONE
    def update_rev_config(self, config):
        self.config = config
        self.rev_config = {}

        # the config file is created the mirror the business logic, take make it easy
        # to enter, but not fast to execute.  So we create a converse config which lists
        # every src_var's and every var which is dependent on it.
        # create the reverse_config
        for i in range(len(config)):
            action_type = config[i][0] # action is always in the first field of the tuple
            row_to_parse = config[i][1]

            # depending on the action_type, dispatch a different funciton to parse and update
            # the rev_config
            match action_type:

                # UPDATE_VALUE
                case ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE:
                    self.parse_UPDATE_VALUE(row_to_parse)

                # RUN_FUNCT
                case ForecastFormTriggerCalc.TriggerType.RUN_FUNCT:
                    self.parse_RUN_FUNCT(row_to_parse)

                # UNKNOWN ACTION
                case _:
                    raise ValueError(f"update_config:  Unknown action_type '{action_type}'")   



    # execute_trigger
    # Given a field_name that has just changed in update_config, if the field_name has an action-chain
    # 
    # INPUTS:
    #   biz_rules:  The structure shown above for executing the business rules on the form in the component (see file header for examples and information)
    # 
    # OUTPUTS:
    #   NONE
    def execute_trigger(self, build_config, form_trigger_rules, field_value, field_name, **kwargs):
        # store the config information, create a reverse_config (fields that trigger dependencies)
        self.update_rev_config(form_trigger_rules)

        # if the updated field (field_name) is in our reverse configuration (i.e. variables which have dependents),
        if field_name not in self.rev_config.keys():
            return(build_config)
        

        # get the action chain (list of tuples) that needs to be activated whenever this trigger variable changes its value
        # run through each action in the list and dispatch to the approapriate action hander to execute
        action_chain = self.rev_config[field_name]

        for action in action_chain:
            action_type = action[0]

            match action_type:
                case ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE:
                    build_config = self.exec_UPDATE_VALUE(build_config, action, field_value, field_name, kwargs)
                case ForecastFormTriggerCalc.TriggerType.RUN_FUNCT:
                    build_config = self.exec_RUN_FUNCT(build_config, action, field_value, field_name, kwargs)
                case _:
                    raise ValueError(f"update_config:  Unknown action_type '{action_type}'")
                
        return(build_config)
    



    # HELPER FUNCTIONS
    # ----------------

    # UPDATE_VALUE
    # row_to_parse structure:  ("field_to_update", "funct_to_run", ["trigger_1", "trigger_2", "trigger_3"])
    # 
    # function_to_call has the form:
    #   function_to_call(self, field_value: str, field_name: str) -> 'value compatible with it's Input Type':
    def parse_UPDATE_VALUE(self, row_to_parse: Tuple):
        variable_to_update = row_to_parse[0]
        funct_to_run = row_to_parse[1]
        trigger_vars = row_to_parse[2]

        for trigger_var in trigger_vars:
            action_to_add = (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, variable_to_update, funct_to_run)
            self.add_to_rev(trigger_var, action_to_add)


    def exec_UPDATE_VALUE(self, build_config, action, field_value, field_name, kwargs):
        field_to_update = action[1]
        function_to_call = action[2]

        build_config[field_to_update]["value"] = kwargs[function_to_call](field_value, field_name) # NOTE:  need to explicity pass self to this function
        return(build_config)

    



    # RUN_FUNCT
    # row_to_parse structure:  ("funct_to_run", ["trigger_1", "trigger_2", "trigger_3"])
    # 
    # function_to_call has the form:
    #   function_to_call(self, build_config: dotdict, field_value: str, field_name: str) -> 'updated_biuld_config' dotdict:
    def parse_RUN_FUNCT(self, row_to_parse: Tuple):
        funct_to_run = row_to_parse[0]
        trigger_vars = row_to_parse[1]

        for trigger_var in trigger_vars:
            action_to_add = (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, funct_to_run)
            self.add_to_rev(trigger_var, action_to_add)


    def exec_RUN_FUNCT(self, build_config, action, field_value, field_name, kwargs):
        function_to_call = action[1]
        build_config = kwargs[function_to_call](build_config, field_value, field_name)
        return(build_config)



    # add_to_rev
    # add a tuple to the trigger_var in the reverse_config
    def add_to_rev(self, trigger_var: str, action_to_add: Tuple):
            
            # add this action to the trigger var chain of actions
            if(trigger_var not in self.rev_config.keys()):
                self.rev_config[trigger_var] = [action_to_add]
            else:
                self.rev_config[trigger_var].append(action_to_add)


