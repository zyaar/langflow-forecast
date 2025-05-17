#####################################################################
# ForecastFormTriggerCalc
#
# A helper function that is given a series of calculated variables
# and their upgrade dependencies fields.  If one of those fields 
# changes as part of 
#
# 
# Business rules structure to feed in:
# biz_rules = {
#     "dep_var_1": ("update_func_1", ["src_var_1", "src_var_2", "src_var_3"]),
#     "dep_var_2": ("update_func_2", ["src_var_2", "src_var_3"]),
#     "dep_var_3": ("update_func_3", ["src_var_2", "src_var_3"]),
# }
#####################################################################

# COMMON IMPORTS
# ==============


class ForecastFormTriggerCalc():

    # update_config
    # Reads in the config file and replaces the old one.  Then creates a reverse config file so that it's quick to check all 
    # 
    # INPUTS:
    #   biz_rules:  The structure shown above for executing the business rules on the form in the component (see file header for examples and information)
    # 
    # OUTPUTS:
    #   NONE
    def update_config(self, config):
        self.config = config
        self.rev_config = {}

        # the config file is created the mirror the business logic, take make it easy
        # to enter, but not fast to execute.  So we create a converse config which lists
        # every src_var's and every var which is dependent on it.
        # create the reverse_config
        for dep_var in config.keys():
            src_vars = config[dep_var][1]

            for src_var in src_vars:
                if src_var in self.rev_config.keys():
                    self.rev_config[src_var].append(dep_var)
                else:
                    self.rev_config[src_var] = [dep_var]

        return(0)


    # forecast_update_component_fields
    # TODO
    # 
    # INPUTS:
    # TODO
    #
    # OUTPUTS:
    # TODO

    def forecast_update_fields(self, build_config, form_trigger_rules, field_value, field_name, **kwargs):

        # store the config information, create a reverse_config (fields that trigger dependencies)
        self.update_config(form_trigger_rules)

        # if the updated field (field_name) is in our reverse configuration (i.e. variables which have dependents),
        if field_name not in self.rev_config:
            return(build_config)
        
        # iterate over all dependents of field_name and run their update_function (provided in the kwargs)
        for dep_var in self.rev_config[field_name]:
            if(dep_var not in self.config):
                raise ValueError(f"Unable to run ForecastFormTriggerCalc: '{dep_var}' does not have an entry in the config.")
            
            function_to_call = self.config[dep_var][0]

            if function_to_call not in kwargs:
               raise ValueError(f"Unable to run ForecastFormTriggerCalc for '{dep_var}': trigger action: invalid function '{function_to_call}'.")            
            
            build_config[dep_var]["value"] = kwargs[function_to_call]()

        return(build_config)