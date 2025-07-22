#####################################################################
# forecast_treatment_TB.py
#
# Implements the treatment component of the forecasting in a TIME BASED model.
# This component manages the progression curve (in months) for patients in a specific treatment
# as well as the product Rx provided at each step
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  DataFrame (ForecastDataModel format)
#
#####################################################################

from langflow.io import StrInput, DataInput, IntInput, TableInput
from langflow.schema import DataFrame, Data
from langflow.schema.table import EditMode
from langflow.template import Output
from langflow.field_typing.range_spec import RangeSpec


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.components.forecast_component import ForecastComponent
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)

# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List, Tuple



# CLASSES
# =======

# ForecastTreatmentTB
# This class represents applying a treatment regiment of products to an incoming patient flow
class ForecastTreatmentTB(ForecastComponent):

    # CONSTANTS
    # =========
    DEBUG_MODE = True
    MAX_TREATMENT_DURATION = 12*100    # max treatment duration is 100 years (in months)
    MAX_PRODUCTS = 100
    COL_PREFIX = "product_"
    NUM_STATIC_OUTPUTS = 2 # one static output (# patients leaving/month), rest is product
    NUM_STATIC_COLS = 2 # two static columns in table (month of pression, % of people progressing), rest is product
    NUM_STATIC_IGNORE_COLS = 0 # of static columns to ignore
    NUM_STATIC_INPUT_COLS = 2 # of static columsn to read as input (before the variable columns)

    MAX_TREATMENT_DURATION = 240 # max treatment duration supported is 20 years



    # COMPONENT META-DATA
    # ===================
    display_name: str = "Treatment TB"
    description: str = "Apply a treatment regiment of products to an incoming patient flow"
    icon = "Syringe"
    name: str = "TreatmentTB"




    # COMPONENT INPUTS
    # ================
    inputs = [
        # common forecast inputs
        *ForecastComponent.inputs,

        # dataframes in List[DataFrame]
        DataInput(
            name="forecasts_in",
            display_name="Forecast(s)",
            info="Time Based forecast(s) DataFrame(s)",
            dynamic=True,
            real_time_refresh=True,
            is_list = True,
        ),


        # treatment_duration
        IntInput(
            name="treatment_duration",
            display_name = "Treatment Duration",
            info="Total number of MONTHS in the treatment",
            value=0,
            dynamic=True,
            real_time_refresh=True,
            show = True,
            required = True,
            range_spec = RangeSpec(min=0, max=MAX_TREATMENT_DURATION)
        ),
        

        # num_products
        IntInput(
            name="num_products",
            display_name = "Number of products",
            info="Total number of products used in this treatment.",
            value=0,
            dynamic=True,
            real_time_refresh=True,
            show = True,
            required = True,
            range_spec = RangeSpec(min=0, max=MAX_PRODUCTS)
        ),
        
        # treatment_details
        TableInput(
            name="treatment_details",
            display_name="Treatment Details",
            info="Includes the pregression curve and the number and types of product (SKUs) provided at each month of the progression curve",
            required=True,
            show=True,
            dynamic=True,
            real_time_refresh=True,
            table_schema=[
                {
                    "name": "month",
                    "display_name": "Month",
                    "type": "int",
                    "description": "Months after treatment start",
                    "edit_mode": EditMode.INLINE,
                    "disable_edit": True,
                },
                {
                    "name": ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME,
                    "display_name": "Progression curve",
                    "type": "float",
                    "description": "For each time period, enter the % of patients from the start of the treatment who are still on the treatment.",
                    "edit_mode": EditMode.INLINE,
                    "disable_edit": False,
                },
            ],
            value=[],
        ),
    ]

    # OUTPUTS
    # =======
    outputs = [
        Output(display_name="# patient ON-THERAPY", name="patient_on_treatment", method="calc_patients_on_treatment"),
        Output(display_name="# patient LEAVING", name="patients_leaving_treatment", method="calc_patients_leaving_treatment"),
    ]



    # FORM UPDATE RULES
    # =================
    form_update_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_treatment_table_def", ["num_products", "treatment_duration"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("treatment_details", "generate_table_values", ["num_products", "treatment_duration"])),
    ]


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
                                                               field_name=field_name,
                                                               
                                                               # list of all the updater functions for calculated fields
                                                               update_treatment_table_def=self.generate_table_schema,
                                                               generate_table_values=self.generate_table_values)


        # return updated config         
        return(build_config)



    def update_outputs(self, frontend_node: dict, field_name: str, field_value: Any) -> dict:
        curr_prod_outputs = len(frontend_node["outputs"])-self.NUM_STATIC_OUTPUTS

        # check if this is an update to the number of segments, in which case we definitely need
        # to refresh the outputs... alternatively, it could be an update to something else, but
        # there is an edge case when the component first starts the number of outputs may not match
        # the number of segments, in which case, we need to do it anyway
        if(field_name == "num_products"):
            num_products = field_value
        else:
            num_products = self.num_products
 
        # check if the length of outputs is different than the value of num_products, if not, then return
        prod_outputs_to_add = num_products - curr_prod_outputs

        if(prod_outputs_to_add != 0):
            # if less value, then remove the last few nodes
            if(prod_outputs_to_add < 0):
                for i in range(-prod_outputs_to_add):
                    frontend_node["outputs"].pop()
        
            # if it's greater than, then add a bunch of product output nodes to the end
            else:
                for i in range(curr_prod_outputs, curr_prod_outputs + prod_outputs_to_add):
                    frontend_node["outputs"].append(Output(
                        name=f"{ForecastTreatmentTB.COL_PREFIX}{i+1}", 
                        display_name=f"Product {i+1} Rx", 
                        method=f"update_forecast_model_product_{i+1}"
                    ))

        return frontend_node
        


    # __getattribute__
    # Because Langflow does not allow calling methods in outputs with arguments, we need a way to generate a unique methe call for each 
    # of the variable outputs, but then convert those individual calls into a common method with a different argument for the segment number.
    # To do that, we created above individual methods "update_forecast_model_product_1", "update_forecast_model_product_2", "update_forecast_model_product_3", etc.
    # This function overrides the __getattribute__ method call which looks up the name of a function, takes the function name and parses out the segment id ("_3" -> int(3))
    # And then redirects that method call ("update_forecast_model_product_1"), to the generic method ("update_forecast_model_product"), but with a wrapper function around
    # it
    # 
    # INPUTS:
    #   func - the function call to the generic 'update_forecast_model_product'
    #   seg_num - integer segment number
    #
    # OUTPUTS:
    #   A wrapper around 'update_forecast_model_product' which will put in the the right segment number for the call

    def __getattribute__(self, attr):
        if attr.startswith("update_forecast_model_product_"):
            attribute = super().__getattribute__("update_forecast_model_product")

            if callable(attribute):
                seg_num = int(attr.split("_")[-1])
                return self.wrapper(attribute, seg_num)
            else:
                return attribute
        else:
            return super().__getattribute__(attr)


    # wrapper
    # Takes the segment number and puts a wrapper around the generic call which adds the segment number as an argument
    # 
    # INPUTS:
    #   func - the function call to the generic 'update_forecast_model_product'
    #   seg_num - integer segment number
    #
    # OUTPUTS:
    #   A wrapper around 'update_forecast_model_product' which will put in the the right segment number for the call

    def wrapper(self, func, seg_num):
        def new_funct(seg_num = seg_num, *args, **kwargs) -> Data:
            out = func(seg_num = seg_num, *args, **kwargs)
            return out
        
        return new_funct
    




    # COMMON PRE ALL OUTPUT CALLS
    # ---------------------------
    # calc_patients_treatment_common
    # Common code for outputs:  calc_patients_on_treatment, calc_patients_leaving_treatment
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    #def calc_patients_treatment_common(self, keep_granular: bool = False) -> dict[str, Tuple[DataFrame, DataFrame, DataFrame, ForecastMetaDataFrame]]:
    def calc_patients_treatment_common(self, keep_granular: bool = True):

        # Overall
        # Get the inbound totals in and related meta_data
        # run all validation, merging, etc. which is common to any update call
        # sum up all the inputs to create a single total line and add it to the output model


        # GET COMBINED TOTALS IN

       # run input validation
        self.validate_inputs()


        # AGGREGATE PATIENT FLOW INPUT DATA
        treatment_group_id = self._id

        # sum up all the inputs to create a single total line and add it to the output model
        (updated_model, updated_meta_data, col_total_in_id) = self.check_and_combine_forecasts(totals_display_name = f"{self.display_name} total patients in")
        print(updated_model)
        print(f"col_total_in_id={col_total_in_id}")
        
        # we may or may not have generated a totals column (if there was only one input, we don't, if there was >1 we do), so grab the
        # last ID in the updated_model so that we are pointing to the right totals column (new one or not)
        #col_total_in_id = updated_model.columns[-1]
        updated_model = ForecastDataModel.astype_first_all_cols(updated_model)
        print("Got here A")

        # get in totals_in values (in case we need to use it)
        col_total_in_values = updated_model[col_total_in_id]
        print(col_total_in_values)
        print("Got here B")


        # PROCESS AND SAVE TREATMENT DETAILS TABLE
        # make sure that data-types for treatment_details is set correctly
        treatment_details = ForecastDataModel.astype_first_all_cols(self.treatment_details, first_col_type="int")
        print("Got here C")


        # Create the data and meta-data for the treatment_details table... 
        # since this table is used in a manner so different from the model, 
        # we'll save it as objects instead a row of instructions to the builder on creating a new treatment for the player
        treatment_details_table_group_id = f"{treatment_group_id}_treatment_details"
        print("Got here D")

        (treatment_details_model, treatment_details_meta_data) = self.create_treatment_data_meta_data(treat_group_id = treatment_details_table_group_id,
                                                                                                      table_name = "treatment_details", 
                                                                                                      treatment_details = treatment_details)

        print("Got here E")
        # Add a treatment set-init instructions for a treatment section to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{treatment_group_id}_Init",
                                                                    display_name = self.display_name,
                                                                    data_values = col_total_in_values,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    args = {ForecastDataSeriesMetaDataAction.STEP_INIT: ForecastDataSeriesMetaDataAction.YEAR_TO_MONTH},
                                                                    pred = [col_total_in_id],
                                                                    objs = {"data": treatment_details_model, "meta_data": treatment_details_meta_data})

        print("Got here F")
        (pat_on_treatment_month, pat_leaving_month, updated_meta_data) = ForecastDataModel.calc_treatment_pat_forecast(component_id = self._id,
                                                                                                                       updated_model = updated_model,
                                                                                                                       updated_meta_data = updated_meta_data,
                                                                                                                       treatment_table_col_prefix = f"{treatment_details_table_group_id}",
                                                                                                                       treatment_details_model = treatment_details_model,
                                                                                                                       forecast_timescale = self.timescale,
                                                                                                                       patient_progression_colname = ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME,
                                                                                                                       pc_initial_state = None,
                                                                                                                       keep_granular = keep_granular)

        print("Got here G")

        return({
            "pat_on_treatment": (pat_on_treatment_month, treatment_details_model, updated_model, updated_meta_data),
            "pat_leaving_treatment": (pat_leaving_month, treatment_details_model, updated_model, updated_meta_data),
        })
        #return (pat_on_treatment_month, pat_leaving_month, treatment_details, updated_model)
    




    def create_treatment_data_meta_data(self, treat_group_id: str, table_name: str, treatment_details: DataFrame) -> tuple[(DataFrame, ForecastMetaDataFrame)]:

        # Create an empty dataframe which we will build up using the same ids as we do with the meta-data
        updated_model = DataFrame()

        # create data structure to hold the treatment details data... 
        # generate the meta-dataframe
        updated_meta_data = ForecastMetaDataFrame(input_type = ForecastModelInputTypes.TREATMENT_DETAILS,
                                                  timescale = ForecastModelTimescale.MONTH,
                                                  start_year = None,
                                                  start_month = 1,
                                                  num_periods = int(len(treatment_details)),)
        
        # Add Months / Dates column
        col_name = "month"
        col_treat_col_values = treatment_details[col_name]
        col_treat_col_id = f"{treat_group_id}_{col_name}"


        (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = col_treat_col_id,
                                                                    display_name = self.get_input_table_col_display_name(table_name = table_name, col_name = col_name),
                                                                    data_values = col_treat_col_values,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.DATES,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],)

        # Add Progression
        col_name = ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME
        col_treat_col_values = treatment_details[col_name]
        col_treat_col_id = f"{treat_group_id}_{col_name}"

        (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = col_treat_col_id,
                                                                    display_name = self.get_input_table_col_display_name(table_name = table_name, col_name = col_name),
                                                                    data_values = col_treat_col_values,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.DATES,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}],)

        # Add the variable number of products
        num_cols = len(treatment_details.columns)

        for i in range(self.NUM_STATIC_COLS, num_cols):
            col_name = treatment_details.columns[i]
            col_treat_prod_values = treatment_details[col_name]
            col_treat_prod_id = f"{treat_group_id}_{col_name}" # TODO:  Fix when we have a better way of setting names


            # add the product's number of Rx's per month to meta-data
            (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
                                                                        updated_meta_data,
                                                                        id = col_treat_prod_id ,
                                                                        display_name = self.get_input_table_col_display_name(table_name = table_name, col_name = col_treat_prod_id),
                                                                        data_values = col_treat_prod_values,
                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                        action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                        data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                        display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}])

        return(updated_model, updated_meta_data)








    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        # treatment_duration > 0
        if(self.treatment_duration < 1):
            msg += f"\n* '{self.get_input_display_name("treatment_duration")} must be > 0'."


        # num_products > 0
        if(self.num_products < 1):
            msg += f"\n* '{self.get_input_display_name("num_products")} must be > 0'."


        # treatment_details
        if(self.treatment_details is None or not isinstance(self.treatment_details, list) or len(self.treatment_details) < 1):
            msg += f"\n* Missing values for '{self.get_input_display_name("treatment_details")}'."
                    
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------

    # OUTPUT FUNCTIONS

    # calc_patients_on_treatment
    # return the total number of patients on treatment for use in downstream nodes
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timeperiod and treatment stage
    def calc_patients_on_treatment(self) -> Data:
        results = self.calc_patients_treatment_common(keep_granular = False)
        (pat_on_treatment_month, treatment_details, updated_model, updated_meta_data) = results["pat_on_treatment"]
        updated_model = ForecastDataModel.concat([updated_model, pat_on_treatment_month])

        # bundle the packet together for forwarding to next component(s)
        data_packet = self.gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data, check_ids = not self.DEBUG_MODE)
        return(data_packet)



    # calc_patients_leaving_treatment
    # return the total number of patients that leave treatment at every timescale sample
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def calc_patients_leaving_treatment(self) -> Data:
        results = self.calc_patients_treatment_common(keep_granular = False)
        (pat_leaving_month, treatment_details,  updated_model, updated_meta_data) = results["pat_leaving_treatment"]
        updated_model = ForecastDataModel.concat([updated_model, pat_leaving_month])

        # bundle the packet together for forwarding to next component(s)
        data_packet = self.gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data, check_ids = not self.DEBUG_MODE)
        return(data_packet)
    


    # generate_forecast_model_segment
    # Add the segment % and the new total patients to the model
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_product(self, seg_num=1) -> Data:

        # run the common activities
        results = self.calc_patients_treatment_common(keep_granular = True)
        (total_pat_by_month_in_treat, treatment_details, updated_model, updated_meta_data) = results["pat_on_treatment"]
        (pat_leaving_month, treatment_details,  updated_model, updated_meta_data) = results["pat_leaving_treatment"]

        product_col_prefix = f"{self._id}_treatment_details_product_"
        product_use_colnames = [colname for colname in treatment_details.columns.to_list() if colname.startswith(product_col_prefix)]
        product_use_in_treatment_by_month = treatment_details[product_use_colnames]
        product_use_in_treatment_by_month = ForecastDataModel.calc_treatment_rx_forecast_for_product(treatment_table_colname = f"{product_col_prefix}{seg_num}", # f"{self._id}_treatment_details_product_{seg_num}",
                                                                                                     product_rx_colname_prefix = f"{self._id}_{self.COL_PREFIX}{seg_num}_rxs_for_patients_in_treatment",
                                                                                                     treatment_pat_by_month_forecast = total_pat_by_month_in_treat,
                                                                                                     product_use_in_treatment_by_month = product_use_in_treatment_by_month,
                                                                                                     forecast_timescale = ForecastModelTimescale.MONTH, # we hardcode the timescale for monthly, because we will receive monthly for prev step
                                                                                                     convert_timescale = self.timescale) # but we override with a convert to the actual timescale we have later, so that the results we provide are in the right timescale

        # add these results to merged model to updated_model (the merged results of model) to get the final results and return them
        # if the current timescale YEARLY, adjust pat_on_treatment_month before concat
        if(self.timescale != ForecastModelTimescale.MONTH):
            total_pat_by_month_in_treat = ForecastDataModel.monthly_to_yearly(total_pat_by_month_in_treat)

            # we don't currently use this variable, but if we do, I know I won't remember to convert from monthly to yearly, so putting it in here pre-emptively
            pat_leaving_month = ForecastDataModel.monthly_to_yearly(pat_leaving_month) 

        updated_model = ForecastDataModel.concat([updated_model, total_pat_by_month_in_treat, product_use_in_treatment_by_month])
        
        # bundle the packet together for forwarding to next component(s)
        data_packet = self.gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data, check_ids = not self.DEBUG_MODE)
        return(data_packet)







    # OUTPUT HELPERS

    # generate_table_schema
    # Generates the schema for the treatment details table given the total number of products, now available
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config
    def generate_table_schema(self, build_config, field_value, field_name):

        # get the latest num_products
        if(field_name == "num_products"):
            num_products = int(field_value)
        else:
            num_products = self.num_products

        # if the current table_schema has the same number of products as num_products, then skip
        if(len(build_config["treatment_details"]["table_schema"]["columns"])-self.NUM_STATIC_COLS == num_products):
            return(build_config)

        # otherwise, rebuild the table schema by taking the static columns, and then adding the correct number of num_product columns
        table_schema = build_config["treatment_details"]["table_schema"]["columns"][:self.NUM_STATIC_COLS]

        for i in range(num_products):
            table_schema.append({
                "name": f"{ForecastTreatmentTB.COL_PREFIX}{i+1}",
                "display_name": f"Product {i+1} Rx",
                "type": "float",
                "description": f"Number of prescriptions of product {i+1}, for the N's time period of a treatment",
                "disable_edit": False,
                "sortable": False,
                "filterable": False,
                "edit_mode": EditMode.INLINE,
            })

        build_config["treatment_details"]["table_schema"]["columns"] = table_schema
        return(build_config)


    # generate_table_values
    # Based on the latest schema, generates the values for the table
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config
    def generate_table_values(self, field_value: str, field_name: str) -> List[dict]:
        if(field_name == "num_products"):
            num_products = int(field_value)
        else:
            num_products = self.num_products

        if(field_name == "treatment_duration"):
            treatment_duration = int(field_value)
        else:
            treatment_duration = self.treatment_duration

        # calculate how many rows and cols we need
        new_num_cols = self.NUM_STATIC_COLS + num_products
        new_num_rows = treatment_duration

        # Check if we have existing data
        old_values = self.treatment_details
        if(old_values is not None and isinstance(old_values, list) and len(old_values) > 0):
            new_df = ForecastFormModelUtilities.fill_drataframe(new_dim_rows = new_num_rows,
                                                                new_dim_cols = new_num_cols,
                                                                prev_data  = old_values, 
                                                                default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                                individual_default_col_values = {ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME: 1}, 
                                                                col_name_prefix = self.COL_PREFIX, 
                                                                num_static_cols = self.NUM_STATIC_COLS, 
                                                                month = list(range(1, new_num_rows+1)))
        else:
            new_df = ForecastFormModelUtilities.fill_drataframe(new_dim_rows = new_num_rows,
                                                                new_dim_cols = new_num_cols,
                                                                set_col_names = ["month", ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME],  
                                                                default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                                individual_default_col_values = {ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME: 1}, 
                                                                col_name_prefix = self.COL_PREFIX, 
                                                                num_static_cols = self.NUM_STATIC_COLS, 
                                                                month = list(range(1, new_num_rows+1)))
        
        
        return(new_df.to_data_list())
    
