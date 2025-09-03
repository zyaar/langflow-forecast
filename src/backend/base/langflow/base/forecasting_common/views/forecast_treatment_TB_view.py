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

from langflow.custom import Component
from langflow.io import StrInput, DataInput, IntInput, TableInput, NestedDictInput, DictInput
from langflow.schema import DataFrame, Data
from langflow.schema.table import Column
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

from langflow.base.forecasting_common.components.forecast_sum_input_TB import ForecastSumInputTB

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
                                                                        ForecastDataSeriesMetaDataComparisonType,
                                                                        ForecastMetaDataRange,
                                                                        ForecastMetaDataRangeSchema,
                                                                        ForecastDataSeriesMetaDataArgsTreatmentStepInit)


# COMPONENT SPECIFIC IMPORTS
# ==========================
from enum import Enum
from typing import Any, List, Tuple
from datetime import datetime
import copy
import pandas as pd
from langflow.base.forecasting_common.controllers.forecast_treatment_TB_controller import ForecastTreatmentTBController




# CLASSES
# =======

class ForecastTreatmentStepInitArgs(str, Enum):
    TREATMENT_TABLE_DATA = "treatment_table_data"
    TREATMENT_TABLE_META_DATA = "treatment_table_meta_data"
    PRE_FORECAST_INPUTS_DATA = "pre_forecast_inputs_data"
    PRE_FORECAST_INPUTS_META_DATA = "pre_forecast_inputs_meta_data"
    PRE_FORECAST_PATIENT_FLOW_DATA = "pre_forecast_patient_flow_data"
    PRE_FORECAST_PATIENT_FLOW_META_DATA = "pre_forecast_patient_flow_meta_data"

# ForecastTreatmentTB
# This class represents applying a treatment regiment of products to an incoming patient flow
class ForecastTreatmentTBView(ForecastSumInputTB, Component):


    # CONFIG CONSTANTS
    # ================

    # COMPONENT
    display_name: str = "Treatment TB"
    description: str = "Apply a treatment regiment of products to an incoming patient flow"
    icon = "Syringe"
    name: str = "TreatmentTBView"

    # OUTPUT INFO
    NUM_STATIC_OUTPUTS = 2 # one static output (# patients leaving/month), rest is product

    # ROW_SET VAR
    MAX_TREATMENT_DURATION = 240 # max treatment duration supported is 20 years

    # COL_SET VAR
    MAX_PRODUCTS = 100
    COL_PREFIX = "product"
    MONTH_PREFIX = "month"

    # TABLE
    TABLE_NAME = "treatment_details"
    TABLE_SCHEMA_INPUT_NAME = f"hidden_treatment_details"
    NUM_STATIC_COLS = 2 # two static columns in table (month of pression, % of people progressing), rest is product
    NUM_STATIC_IGNORE_COLS = 0 # of static columns to ignore
    NUM_STATIC_INPUT_COLS = 2 # of static columns to read as input (before the variable columns)
    NUM_STATIC_OUTPUTS = 2 # the static outputs that don't change as we change the number of products (variable outputs) (i.e. # patients ON-THERAPY, # patients LEAVING)
    STATIC_OUTPUTS_AT_START = True

    # MISC
    CHECK_OUTPUT_ID = False



    # INSTANCE ATTRIBUTES
    # generated during the __init__
    # -----------------------------
    # controller - (ForecastTreatmentTBController) has the business logic for component



    # INIT
    # ====
    def __init__(self, **kwargs) -> None:
        # set-up a controller if needed
        if not hasattr(self, "controller"):
            self.controller = ForecastTreatmentTBController()

        super().__init__(**kwargs)


    
    # GENERATE INPUTS / OUTPUTS
    # =========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            *super()._gen_inputs(),

            # hidden field with treatment duration latest config
            NestedDictInput(
                name=self.TABLE_SCHEMA_INPUT_NAME,
                required = False,
                dynamic = True,
                real_time_refresh = True,
                advanced = True,
                value = {},
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
                range_spec = RangeSpec(min=0, max=self.MAX_TREATMENT_DURATION)
            ),

            # num_products
            IntInput(
                name="num_products",
                display_name = "Number of Products",
                info="Total number of products used in this treatment.",
                value=0,
                dynamic=True,
                real_time_refresh=True,
                show = True,
                required = True,
                range_spec = RangeSpec(min=0, max=self.MAX_PRODUCTS)
            ),


            # product_names
            TableInput(
                name="product_names",
                display_name="Product names",
                info="Display names for each product",
                required=True,
                show=True,
                dynamic=True,
                real_time_refresh=True,
                table_schema=[
                    {
                        "name": "prod_num",
                        "display_name": "ID",
                        "type": "int",
                        "description": "The id of the product.",
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": True,
                    },
                    {
                        "name": "prod_name",
                        "display_name": "Name",
                        "type": "str",
                        "description": "Name of the product",
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": False,
                    },
                ],
                value=[],
            ),


            # treatment_details
            
            TableInput(
                name=ForecastTreatmentTBView.TABLE_NAME,
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

        return(inputs_list)



    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),
            Output(display_name="# patient ON-THERAPY", name="patient_on_treatment", method="calc_patients_on_treatment"),
            Output(display_name="# patient LEAVING", name="patients_leaving_treatment", method="calc_patients_leaving_treatment"),
        ]

        return(outputs_list)





    # INPUT/OUTPUT VALIDATIONS
    # ========================
    def validate_inputs(self):
        super().validate_inputs()

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
        
        
        

    # FORM UPDATE RULES
    # =================

    form_update_rules = {}
    form_trigger_rules = [
        # update_prod_table_display_names
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_treatment_table_def", ["num_products", "treatment_duration"])),
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_prod_table_display_names", ["product_names"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("product_names", "generate_table_prod_name_values", ["num_products"])),
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
                                                               update_prod_table_display_names = self.update_prod_table_display_names,
                                                               generate_table_prod_name_values = self.generate_table_prod_name_values,
                                                               generate_table_values=self.generate_table_values)

        # return updated config         
        return(build_config)




    # OUTPUT UPDATES
    # ==============

    # Updates real_time_refreshing OUTPUT fields whenever an update happens from a dynamic field
    def update_outputs(self, frontend_node, field_name: str, field_value: Any) -> dict:

        new_output_config = frontend_node.copy()

        # get num_products
        if (field_name == "num_products") and (isinstance(field_value, int | str)):
            num_products = int(field_value)
        elif (hasattr(self, "num_products")):
            num_products = int(self.num_products)
        else:
            num_products = 0

        num_outputs = len(new_output_config["outputs"])

        # remove outputs
        if(num_outputs > num_products + self.NUM_STATIC_OUTPUTS):
            # determine number to remove
            num_remove = num_outputs - (num_products + self.NUM_STATIC_OUTPUTS)

            # pop the required number of outputs off the END of the list
            for i in range(num_remove):
                new_output_config["outputs"].pop()

        # add outputs
        elif(num_outputs < num_products + self.NUM_STATIC_OUTPUTS):
            # determine number to add
            num_add = (num_products + self.NUM_STATIC_OUTPUTS) - num_outputs

            for i in range(num_add):
                new_display_name = f"Product {i+1}"

                new_output_config["outputs"].append(Output(name = f"{ForecastTreatmentTBView.COL_PREFIX}_{i+1}", 
                                                           display_name = new_display_name, 
                                                           method = f"update_forecast_model_product_{i+1}",
                                                           group_outputs = True))
                
        # already equal, do nothing
        else:
            # same number of outputs, no more is required
            pass

        # go through all the new_outputs making sure that they have the latest product names
        for i in range(num_products):
            new_display_name = self.get_prod_display_name(i)

            if(new_display_name is None):
                new_display_name = f"Product {i+1}"

            new_output_config["outputs"][i+self.NUM_STATIC_OUTPUTS].display_name = new_display_name

        return(new_output_config)



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
    



    # OUTPUT FUNCTIONS
    # ================

    # calc_patients_on_treatment
    # return the total number of patients on treatment for use in downstream nodes
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timeperiod and treatment stage
    def calc_patients_on_treatment(self) -> Data:
        results = self._forecast_model_common_input(keep_granular = False)
        
        updated_model = results["pat_on_treatment"]["pat_by_treatment_month_data"]
        updated_meta_data = results["pat_on_treatment"]["pat_by_treatment_month_meta_data"]

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, check_ids = self.CHECK_OUTPUT_ID)



    # calc_patients_leaving_treatment
    # return the total number of patients that leave treatment at every timescale sample
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def calc_patients_leaving_treatment(self) -> Data:
        results = self._forecast_model_common_input(keep_granular = False)
        updated_model = results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_data"]
        updated_meta_data: ForecastMetaDataFrame = results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_meta_data"]

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, check_ids = self.CHECK_OUTPUT_ID)


    # update_forecast_model_product
    # return the total number of patients on treatment for use in downstream nodes AND the total number of Rx each munch for a specific product (seg_num)
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_product(self, seg_num=1) -> Data:
        results = self._forecast_model_common_input(keep_granular = True)

        # unpack results
        treatment_details_model = results["pat_on_treatment"]["treatment_table_data"]
        treatment_details_meta_data = results["pat_on_treatment"]["treatment_table_meta_data"]

        updated_data= results["pat_on_treatment"]["pat_by_treatment_month_data"]
        updated_meta_data = results["pat_on_treatment"]["pat_by_treatment_month_meta_data"]

        updated_data = results["pat_on_treatment"]["updated_data"]

        # calculate the number of Rx for this product, total and by treatment month
        treatment_group_id = self._id

        product_id = f"{ForecastTreatmentTBView.COL_PREFIX}_{seg_num}"
        product_display_name = DataFrame(self.product_names)["prod_name"][seg_num-1] # subtract 1 because seg_num starts at 1, not 0

        (updated_data, updated_meta_data) = self.controller.calc_treatment_rx_forecast_for_product(seg_num = seg_num,
                                                                                                   # self variables passed in
                                                                                                   treatment_id = self._id,
                                                                                                   treatment_display_name = self.get_display_name(),
                                                                                                   product_id = product_id,
                                                                                                   product_display_name = product_display_name,
                                                                                                   month_prefix = self.MONTH_PREFIX,

                                                                                                   # current forecast
                                                                                                   updated_data = updated_data,
                                                                                                   updated_meta_data = updated_meta_data,

                                                                                                   # treatment details table
                                                                                                   treatment_table_data = treatment_details_model,
                                                                                                   treatment_table_meta_data = treatment_details_meta_data)

        # if the current forecast timescale is yearly, we need to convert the updated data and meta-data to yearly as well
        if(self.timescale == ForecastModelTimescale.YEAR):
            (updated_data, updated_meta_data, last_col_id) = ForecastDataModel.convert_timescale(updated_data, updated_meta_data, target = ForecastModelTimescale.YEAR)

        # TREATMENT STEP_END
        
        # add the dates into the values (used only to get the value length)
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{treatment_group_id}_End",
                                                                    display_name = self.get_display_name(),
                                                                    data_values = updated_meta_data.get_series(updated_meta_data.get_last_value_id()).get_data_values(),
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_END,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [updated_meta_data.get_last_value_id()],
                                                                    update_last_id = True,)
                
        return self._forecast_model_common_output(data = updated_data, meta_data = updated_meta_data, check_ids = self.CHECK_OUTPUT_ID)



    # INPUT/OUTPUTS CALCULATIONS
    # ==========================

    # calc_patients_treatment_common
    # Common code for outputs:  calc_patients_on_treatment, calc_patients_leaving_treatment
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    # (treatment_details_model, treatment_details_meta_data, pat_on_treatment_data, pat_on_treatment_meta_data, updated_model) 
    def _forecast_model_common_input(self, keep_granular: bool = True) -> dict[str, list]:
        (updated_model, updated_meta_data, col_total_in_id, curr_display_name) = super()._forecast_model_common_input()

        # if the treatment duration is only 1 month, we don't need to calculate any pre-forecast data
        if(self.treatment_duration > 1):
            need_pre_forecast_data = True
        else:
            need_pre_forecast_data = False

        # if the current forcast is YEARLY, convert to MONTHLY to get the ealiest date, otherwise, just grab the first date
        if(updated_meta_data.get_timescale() != ForecastModelTimescale.MONTH):
            converted_dates = ForecastDataModel.conv_forecast_dates_yearly_to_monthly(data = updated_model[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME].to_list())
            earliest_date = converted_dates[0]
        else:
            earliest_date = updated_meta_data.get_first_date()

        # setup pre_forecast_patient_flow (currently disabled), TODO:  implement an input to allow the setting of initial state
        # ZIV
        #pre_forecast_patient_flow = [ForecastDataModel.EDITABLE_VALUES_TOKEN] * (self.treatment_duration-1)
        pre_forecast_patient_flow = list(range(100,(self.treatment_duration)*100, 100)) # testing set for pc initial state

        # PROCESS AND SAVE TREATMENT DETAILS TABLE
        treatment_group_id = self._id

        # make sure that data-types for treatment_details is set correctly
        treatment_details = ForecastDataModel.astype_first_all_cols(self.treatment_details, first_col_type="int")
        product_names= DataFrame(self.product_names)

        # Create treatment table meta_data
        # Create the data and meta-data for the treatment_details table... 
        # since this table is used in a manner so different from the model, 
        # we'll save it as objects instead a row of instructions to the builder on creating a new treatment for the player
        treatment_details_table_group_id = f"{treatment_group_id}_treatment_details"
        (treatment_details_model, treatment_details_meta_data, pc_col_id) = self.create_treatment_data_object(id = treatment_details_table_group_id,
                                                                                                              table_name = "treatment_details", 
                                                                                                              treatment_details = treatment_details,
                                                                                                              product_names = product_names)
        
        if(need_pre_forecast_data):
            # create pre-forecast inputs meta_data
            pre_forecast_table_group_id = f"{treatment_group_id}_pre_forecast_inputs"
            (pre_forecast_inputs_model, pre_forecast_inputs_meta_data, pf_col_id, last_date) = self.create_pre_forecast_inputs_object(id = pre_forecast_table_group_id,
                                                                                                                                    pre_forecast_patient_flow = pre_forecast_patient_flow,
                                                                                                                                    first_forecast_date = earliest_date)

            # create prior month patient flow
            pre_forecast_patient_flow_group_id = f"{treatment_group_id}_prior_month_patient_flow"
            (pre_forecast_patient_flow_model, pre_forecast_patient_flow_meta_data, pmpf_col_prefix) = self.create_prior_month_patient_flow_object(id = pre_forecast_patient_flow_group_id,
                                                                                                                                                target_date = last_date,
                                                                                                                                                treatment_details_model = treatment_details_model,
                                                                                                                                                treatment_details_meta_data = treatment_details_meta_data,
                                                                                                                                                pc_col_id = pc_col_id,
                                                                                                                                                pre_forecast_inputs_model = pre_forecast_inputs_model,
                                                                                                                                                pre_forecast_inputs_meta_data = pre_forecast_inputs_meta_data,
                                                                                                                                                pf_col_id = pf_col_id)
        else:
            pre_forecast_inputs_model = None
            pre_forecast_inputs_meta_data = None
            pre_forecast_patient_flow_model = None
            pre_forecast_patient_flow_meta_data = None
            pf_col_id = None
            pmpf_col_prefix = None

        

        # TREATMENT STEP_INIT
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{treatment_group_id}_Init",
                                                                    display_name = self.get_display_name(),
                                                                    #data_values = updated_model[updated_meta_data.get_last_id()].to_list(),
                                                                    data_values = None,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    update_last_id = True,
                                                                    args = {ForecastDataSeriesMetaDataArgsTreatmentStepInit.NEED_PRE_FORECAST_DATA: need_pre_forecast_data},
                                                                    pred = [col_total_in_id],
                                                                    objs = {ForecastTreatmentStepInitArgs.TREATMENT_TABLE_DATA.value: treatment_details_model, 
                                                                            ForecastTreatmentStepInitArgs.TREATMENT_TABLE_META_DATA.value: treatment_details_meta_data,
                                                                            ForecastTreatmentStepInitArgs.PRE_FORECAST_INPUTS_DATA.value: pre_forecast_inputs_model,
                                                                            ForecastTreatmentStepInitArgs.PRE_FORECAST_INPUTS_META_DATA.value: pre_forecast_inputs_meta_data,
                                                                            ForecastTreatmentStepInitArgs.PRE_FORECAST_PATIENT_FLOW_DATA.value: pre_forecast_patient_flow_model,
                                                                            ForecastTreatmentStepInitArgs.PRE_FORECAST_PATIENT_FLOW_META_DATA.value: pre_forecast_patient_flow_meta_data})
        
        # this function can only work with monthly data, so model is set to yearly, convert here to monthly
        if(updated_meta_data.get_timescale() != ForecastModelTimescale.MONTH):
            (updated_model, updated_meta_data, col_total_in_id) = ForecastDataModel.convert_timescale(data_model = updated_model,
                                                                                                      meta_data = updated_meta_data, 
                                                                                                      target = ForecastModelTimescale.MONTH, 
                                                                                                      step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)


        # CALCULATE THE PATIENTS ON TREATMENT AND PATIENTS LEAVING TREATMENT
        # generates the patients on therapy by forecast month and month of treatment, the patients leaving therapy for forecast month and month of therapy
        # returns as a dict of two elements in the following format (need to update this)
        # return({"pat_on_treatment": (treatment_table_data, treatment_table_meta_data, pat_by_treatment_month_data, pat_by_treatment_month_meta_data, updated_data),
        #         "pat_leaving_treatment": (treatment_table_data, treatment_table_meta_data, pat_leaving_by_treatment_month_data, pat_leaving_by_treatment_month_meta_data, updated_data)})
    
        results = self.controller.calc_treatment_pat_forecast(
            # self variables
            id = self._id,
            display_name = self.get_display_name(),
            month_prefix = self.MONTH_PREFIX,

            # current forecast
            updated_data = updated_model,
            updated_meta_data = updated_meta_data,
            
            # treatment details table
            treatment_table_data = treatment_details_model,
            treatment_table_meta_data = treatment_details_meta_data,
            pc_col_id = pc_col_id,

            # pre-forecast input table
            pre_forecast_inputs_data = pre_forecast_inputs_model,
            pre_forecast_inputs_meta_data = pre_forecast_inputs_meta_data,
            pf_col_id = pf_col_id,

            # pre-forecast patient flow table
            pre_forecast_patient_flow_data = pre_forecast_patient_flow_model,
            pre_forecast_patient_flow_meta_data = pre_forecast_patient_flow_meta_data,
            pmpf_col_prefix = pmpf_col_prefix)
        

        # if keep_granular was set, then keep at the monthly granularity and return
        if keep_granular:
            return results
        
        # if keep_granular was NOT set, but the target forecast granularity is MONTHLY, then return
        elif self.timescale == ForecastModelTimescale.MONTH:
            return results
        
        # otherwise, convert the key data_models: "pat_on_treatment_data" and "pat_leaving_treatment_meta_data" back to YEARLY
        else:
            # update timescale for patients on treatment
            (updated_pat_on_treatment_model, updated_pat_on_treatment_meta_data_model, pat_on_treatment_col_total_in_id) = ForecastDataModel.convert_timescale(data_model = results["pat_on_treatment"]["pat_by_treatment_month_data"],
                                                                                                                                                               meta_data = results["pat_on_treatment"]["pat_by_treatment_month_meta_data"],
                                                                                                                                                               target = ForecastModelTimescale.YEAR,
                                                                                                                                                               step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)

            results["pat_on_treatment"]["updated_pat_on_treatment_model"] = updated_pat_on_treatment_model  # data
            results["pat_on_treatment"]["updated_pat_on_treatment_meta_data_model"] = updated_pat_on_treatment_meta_data_model # meta_data

            # update timescale for patients leaving treatment
            (updated_pat_leaving_by_treatment_month_data, updated_pat_leaving_by_treatment_month_meta_data, pat_leaving_treatment_col_total_in_id) = ForecastDataModel.convert_timescale(data_model = results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_data"],
                                                                                                                                                                                         meta_data = results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_meta_data"],
                                                                                                                                                                                         target = ForecastModelTimescale.YEAR,
                                                                                                                                                                                         step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)
            
            results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_data"] = updated_pat_leaving_by_treatment_month_data
            results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_meta_data"] = updated_pat_leaving_by_treatment_month_meta_data

            return results




    # create_treatment_data_object
    # Create meta_data for the treatment details table (needs to be added separately into the meta_data)
    #
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    #   MetaDataFrame with the meta-data for the same thing
    #   id of the row that holds the progression curve

    def create_treatment_data_object(self, 
                                     id: str, 
                                     table_name: str, 
                                     treatment_details: DataFrame, 
                                     product_names: DataFrame) -> tuple[(DataFrame, ForecastMetaDataFrame, str, str)]:

        # Create an empty dataframe which we will build up using the same ids as we do with the meta-data
        updated_model = DataFrame()

        # create data structure to hold the treatment details data... 
        # generate the meta-dataframe
        updated_meta_data = ForecastMetaDataFrame(id = id,
                                                  input_type = ForecastModelInputTypes.TIME_BASED,
                                                  timescale = ForecastModelTimescale.MONTH,
                                                  start_year = None,
                                                  start_month = None,
                                                  num_periods = self.treatment_duration)
        
        # Add Months / Dates column
        col_name = "month"
        col_treat_col_values = treatment_details[col_name]
        col_treat_col_id = f"{col_name}"
        #col_treat_col_id = f"{id_prefix}_{col_name}"



        (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                                  updated_meta_data,
                                                                                  id = col_treat_col_id,
                                                                                  display_name = self._get_input_table_col_display_name(table_name = table_name, col = col_name),
                                                                                  data_values = col_treat_col_values,
                                                                                  step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                  action = ForecastDataSeriesMetaDataAction.VALUES,
                                                                                  data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                  display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                  update_last_id = True)

        # Add Progression
        col_name = ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME
        col_treat_col_values = treatment_details[col_name]
        col_treat_col_id = f"{col_name}"
        col_pc_col_id = col_treat_col_id

        (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                                  updated_meta_data,
                                                                                  id = col_treat_col_id,
                                                                                  display_name = self._get_input_table_col_display_name(table_name = table_name, col = col_name),
                                                                                  data_values = col_treat_col_values,
                                                                                  step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                  action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                                  data_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                                  display_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK},
                                                                                                {ForecastDataSeriesMetaDataValidationSchema.VALUE_CHECK: ForecastDataSeriesMetaDataComparisonType.LE}],
                                                                                  args = {ForecastDataSeriesMetaDataComparisonType.LE: 1},  # add argument with the value for LESS_EQUAL_THAN validation
                                                                                  update_last_id = True)

        # Add the variable number of products
        num_cols = len(treatment_details.columns) - self.NUM_STATIC_COLS

        for i in range(num_cols):
            col_name = treatment_details.columns[i+self.NUM_STATIC_COLS]
            col_display_name = product_names["prod_name"][i]
            col_treat_prod_values = treatment_details[col_name]
            col_treat_prod_id = f"{ForecastTreatmentTBView.COL_PREFIX}_{i+1}"



            # add the product's number of Rx's per month to meta-data
            (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                        updated_meta_data,
                                                                        id = col_treat_prod_id ,
                                                                        #display_name = self._get_input_table_col_display_name(table_name = table_name, col = col_treat_prod_id),
                                                                        display_name = col_display_name,
                                                                        data_values = col_treat_prod_values,
                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                        action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                        data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                        display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}],
                                                                        update_last_id = True)

        # Updated model, Updated meta_data, id of the PC row
        return(updated_model, updated_meta_data, col_pc_col_id)





    # create_pre_forecast_inputs_object
    # Create the ForecastMetaDataFrame and supporting DataFrame object holding the pre_forecast_inputs object
    #
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    #   MetaDataFrame with the meta-data for the same thing
    #   id of the row that holds the progression curve

    # ZIV
    def create_pre_forecast_inputs_object(self, id: str, pre_forecast_patient_flow: list[int], first_forecast_date: datetime) -> tuple[(DataFrame, ForecastMetaDataFrame, str, str, datetime)]:
        num_elements = self.treatment_duration-1


        # PRE_FORECAST DATES
        pre_forecast_dates = ForecastDataModel.gen_pre_dates(first_forecast_date = first_forecast_date, num_periods = num_elements, time_scale = ForecastModelTimescale.MONTH)
        last_date = pre_forecast_dates[-1]
        first_date = pre_forecast_dates[0]

        # create data structure to hold the pre_forecast_inputs 
        # generate the meta-dataframe
        pre_forecast_inputs_meta_data = ForecastMetaDataFrame(id = id,
                                                              input_type = ForecastModelInputTypes.TIME_BASED,
                                                              timescale = ForecastModelTimescale.MONTH,
                                                              start_year = first_date.year,
                                                              start_month = first_date.month,
                                                              num_periods = num_elements)
        
        # Create an empty dataframe which we will build up using the same ids as we do with the meta-data
        pre_forecast_inputs_data = DataFrame()


        # PRE_FORECAST DATES
        (pre_forecast_inputs_data, pre_forecast_inputs_meta_data) = ForecastComponent._add_col_data_meta(pre_forecast_inputs_data,
                                                                                                         pre_forecast_inputs_meta_data,
                                                                                                         id = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME,
                                                                                                         display_name = ForecastDataSeriesMetaDataDataType.DATE,
                                                                                                         data_values = pre_forecast_dates,
                                                                                                         step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                         action = ForecastDataSeriesMetaDataAction.DATES,
                                                                                                         data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                                                                                         display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                                                                                         validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                         update_last_id = True)


        # PRE_FORECAST PATIENT_FLOW INPUT
        pf_col_id = "patients_entering_treatment"
        (pre_forecast_inputs_data, pre_forecast_inputs_meta_data) = ForecastComponent._add_col_data_meta(pre_forecast_inputs_data,
                                                                                                         pre_forecast_inputs_meta_data,
                                                                                                         id = pf_col_id,
                                                                                                         display_name = "# Patients entering treatment (per month):",
                                                                                                         data_values = pre_forecast_patient_flow,
                                                                                                         step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                         action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                                                         data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                         display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                         validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}],
                                                                                                         update_last_id = True)

        # Updated model, Updated meta_data, id of the PC row
        return(pre_forecast_inputs_data, pre_forecast_inputs_meta_data, pf_col_id, last_date)




    # create_prior_month_patient_flow_object
    # Create an ForecastMetaDataFrame object to hold the caculations which create the forecasted patient flow for the month BEFORE the first month of the forecast.
    #
    # INPUTS:
    #   id = (str) the id_prefix to append to this models ID
    #   treatment_details_model  = (DataFrame) the data frame of values for the treatment details table
    #   treatment_details_meta_data = (ForecastMetaDataFrame) the object holding the meta data for the treatment details
    #   pre_forecast_input_model = (DataFrame_ the data frame of values for the pre_forecast inputs
    #   pre_forecast_input_meta_data = (ForecastMetaDataFrame) the object holding the meta_data for the pre_forecast inputs
    #
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    #   MetaDataFrame with the meta-data for the same thing
    #   id of the row that holds the progression curve

    def create_prior_month_patient_flow_object(self, 
                                               id: str,
                                               target_date: datetime,
                                               treatment_details_model: DataFrame, 
                                               treatment_details_meta_data: ForecastMetaDataFrame,
                                               pc_col_id: str,
                                               pre_forecast_inputs_model: DataFrame, 
                                               pre_forecast_inputs_meta_data: ForecastMetaDataFrame,
                                               pf_col_id: str) -> tuple[(DataFrame, ForecastMetaDataFrame, str)]:
        
        # prefix to add to every column which has the calculated data
        pmpf_col_prefix = "num_patients_in_month"

        # the number of rows to calculate for patients in various stages of treatment by month
        num_elements = self.treatment_duration-1

        # create data structure to hold the prior_month patient flow 
        # generate the meta-dataframe
        prior_month_patient_flow_meta_data = ForecastMetaDataFrame(id = id,
                                                                   input_type = ForecastModelInputTypes.TIME_BASED,
                                                                   timescale = ForecastModelTimescale.MONTH,
                                                                   start_year = target_date.year,
                                                                   start_month = target_date.month,
                                                                   num_periods = 1)
        
        # Create an empty dataframe which we will build up using the same ids as we do with the meta-data
        prior_month_patient_flow_data = DataFrame()


        # PRIOR_MONTH_DATE
        (prior_month_patient_flow_data, prior_month_patient_flow_meta_data) = ForecastComponent._add_col_data_meta(prior_month_patient_flow_data,
                                                                                                                   prior_month_patient_flow_meta_data,
                                                                                                                   id = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME,
                                                                                                                   display_name = "Date",
                                                                                                                   data_values = [target_date],
                                                                                                                   step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                                   action = ForecastDataSeriesMetaDataAction.DATES,
                                                                                                                   data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                                                                                                   display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                                                                                                   validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                                   update_last_id = True)

        # NUM PATIENTS BY TREATMENT MONTH
        for i in range(num_elements):
            data_value = treatment_details_model[pc_col_id][i] * pre_forecast_inputs_model[pf_col_id][num_elements-1-i]
            (prior_month_patient_flow_data, prior_month_patient_flow_meta_data) = ForecastComponent._add_col_data_meta(prior_month_patient_flow_data,
                                                                                                                       prior_month_patient_flow_meta_data,
                                                                                                                       id = f"{pmpf_col_prefix}_{i+1}",
                                                                                                                       display_name = f"# of patients in '{self.get_display_name()}' Month {i+1}",
                                                                                                                       data_values = [float(data_value)],
                                                                                                                       step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT,
                                                                                                                       action = ForecastDataSeriesMetaDataAction.PROD,
                                                                                                                       data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                                                                       display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                                                                       pred = [f"{treatment_details_meta_data.get_id()}.{pc_col_id}:{i}", f"{pre_forecast_inputs_meta_data.get_id()}.{pf_col_id}:{num_elements-1-i}"],
                                                                                                                       validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                                                       update_last_id = True)

        return(prior_month_patient_flow_data, prior_month_patient_flow_meta_data, pmpf_col_prefix)












    # INPUT HELPERS
    # =============

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
        table_schema_cols = build_config["treatment_details"]["table_schema"]["columns"]
        build_config["treatment_details"]["table_schema"]["columns"] = self._updated_table_schema_cols(table_schema_cols, num_products, self.NUM_STATIC_COLS, field_value, field_name)
        
        # save updated table schema in build_config[self.TABLE_SCHEMA_INPUT_NAME]
        build_config[self.TABLE_SCHEMA_INPUT_NAME]["value"] = dict(build_config["treatment_details"])

        return(build_config)


    # callback from the _updated_table_schema in ForecastComponent that delegates
    # the specific details of the new column attributes to this class
    # ZIV
    def _gen_new_table_col(self, col_num: int) -> dict:
        
        product_display_name = self.get_prod_display_name(col_num)

        if product_display_name is None:
            product_display_name = f"Product {col_num+1}"

        return({
                "name": f"{ForecastTreatmentTBView.COL_PREFIX}_{col_num+1}",
                "display_name": f"Product {col_num+1}",
                "type": "float",
                "description": f"Number of prescriptions of '{product_display_name}', for each month of treatment",
                "disable_edit": False,
                "sortable": False,
                "filterable": False,
                "edit_mode": EditMode.INLINE,
        })




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
            new_df = ForecastFormModelUtilities.fill_dataframe(new_dim_rows = new_num_rows,
                                                                new_dim_cols = new_num_cols,
                                                                prev_data  = old_values, 
                                                                default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                                #individual_default_col_values = {ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME: 1}, 
                                                                col_name_prefix = f"{self.COL_PREFIX}_", 
                                                                num_static_cols = self.NUM_STATIC_COLS, 
                                                                month = list(range(1, new_num_rows+1)))
        else:
            new_df = ForecastFormModelUtilities.fill_dataframe(new_dim_rows = new_num_rows,
                                                                new_dim_cols = new_num_cols,
                                                                set_col_names = ["month", ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME],  
                                                                default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                                #individual_default_col_values = {ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME: 1}, 
                                                                col_name_prefix = f"{self.COL_PREFIX}_", 
                                                                num_static_cols = self.NUM_STATIC_COLS, 
                                                                month = list(range(1, new_num_rows+1)))
        
        return(new_df.to_data_list())
    



    # _get_input_table_col_display_name
    # Convenience function to get the display name of a column in an input_table
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_num = column number as defined in the TableSchema
    #
    # OUTPUTS
    #   column display name

    def _get_input_table_col_display_name(self, table_name: str, col: int | str) -> str:
        if not self._hidden_exists():
            return super()._get_input_table_col_display_name(table_name, col)
        
        else:
            input_col = self._get_input_table_col(table_name = table_name, col = col)

            if("display_name" in input_col.keys()):
                return(input_col["display_name"])
            else:
                return self._get_input_table_col_name(table_name, col)                    


    # _get_input_table_col_name
    # Convenience function to get the name (id) of a column in an input_table
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col = can be the index of a column or the name of a column in the InputTable's TableSchema
    #
    # OUTPUTS
    #   column name (id)

    def _get_input_table_col_name(self, table_name: str, col: int | str) -> str:
        if not self._hidden_exists():
            return super()._get_input_table_col_name(table_name, col)
        
        else:
            input_col = self._get_input_table_col(table_name = table_name, col = col)

            if("name" in input_col.keys()):
                return(input_col["name"])
            else:
                return str(col)


    # _get_input_table_col
    # Convenience function to get a Column object from an TableInput's TableSchema by column index or name
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_num = column number as defined in the TableSchema
    #
    # OUTPUTS
    #   column name (id)

    def _get_input_table_col(self, table_name: str, col: int | str) -> dict | Column:
        if not self._hidden_exists():
            return super()._get_input_table_col(table_name, col)
        
        else:
            table_schema = getattr(self, self.TABLE_SCHEMA_INPUT_NAME, None)["table_schema"]
            cols = table_schema["columns"]
            col_num = col if isinstance(col, int) else self._get_input_table_col_num_from_name(table_name = table_name, col_name = col)

            return(cols[col_num])


    # _get_input_table_col_num_from_name
    # Convenience function to get the index of a column in an InputTable based on it's name.
    # NOTE:  Assumes column names are unique and only returns the first match
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_name = column name in the table
    #
    # OUTPUTS
    #   index of column name in table

    def _get_input_table_col_num_from_name(self, table_name: str, col_name: str) -> int:
        # remove prefix from col_name
        col_name = col_name.removeprefix(f"{self._id}_")

        if not self._hidden_exists():
            return super()._get_input_table_col_num_from_name(table_name, col_name)
        
        else:
            table_schema = getattr(self, self.TABLE_SCHEMA_INPUT_NAME, None)["table_schema"]
            cols = table_schema["columns"]
            col_names = []

            for i in range(len(cols)):
                #if col_name == cols[i]["name"]:
                if col_name == cols[i]["name"]:
                    return(i)
                else:
                    #col_names.append(cols[i]["name"])
                    col_names.append(cols[i]["name"])

            raise ValueError(f"\n*  _get_input_table_col_num_from_name:  column name '{col_name}' not found in '{table_name}', list of columns {col_names}.")
        





    # _hidden_exists
    # Convenience function to check if we have the HIDDEN InputType with values in it
     #
    # INPUTS
    #   NA
    #
    # OUTPUTS
    #   True if it exists
    #   False if it doesn't

    def _hidden_exists(self) -> bool:
        if not hasattr(self, self.TABLE_SCHEMA_INPUT_NAME):
            return False
        
        if not getattr(self, self.TABLE_SCHEMA_INPUT_NAME, None):
            return False
        
        return True



    # generate_table_prod_name_values
    # Based on the latest schema, generates the values for the table
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config

    def generate_table_prod_name_values(self, field_value: str, field_name: str) -> List[dict]:
        # determine how many rows we need
        if(field_name == "num_products"):
            num_rows = int(field_value)
        else:
            num_rows = self.num_products
            
        # Check if we have existing data
        old_values = self.product_names
        if(old_values is not None and isinstance(old_values, list) and len(old_values) > 0):
            new_df = ForecastFormModelUtilities.fill_dataframe(new_dim_rows = num_rows,
                                                               new_dim_cols = 2,
                                                               prev_data  = old_values, 
                                                               default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                               col_name_prefix = None, 
                                                               num_static_cols = 2, 
                                                               prod_num = list(range(1, num_rows+1)),
                                                               prod_name = [f"Product {i}" for i in range(1, num_rows+1)])
        else:
            new_df = ForecastFormModelUtilities.fill_dataframe(new_dim_rows = num_rows,
                                                               new_dim_cols = 2,
                                                               set_col_names = ["prod_num", "prod_name"],  
                                                               default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                               col_name_prefix = None, 
                                                               num_static_cols = 2, 
                                                               prod_num = list(range(1, num_rows+1)),
                                                               prod_name = [f"Product {i}" for i in range(1, num_rows+1)])
        
        return(new_df.to_data_list())
    



    # update_prod_table_display_names
    # Update the display names for the products in the Paroducts Table whenever updates are made to the Product Names Table
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config

    # src_table = "segment_names"
    # src_col = "seg_name"
    # dst_table = "segment_table"
    # seg_names = display_names

    def update_prod_table_display_names(self, build_config, field_value, field_name):

        if (field_name != "product_names") or (not hasattr(self, "product_names")) or (self.product_names is None) or (len(self.product_names) < 1):
            return(build_config)

        # get list of segment names
        prod_names = [self.product_names[i]["prod_name"] for i in range(len(self.product_names))]

        for i in range(len(prod_names)):
            build_config[ForecastTreatmentTBView.TABLE_NAME]["table_schema"]["columns"][i+self.NUM_STATIC_COLS]["display_name"] = prod_names[i]
            build_config[ForecastTreatmentTBView.TABLE_NAME]["table_schema"]["columns"][i+self.NUM_STATIC_COLS]["description"] = f"# of Rx written for '{prod_names[i]}' each month of the '{self.display_name2}' treatment."

        return(build_config)



    # ================
    # HELPER FUNCTIONS
    # ================

    # get_prod_display_name
    # Does a "safe" get of a display name for a product from the product_names table, handling all the issues that may happen silently
    # 
    # INPUTS:
    #   idx - index of the display name (zero based index, same as the rest)
    #
    # OUTPUTS:
    #   str or None

    def get_prod_display_name(self, idx: int) -> str | None:
        if (not hasattr(self, "product_names")) or (self.product_names is None) or (len(self.product_names) < 1):
            #print("\n\nWARNING:  (not hasattr(self, 'product_names')) or (self.product_names is None) or (len(self.product_names)\n\n")
            return None
        
        if(len(self.product_names) <= idx):
            #print("\n\nWARNING:  len(self.product_names) <= idx\n\n")
            return None
        
        prod_names = [self.product_names[i]["prod_name"] for i in range(len(self.product_names))]

        return(prod_names[idx])
