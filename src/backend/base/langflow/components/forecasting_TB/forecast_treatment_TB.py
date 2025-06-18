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
from typing import Any, List, Tuple



# CLASSES
# =======

# ForecastTreatmentTB
# This class represents applying a treatment regiment of products to an incoming patient flow
class ForecastTreatmentTB(Component):

    # CONSTANTS
    # =========
    MAX_TREATMENT_DURATION = 12*100    # max treatment duration is 100 years (in months)
    MAX_PRODUCTS = 100
    COL_PREFIX = "product_"
    NUM_STATIC_OUTPUTS = 2 # one static output (# patients leaving/month), rest is product
    NUM_STATIC_COLS = 2 # two static columns in table (month of pression, % of people progressing), rest is product

    MAX_TREATMENT_DURATION = 240 # max treatment duration supported is 20 years

    # COMPONENT META-DATA
    # -------------------
    display_name: str = "Treatment TB"
    description: str = "Apply a treatment regiment of products to an incoming patient flow"
    icon = "Syringe"
    name: str = "TreatmentTB"


    # COMPONENT INPUTS
    # ----------------
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
            dynamic = True,
            real_time_refresh = True,
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
        
        # therapy_details
        TableInput(
            name="therapy_details",
            display_name="Therapy Details",
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

    # COMPONENT OUTPUTS
    # -----------------
    outputs = [
        Output(display_name="# patient ON-THERAPY", name="patient_on_therapy", method="calc_patients_on_therapy"),
        Output(display_name="# patient LEAVING", name="patients_leaving_therapy", method="calc_patients_leaving_therapy"),
    ]



    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_segments_table_def", ["num_products", "treatment_duration"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("therapy_details", "generate_table_values", ["num_products", "treatment_duration"])),
    ]

    


    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing INPUTS fields whenever an update happens from a dynamic field
    # -------------------
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
                                                               update_segments_table_def=self.generate_table_schema,
                                                               generate_table_values=self.generate_table_values)


        # return updated config         
        return(build_config)
    


    # UPDATE_OUTPUTS
    # Updates real_time_refreshing OUTPUT fields whenever an update happens from a dynamic field
    # -------------------
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
                        method=f"update_forecast_model_segment_{i+1}"
                    ))

        return frontend_node
        


    # __getattribute__
    # Because Langflow does not allow calling methods in outputs with arguments, we need a way to generate a unique methe call for each 
    # of the variable outputs, but then convert those individual calls into a common method with a different argument for the segment number.
    # To do that, we created above individual methods "update_forecast_model_segment_1", "update_forecast_model_segment_2", "update_forecast_model_segment_3", etc.
    # This function overrides the __getattribute__ method call which looks up the name of a function, takes the function name and parses out the segment id ("_3" -> int(3))
    # And then redirects that method call ("update_forecast_model_segment_1"), to the generic method ("update_forecast_model_segment"), but with a wrapper function around
    # it
    # 
    # INPUTS:
    #   func - the function call to the generic 'update_forecast_model_segment'
    #   seg_num - integer segment number
    #
    # OUTPUTS:
    #   A wrapper around 'update_forecast_model_segment' which will put in the the right segment number for the call

    def __getattribute__(self, attr):
        if attr.startswith("update_forecast_model_segment_"):
            attribute = super().__getattribute__("update_forecast_model_segment")

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
    #   func - the function call to the generic 'update_forecast_model_segment'
    #   seg_num - integer segment number
    #
    # OUTPUTS:
    #   A wrapper around 'update_forecast_model_segment' which will put in the the right segment number for the call

    def wrapper(self, func, seg_num):
        def new_funct(seg_num = seg_num, *args, **kwargs) -> DataFrame:
            out = func(seg_num = seg_num, *args, **kwargs)
            return out
        
        return new_funct
    

    # COMMON PRE ALL OUTPUT CALLS
    # ---------------------------
    def pre_output(self) -> Tuple[DataFrame, DataFrame]:
        # run input validation
        self.validate_inputs()

        # sum up all the inputs to create a single total line and return it to the output function
        updated_model = self.check_and_combine_forecasts()
        updated_model = ForecastDataModel.astype_first_all_cols(updated_model)

        # make sure that data-types for therapy_details is set correctly
        # and return that to the output function
        therapy_details = ForecastDataModel.astype_first_all_cols(self.therapy_details, first_col_type="int")

        return(therapy_details, updated_model)


    


    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        # therapy_details
        if(self.therapy_details is None or not isinstance(self.therapy_details, list) or len(self.therapy_details) < 1):
            msg += f"\n* Missing values for '{self.get_input_display_name("therapy_details")}'."
                    
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------

    # OUTPUT FUNCTIONS

    # calc_patients_on_therapy
    # return the total number of patients on therapy at a MONTHLY level (best granularity), for use in downstream nodes
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per month and treatment stage
    def calc_patients_on_therapy(self) -> DataFrame:
        (pat_on_therapy_month, pat_leaving_month, therapy_details) = self.calc_patients_therapy_common()
        return(pat_on_therapy_month)



    # calc_patients_leaving_therapy
    # return the total number of patients that leave therapy at every timescale sample
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def calc_patients_leaving_therapy(self) -> DataFrame:
        (pat_on_therapy_month, pat_leaving_month, therapy_details) = self.calc_patients_therapy_common()
        return(pat_leaving_month)
    

    # generate_forecast_model_segment
    # Add the segment % and the new total patients to the model
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_segment(self, seg_num=1) -> DataFrame:
        (pat_on_therapy_month, pat_leaving_month, therapy_details) = self.calc_forecast_model_segment_common()
        product_use_in_treatment_by_month = ForecastDataModel.calc_treatment_rx_forecast_for_product(product_name = f"{self.COL_PREFIX}{seg_num}",
                                                                                                     col_prefix = f"{self._id}_",
                                                                                                     forecast_in = pat_on_therapy_month,
                                                                                                     treatment_details = therapy_details,
                                                                                                     forecast_timescale = ForecastModelTimescale.MONTH, # we hardcode the timescale for monthly, because we will receive monthly for prev step
                                                                                                     convert_timescale = self.timescale) # but we override with a convert to the actual timescale we have later, so that the results we provide are in the right timescale
        return(product_use_in_treatment_by_month)







    # OUTPUT HELPERS

    # calc_patients_therapy_common
    # Common code for outputs:  calc_patients_on_therapy, calc_patients_leaving_therapy
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    def calc_patients_therapy_common(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        # run all validation, merging, etc. which is common to any update call
        # sum up all the inputs to create a single total line and add it to the output model
        (therapy_details, updated_model) = self.pre_output()

        # we return two ancillary data sets that can be plugged into other treatment related steps, these sets are for the forecast time period
        # aggregate to a timescale level:
        #   return total number of patients ON THERAPY per month for the forecast (we WILL return this)
        #   return total number of patients leaving therapy PER MONTH fore the forecast time period from the therapy (we WON'T return this)
        (pat_on_therapy_month, pat_leaving_month) = ForecastDataModel.calc_treatment_pat_forecast(col_prefix = f"{self._id}_",
                                                                                                  forecast_in = updated_model,
                                                                                                  treatment_details = therapy_details,
                                                                                                  forecast_timescale = self.timescale,
                                                                                                  keep_granular = False)
        return (pat_on_therapy_month, pat_leaving_month, therapy_details)
    


    # calc_forecast_model_segment_common
    # Common code for outputs:  update_forecast_model_segment
    # 
    # INPUTS:
    #   N/A
    # OUTPUTS:
    #   DataFrame with the number of patients per timescale and treatment stage
    def calc_forecast_model_segment_common(self) -> Tuple[DataFrame, DataFrame, DataFrame]:
        # run all validation, merging, etc. which is common to any update call
        # sum up all the inputs to create a single total line and add it to the output model
        (therapy_details, updated_model) = self.pre_output()

        # we return two ancillary data sets that can be plugged into other treatment related steps, these sets are for the forecast time period
        # KEEP AT A MONTHLY LEVEL (for later steps):
        #   return total number of patients ON THERAPY per month for the forecast (we WILL return this)
        #   return total number of patients leaving therapy PER MONTH fore the forecast time period from the therapy (we WON'T return this)
        (pat_on_therapy_month, pat_leaving_month) = ForecastDataModel.calc_treatment_pat_forecast(col_prefix = f"{self._id}_",
                                                                                                  forecast_in = updated_model,
                                                                                                  treatment_details = therapy_details,
                                                                                                  forecast_timescale = self.timescale,
                                                                                                  keep_granular = True)
        return (pat_on_therapy_month, pat_leaving_month, therapy_details)



    # check_and_combine_forecasts
    # Consolidate all the value checking and dataframe concat into one function
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def check_and_combine_forecasts(self) -> DataFrame:
        updated_model = ForecastDataModel.concat_and_sum(datas=self.forecasts_in, new_col_name = str("Total_"+self._id), skip_total_if_one=True)
        return updated_model




    # generate_table_schema
    # Generates the schema for the therapy details table given the total number of products, now available
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
        if(len(build_config["therapy_details"]["table_schema"]["columns"])-self.NUM_STATIC_COLS == num_products):
            return(build_config)

        # otherwise, rebuild the table schema by taking the static columns, and then adding the correct number of num_product columns
        table_schema = build_config["therapy_details"]["table_schema"]["columns"][:self.NUM_STATIC_COLS]

        for i in range(num_products):
            table_schema.append({
                "name": f"{ForecastTreatmentTB.COL_PREFIX}{i+1}",
                "display_name": f"Product {i+1} Rx",
                "type": "float",
                "description": f"Number of prescriptions of product {i+1}, for the N's time period of a therapy",
                "disable_edit": False,
                "sortable": False,
                "filterable": False,
                "edit_mode": EditMode.INLINE,
            })

        build_config["therapy_details"]["table_schema"]["columns"] = table_schema
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
        old_values = self.therapy_details
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
    