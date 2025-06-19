#####################################################################
# forecast_pricing_TB.py
#
# Implements the pricing component of the forecasting in a TIME BASED model.
# The pricing component applies a TIME-BASED price on a Product / SKU to
# return a revenue stream
# 
# INPUTS:  DataFrame
# OUTPUTS:  DataFrame
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
from typing import Any, List


# CLASSES
# =======

# ForecastPricingTB
# This class represents converting a series of Product Rx / SKU orders in to a revenue stream by applying price
class ForecastPricingTB(Component):

    # CONSTANTS
    # =========
    MAX_SEGMENTS = 1
    COL_PREFIX = "pricing_"

    # COMPONENT META-DATA
    # -------------------
    display_name: str = "Pricing TB"
    description: str = "Apply a timescale specific price to a series of Product/SKU Rx/orders to return a revenue stream."
    icon = "DollarSign"
    name: str = "PricingTB"


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
            #dynamic = True,
            #real_time_refresh = True,
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

        # num_segemtns
        IntInput(
            name="num_segments",
            display_name = "Number of segments",
            info="Select the total number of segments for this node.  Segments do not need to add up to 1, however, any excess will go into the remainder_patient_model",
            value=1,
            real_time_refresh=True,
            show = False,
            required = True,
            range_spec = RangeSpec(min=0, max=MAX_SEGMENTS)
        ),
        
        # segmentation_table
        TableInput(
            name="segment_table",
            display_name="Product price",
            info="The price of the product at each time_period",
            required=True,
            show=True,
            dynamic=True,
            real_time_refresh=True,
            table_schema=[
                {
                    "name": "dates",
                    "display_name": "Date",
                    "type": "date",
                    "description": "Date of product price",
                    "edit_mode": EditMode.INLINE,
                    "disable_edit": True,
                },
                {
                    "name": f"{COL_PREFIX}1",
                    "display_name": "Population retained",
                    "type": "float",
                    "description": "The price of the product.",
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
        Output(display_name="Product revenue", name="product_revenue", method="update_forecast_model_retained"),
    ]



    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("segment_table", "generate_table_values", ["num_segments", "segment_table"])),
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
                                                               generate_table_values=self.generate_table_values)


        # return updated config         
        return(build_config)
    




    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        # segment_table
        if(self.segment_table is None or not isinstance(self.segment_table, list) or len(self.segment_table) < 1):
            msg += f"\n* Missing values for '{self.get_input_display_name("segment_table")}'."
                    

        # # check to make sure all percentage in the segment add up to >= 100% or throw an error
        # self.check_segment_pcts_add_up()
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
        
    # update_forecast_model_retained
    # Apply the retained percentage to the incoming forecast and return the retained patient flow
    # 
    # INPUTS:
    #   seg_num - the segment number (leverages the code of forecast_segment for speed, and just hardcodes to '1' since we will never have more than 1 "segment")
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_retained(self, seg_num=1) -> DataFrame:
        # run input validation
        self.validate_inputs()

        # get segment name
        seg_name = f"{self.COL_PREFIX}{self._id}"

        # sum up all the inputs to create a single total line and add it to the output model
        updated_model = self.check_and_combine_forecasts()
        curr_total_values = updated_model[updated_model.columns[-1]]

        # get the segment table data
        segment_table = ForecastDataModel.astype_first_all_cols(self.segment_table)
        curr_seg_name = segment_table.columns[seg_num]
        curr_seg_values = segment_table[curr_seg_name]
    
        # add the percentages for this segment as a new column in the output model
        updated_model = ForecastDataModel.add_col_to_model(updated_model, curr_seg_values.to_list(), new_col_name=f"Price_{curr_seg_name}_{self._id}")

        # add the totals for this segment (assumes the RESERVED token for editing will always "win" in multiplication, so needs to be NAN or zero)
        curr_seg_total_values = curr_total_values.multiply(curr_seg_values)
        updated_model = ForecastDataModel.add_col_to_model(updated_model,  curr_seg_total_values.to_list(), new_col_name=f"Total_{curr_seg_name}_{self._id}")
        return(updated_model)
    

    # check_and_combine_forecasts
    # Consolidate all the value checking and dataframe concat into one function
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def check_and_combine_forecasts(self) -> DataFrame:
        updated_model = ForecastDataModel.concat_and_sum(datas=self.forecasts_in, new_col_name = str("Total_"+self._id), skip_total_if_one=True)
        return updated_model


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
        
        num_segments = self.num_segments
        num_cols = num_segments+1 # have to add an extra column to cover the dates column

        # get the current values in the patient_counts table
        hasOldValues = False

        old_values = self.segment_table
        
        if(old_values is not None and isinstance(old_values, list) and len(old_values) > 0 and len(old_values[0].keys()) > 1):
            hasOldValues = True

        # generate the dates needed (we'll need this regardless of whether we have old values or not)
        dates = ForecastDataModel.gen_forecast_dates(start_year = int(self.start_year),
                                                     start_month = int(self.start_month),
                                                     num_years = int(self.num_years),
                                                     timescale = ForecastModelTimescale(self.timescale))
        num_rows = len(dates)

        # if the number of segments is zero, no need to use the old data, just generate the dates
        if(num_segments == 0):
            segment_table = [{ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: dates[i]} for i in range(num_rows)]
            return segment_table
        
        # if there are no old values, generate a brand new list of dicts for the table
        if(not hasOldValues):
            segment_table = [{ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: dates[i]} for i in range(num_rows)]

            # add the individual segment values
            for curr_row in segment_table:
                for i in range(num_segments):
                    curr_row[f"{self.COL_PREFIX}{i+1}"] = ForecastDataModel.EDITABLE_VALUES_TOKEN
            return(segment_table)
                
        # otherwise, resize the exist values into the new size (note: always add the dates in)
        else:
            old_values_df = ForecastDataModel.astype_first_all_cols(old_values)    # simple helper to make sure that the datatimes of the resulting DataFrame have the first col as type datetime, and all other cols as type float
            new_df = ForecastFormModelUtilities.refill_drataframe(new_dim_rows=num_rows, new_dim_cols=num_cols, prev_data=old_values_df, col_name_prefix=self.COL_PREFIX, dates=dates)
            return new_df.to_data_list()
