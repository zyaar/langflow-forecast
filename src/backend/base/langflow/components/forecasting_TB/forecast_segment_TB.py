#####################################################################
# forecast_segment_TB.py
#
# Implements the segment component of the forecasting in a TIME BASED model.
# The segment component applies one timescale based percentage to the incoming flow
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
from typing import Any, List


# CLASSES
# =======

# ForecastSegmentTB
# This class represents dividing a stream of patients into a fixed number of segments, based on percentages of the total assigned at
# each time period of the forecast
class ForecastSegmentTB(Component):

    # CONSTANTS
    # =========
    MAX_SEGMENTS = 100
    SEGMENT_COL_PREFIX = "segment_"

    # COMPONENT META-DATA
    # -------------------
    display_name: str = "Segment TB"
    description: str = "Apply a timescale specific % split critera each branch (segement, remainder) of which can be linked to a different flow."
    icon = "Puzzle"
    name: str = "SegmentTB"


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

        # num_segemtns
        IntInput(
            name="num_segments",
            display_name = "Number of segments",
            info="Select the total number of segments for this node.  Segments do not need to add up to 1, however, any excess will go into the remainder_patient_model",
            value=0,
            real_time_refresh=True,
            show = True,
            required = True,
            range_spec = RangeSpec(min=0, max=MAX_SEGMENTS)
        ),
        
        # segmentation_table
        TableInput(
            name="segment_table",
            display_name="Segments",
            info="For each segment, provide a name and (optional) percentages of total patient flow for each time period",
            required=True,
            show=True,
            dynamic=True,
            real_time_refresh=True,
            table_schema=[
                {
                    "name": "dates",
                    "display_name": "Date",
                    "type": "date",
                    "description": "Date of patient count",
                    "edit_mode": EditMode.INLINE,
                    "disable_edit": True,
                },
                {
                    "name": f"{SEGMENT_COL_PREFIX}1",
                    "display_name": "Segment 1",
                    "type": "float",
                    "description": "Segment 1",
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
        Output(display_name="Remainder Patient Flow", name="remainder_patient_model", method="update_forecast_model_remainder"),
    ]



    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    #form_trigger_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_segments_table_def", ["num_segments", "segment_table"])),
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
                                                               update_segments_table_def=self.generate_table_schema,
                                                               generate_table_values=self.generate_table_values)


        # return updated config         
        return(build_config)
    


    # UPDATE_OUTPUTS
    # Updates real_time_refreshing OUTPUT fields whenever an update happens from a dynamic field
    # -------------------
    def update_outputs(self, frontend_node: dict, field_name: str, field_value: Any) -> dict:
        curr_num_output_nodes = len(frontend_node["outputs"])-1

        # check if this is an update to the number of segments, in which case we definitely need
        # to refresh the outputs... alternatively, it could be an update to something else, but
        # there is an edge case when the component first starts the number of outputs may not match
        # the number of segments, in which case, we need to do it anyway
        if(field_name == "num_segments"):
            target_segments = field_value
        else:
            target_segments = self.num_segments
 
        # check if the length of outputs is different than the value of num_segments, if not, then return
        if(target_segments != curr_num_output_nodes):
            remainder_output = frontend_node["outputs"].pop()

            # if less value, then remove the last few nodes
            if(field_value < curr_num_output_nodes):
                num_nodes_remove = curr_num_output_nodes - field_value

                for i in range(num_nodes_remove):
                    frontend_node["outputs"].pop()
        
            # if it's greater than, then add a bunch of blank nodes
            else:
                num_nodes_add = field_value - curr_num_output_nodes

                for i in range(num_nodes_add):
                    curr_num = curr_num_output_nodes + (i+1)
                    frontend_node["outputs"].append(Output(name=f"{ForecastSegmentTB.SEGMENT_COL_PREFIX}{curr_num}", display_name=f"Segment {curr_num}", method=f"update_forecast_model_segment_{curr_num}"))

            frontend_node["outputs"].append(remainder_output)

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
    


    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        # segment_table
        if(self.segment_table is None or not isinstance(self.segment_table, list) or len(self.segment_table) < 1):
            msg += f"\n* Missing values for '{self.get_input_display_name("segment_table")}'."
                    

        # check to make sure all percentage in the segment add up to >= 100% or throw an error
        self.check_segment_pcts_add_up()
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
        
    # generate_forecast_model_segment
    # Add the segment % and the new total patients to the model
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_segment(self, seg_num=1) -> DataFrame:
        # run input validation
        self.validate_inputs()

        # get segment name
        seg_name = f"{ForecastSegmentTB.SEGMENT_COL_PREFIX}{seg_num}"

        # sum up all the inputs to create a single total line and add it to the output model
        updated_model = self.check_and_combine_forecasts()
        curr_total_values = updated_model[updated_model.columns[-1]]

        # get the segment table data
        segment_table = ForecastDataModel.astype_first_all_cols(self.segment_table)
        curr_seg_name = segment_table.columns[seg_num]
        curr_seg_values = segment_table[curr_seg_name]
    
        # add the percentages for this segment as a new column in the output model
        updated_model = ForecastDataModel.add_col_to_model(updated_model, curr_seg_values.to_list(), new_col_name=f"Percent_{curr_seg_name}_{self._id}")

        # add the totals for this segment (assumes the RESERVED token for editing will always "win" in multiplication, so needs to be NAN or zero)
        curr_seg_total_values = curr_total_values.multiply(curr_seg_values)
        updated_model = ForecastDataModel.add_col_to_model(updated_model,  curr_seg_total_values.to_list(), new_col_name=f"Total_{curr_seg_name}_{self._id}")
        return(updated_model)
    

    # generate_forecast_model_remainder
    # Add the remainder (1- segment %) and the new total to the model
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_remainder(self) -> DataFrame:
        self.validate_inputs()

        # combine all the inputs to create a single total line
        updated_model = self.check_and_combine_forecasts()
        updated_model = ForecastDataModel.add_col_to_model(updated_model, new_col_values=[0] * len(updated_model.index), new_col_name=f"Percent_Remainder_{self._id}")
        updated_model = ForecastDataModel.add_col_to_model(updated_model, new_col_values=[0] * len(updated_model.index), new_col_name=f"Total_Remainder_{self._id}")
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




    # generate_table_schema
    # Generates the schema for the segment table given the total number of segments, now available
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config
    def generate_table_schema(self, build_config, field_value, field_name):
        num_segments = int(field_value)

        # generate the table schema
        # first generate the Dates column def (it will always have this)
        table_schema = [
            {
                "name": str(ForecastDataModel.RESERVED_COLUMN_INDEX_NAME),
                "display_name": "Date",
                "type": "date",
                "description": "Date of for the forecast",
                "edit_mode": EditMode.INLINE,
                "disable_edit": True,
            },
        ]

        # then generate a variable number of segment column defs, depending on number of segments
        for i in range(num_segments):
            table_schema.append({
                "name": f"{ForecastSegmentTB.SEGMENT_COL_PREFIX}{i+1}",
                "display_name": f"Segment {i+1}",
                "type": "float",
                "description": f"Percent of total population going to Segment {i+1}, for each time period",
                "disable_edit": False,
                "sortable": False,
                "filterable": False,
                "edit_mode": EditMode.INLINE,
            })

        build_config["segment_table"]["table_schema"] = table_schema
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
                    curr_row[f"{ForecastSegmentTB.SEGMENT_COL_PREFIX}{i+1}"] = ForecastDataModel.EDITABLE_VALUES_TOKEN
            return(segment_table)
                
        # otherwise, resize the exist values into the new size (note: always add the dates in)
        else:
            old_values_df = ForecastDataModel.astype_first_all_cols(old_values)    # simple helper to make sure that the datatimes of the resulting DataFrame have the first col as type datetime, and all other cols as type float
            new_df = ForecastFormModelUtilities.refill_drataframe(new_dim_rows=num_rows, new_dim_cols=num_cols, prev_data=old_values_df, col_name_prefix=ForecastSegmentTB.SEGMENT_COL_PREFIX, dates=dates)
            return new_df.to_data_list()
    


    # check_segment_pcts_add_up
    # Goes over each row of the segment percentages to ensure that the total of all segment percentages add up to less than 100%
    # Optionally:  Will put the remainder % in the remainders 
    # 
    # INPUTS:
    #
    # OUTPUTS:
    #   Throw error if problem, otherwise silent

    def check_segment_pcts_add_up(self):
        segment_df = ForecastDataModel.astype_first_all_cols(self.segment_table)    # simple helper to make sure that the datatimes of the resulting DataFrame have the first col as type datetime, and all other cols as type float
        segment_cols = segment_df.columns[1:]   # get just the segment columns (which are all columns except the date column)

        errMsg = ""

        # Go through each row in the % of total in the segments table and make sure that all the hardcoded values
        for i in range(len(segment_df)):
            seg_values = segment_df[segment_cols].iloc[i]
            seg_values = seg_values[seg_values != ForecastDataModel.EDITABLE_VALUES_TOKEN]
            seg_total = seg_values.sum()

            if(seg_total > 1):
                errMsg += f"* {segment_df[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME][i]}: Total value of all segments for this time period is {seg_total} (>100%).  Please correct.\n"

        if(errMsg != ""):
            errMsg = f"Error, invalid values for segments percentages found in '{self.get_input_display_name("segment_table")}':\n" + errMsg
            raise ValueError(errMsg)