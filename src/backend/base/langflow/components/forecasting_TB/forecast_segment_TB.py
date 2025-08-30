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
from langflow.io import StrInput, DataInput, IntInput, TableInput, NestedDictInput
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
                                                                        ForecastDataSeriesMetaDataComparisonType)



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List
from langflow.base.forecasting_common.controllers.forecast_segment_TB_controller import ForecastSegmentTBController



# CLASSES
# =======

# ForecastSegmentTB
# This class represents dividing a stream of patients into a fixed number of segments, based on percentages of the total assigned at
# each time period of the forecast
class ForecastSegmentTB(ForecastSumInputTB, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT
    display_name: str = "Segment TB"
    description: str = "Apply a timescale specific % split critera each branch (segement, remainder) of which can be linked to a different flow."
    icon = "Puzzle"
    name: str = "SegmentTB"

    # COL_SET_VAR
    COL_PREFIX = "segment_"
    MAX_SEGMENTS = 100

    # INPUTS / OUTPUTS
    NUM_STATIC_COLS = 1 # one static columns in 'segment_table' ('Date' is static, rest is segment specific)
    NUM_STATIC_OUTPUTS = 1 # only 'Remainder Patient Flow'
    STATIC_OUTPUTS_AT_START = False


    # INIT
    # ====
    def __init__(self, **kwargs) -> None:
        # set-up a controller if needed
        if not hasattr(self, "controller"):
            self.controller = ForecastSegmentTBController()

        # call parent init
        super().__init__(**kwargs)

    
    # GENERATE INPUTS / OUTPUTS
    # =========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            *super()._gen_inputs(),
            # num_segemtns
            IntInput(
                name="num_segments",
                display_name = "Number of segments",
                info="Select the total number of segments for this node.  Segments do not need to add up to 1, however, any excess will go into the remainder_patient_model",
                value=0,
                real_time_refresh=True,
                show = True,
                required = True,
                range_spec = RangeSpec(min=0, max=self.MAX_SEGMENTS)
            ),
            
            # Table to enter segment names
            TableInput(
                name="segment_names",
                display_name="Segment names",
                info="Display names for each segment",
                required=True,
                show=True,
                dynamic=True,
                real_time_refresh=True,
                table_schema=[
                    {
                        "name": "seg_num",
                        "display_name": "ID",
                        "type": "int",
                        "description": "The id of the segment.",
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": True,
                    },
                    {
                        "name": "seg_name",
                        "display_name": "Name",
                        "type": "str",
                        "description": "Name of the segment",
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": False,
                    },
                ],
                value=[],
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
                        "name": f"{self.COL_PREFIX}1",
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

        return(inputs_list)


    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),
            Output(display_name="Remainder Patient Flow", name="remainder_patient_model", method="update_forecast_model_remainder"),
        ]

        return(outputs_list)




    # INPUT/OUTPUT VALIDATIONS
    # ========================
    def validate_inputs(self):
        super().validate_inputs()

        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        if(self.num_segments < 1):
            msg += f"\n* '{self.get_input_display_name("segment_num")}' have at least 1 segment."
        

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
    

    # FORM UPDATE RULES
    # =================
    form_update_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_segments_table_def", ["num_segments", "segment_table"])),
        (ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("update_seg_table_display_names", ["segment_names"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("segment_names", "generate_table_seg_name_values", ["num_segments"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("segment_table", "generate_table_values", ["num_segments", "segment_table"])),
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
                                                               update_segments_table_def=self.generate_table_schema,
                                                               generate_table_values=self.generate_table_values,
                                                               update_seg_table_display_names = self.update_seg_table_display_names,
                                                               generate_table_seg_name_values = self.generate_table_seg_name_values)


        # return updated config         
        return(build_config)




    # OUTPUT UPDATES
    # ==============

    # Updates real_time_refreshing OUTPUT fields whenever an update happens from a dynamic field
    def update_outputs(self, frontend_node: list, field_name: str, field_value: Any) -> dict:

        new_output_config = frontend_node.copy()

        # get num_segments
        if (field_name == "num_segments") and (isinstance(field_value, int | str)):
            num_segments = int(field_value)
        elif (hasattr(self, "num_segments")):
            num_segments = int(self.num_segments)
        else:
            num_segments = 0

        num_outputs = len(new_output_config["outputs"])

        # remove outputs
        if(num_outputs > num_segments + self.NUM_STATIC_OUTPUTS):
            # determine number to remove
            num_remove = num_outputs - (num_segments + self.NUM_STATIC_OUTPUTS)

            # temporarily remove the one at the very end 'Remainder Patient Flow'
            temp_holder = new_output_config["outputs"].pop()

            # pop the required number of outputs off the END of the list
            for i in range(num_remove):
                new_output_config["outputs"].pop()

            # add 'Remainder Patient Flow' back to the very end
            new_output_config["outputs"].append(temp_holder)
            pass

        # add outputs
        elif(num_outputs < num_segments + self.NUM_STATIC_OUTPUTS):
            # determine number to add
            num_add = (num_segments + self.NUM_STATIC_OUTPUTS) - num_outputs

            # temporarily remove the one at the very end 'Remainder Patient Flow'
            temp_holder = new_output_config["outputs"].pop()

            for i in range(num_add):
                new_display_name = f"Segment {i+1}"

                new_output_config["outputs"].append(Output(name = f"{self.COL_PREFIX}{i+1}", 
                                                           display_name = new_display_name, 
                                                           method = f"update_forecast_model_segment_{i+1}",
                                                           group_outputs = True))
            
            # add 'Remainder Patient Flow' back to the very end
            new_output_config["outputs"].append(temp_holder)

        # already equal, do nothing
        else:
            # same number of outputs, no more is required
            pass


        # go through all the new_outputs making sure that they have the latest segment names
        for i in range(num_segments):
            new_display_name = self.get_seg_display_name(i)

            if(new_display_name is None):
                new_display_name = f"Segment {i+1}"

            new_output_config["outputs"][i].display_name = new_display_name

        return(new_output_config)



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
        def new_funct(seg_num = seg_num, *args, **kwargs) -> Data:
            out = func(seg_num = seg_num, *args, **kwargs)
            return out
        
        return new_funct
    


    # INPUT/OUTPUTS CALCULATIONS
    # ==========================

    # _forecast_model_common_input
    # common code for all 'update_forcast_model' both segments remainder functions
    # INPUTS:
    #   seg_num = (optional) the current segment being returned, or "remainder" if none provided
    # OUTPUTS:
    #   updated_model = the updated ForecastDataModel
    #   updated_meta_data = the updated ForecastMetaDataFrame
    #   curr_seg_name = the name of the current segment (or "remainder")
    #   total_values_id = the id for the total number of patients for THIS segment, to be set to last_id outside of this component
    #   seg_group_id = the unique id for this component
    def _forecast_model_common_input(self, seg_num: int = None) -> tuple[DataFrame, ForecastMetaDataFrame, str, str, str]:
        (updated_model, updated_meta_data, curr_total_values_id, curr_display_name) = super()._forecast_model_common_input()

        # get the segment data
        segment_table = ForecastDataModel.astype_first_all_cols(self.segment_table)
        segment_names = DataFrame(self.segment_names)

        (updated_model, updated_meta_data, total_values_id) = self.controller.calc_segment_values(seg_num = seg_num,
                                                                                                  id = self._id, 
                                                                                                  display_name = self.get_display_name(), 
                                                                                                  segment_table = segment_table,
                                                                                                  segment_names = segment_names,
                                                                                                  col_prefix = self.COL_PREFIX, 
                                                                                                  num_static_cols = self.NUM_STATIC_COLS,
                                                                                                  curr_total_values_id = curr_total_values_id, 
                                                                                                  updated_model = updated_model, 
                                                                                                  updated_meta_data = updated_meta_data)
        
        return(updated_model, updated_meta_data, total_values_id)




    # update_forecast_model_segment
    # Add the segment % and the new total patients to the model for a specific segment
    # 
    # INPUTS:
    #   seg_num - the segment number that this output is handling
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_segment(self, seg_num: int = 1) -> Data:
        (updated_model, updated_meta_data, total_values_id) = self._forecast_model_common_input(seg_num)

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, total_values_id)
    

    # update_forecast_model_remainder
    # Add the remainder (1- segment %) and the new total to the model
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model_remainder(self) -> Data:
        (updated_model, updated_meta_data, total_values_id) = self._forecast_model_common_input()

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, total_values_id)


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

        if (field_name == "num_segments") and isinstance(field_value, (int, float, str)):
            num_segments = int(field_value)
        else:
            num_segments = int(self.num_segments)


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
            seg_display_name = self.get_seg_display_name(i)

            if(seg_display_name is None):
                seg_display_name = f"Segment {i+1}" # segment numbering starts at 1, not at zero

            table_schema.append({
                "name": f"{self.COL_PREFIX}{i+1}",
                "display_name": seg_display_name,
                "type": "float",
                "description": f"Percent of total population who are '{seg_display_name}', for each time period, expressed as decimal between 0 and 1 (i.e. 0.25, 0.5, etc.)",
                "disable_edit": False,
                "sortable": False,
                "filterable": False,
                "edit_mode": EditMode.INLINE,
            })

        
        build_config["segment_table"]["table_schema"]["columns"] = table_schema
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
                    curr_row[f"{self.COL_PREFIX}{i+1}"] = ForecastDataModel.EDITABLE_VALUES_TOKEN
            return(segment_table)
                
        # otherwise, resize the exist values into the new size (note: always add the dates in)
        else:
            old_values_df = ForecastDataModel.astype_first_all_cols(old_values)    # simple helper to make sure that the datatimes of the resulting DataFrame have the first col as type datetime, and all other cols as type float
            new_df = ForecastFormModelUtilities.refill_dataframe(new_dim_rows=num_rows, new_dim_cols=num_cols, prev_data=old_values_df, col_name_prefix=self.COL_PREFIX, dates=dates)
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
        


    # generate_table_seg_name_values
    # Based on the latest schema, generates the values for the table
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config

    def generate_table_seg_name_values(self, field_value: str, field_name: str) -> List[dict]:
        # determine how many rows we need
        if(field_name == "num_segments"):
            num_rows = int(field_value)
        else:
            num_rows = self.num_segments
            
        # Check if we have existing data
        old_values = self.segment_names
        if(old_values is not None and isinstance(old_values, list) and len(old_values) > 0):
            new_df = ForecastFormModelUtilities.fill_dataframe(new_dim_rows = num_rows,
                                                               new_dim_cols = 2,
                                                               prev_data  = old_values, 
                                                               default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                               col_name_prefix = None, 
                                                               num_static_cols = 2, 
                                                               seg_num = list(range(1, num_rows+1)),
                                                               seg_name = [f"Segment {i}" for i in range(1, num_rows+1)])
        else:
            new_df = ForecastFormModelUtilities.fill_dataframe(new_dim_rows = num_rows,
                                                               new_dim_cols = 2,
                                                               set_col_names = ["seg_num", "seg_name"],  
                                                               default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                               col_name_prefix = None, 
                                                               num_static_cols = 2, 
                                                               seg_num = list(range(1, num_rows+1)),
                                                               seg_name = [f"Segment {i}" for i in range(1, num_rows+1)])
        
        return(new_df.to_data_list())
    



    # update_seg_table_display_names
    # Update the display names for the segments in the Segments Table whenever updates are made to the Segment Names Table
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config

    def update_seg_table_display_names(self, build_config, field_value, field_name):

        if (field_name != "segment_names") or (not hasattr(self, "segment_names")) or (self.segment_names is None) or (len(self.segment_names) < 1):
            return(build_config)

        # get list of segment names
        seg_names = [self.segment_names[i]["seg_name"] for i in range(len(self.segment_names))]

        for i in range(len(seg_names)):
            build_config["segment_table"]["table_schema"]["columns"][i+self.NUM_STATIC_COLS]["display_name"] = seg_names[i]
            build_config["segment_table"]["table_schema"]["columns"][i+self.NUM_STATIC_COLS]["description"] = f"Percent of total population who are '{seg_names[i]}', for each time period, expressed as decimal between 0 and 1 (i.e. 0.25, 0.5, etc.)."

        return(build_config)
    

    

    # ================
    # HELPER FUNCTIONS
    # ================

    # get_seg_display_name
    # Does a "safe" get of a display name for a segment from the segment_name table, handling all the issues that may happen silently
    # 
    # INPUTS:
    #   idx - index of the display name (zero based index, same as the rest)
    #
    # OUTPUTS:
    #   str or None

    def get_seg_display_name(self, idx: int) -> str | None:
        if (not hasattr(self, "segment_names")) or (self.segment_names is None) or (len(self.segment_names) < 1):
            #print("\n\nWARNING:  (not hasattr(self, 'segment_names')) or (self.segment_names is None) or (len(self.segment_names)\n\n")
            return None
        
        if(len(self.segment_names) <= idx):
            #print("\n\nWARNING:  len(self.segment_names) <= idx\n\n")
            return None
        
        seg_names = [self.segment_names[i]["seg_name"] for i in range(len(self.segment_names))]

        return(seg_names[idx])
