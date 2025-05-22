#####################################################################
# forecast_segment.py
#
# Implements the segment component of the forecasting.  The segment
# component takes as input one or more DataFrames, merges them, and
# can either then pass them out as is, or provide a split of the
# data based on percentages which can be wired up to other outputs
# 
# INPUTS:  List[DataFrame]
# OUTPUTS:  List[DataFrame]
#
#####################################################################

from langflow.base.data.utils import TEXT_FILE_TYPES, parallel_load_data, parse_text_file_to_data, retrieve_file_paths
from langflow.custom import Component
from langflow.io import DropdownInput, IntInput, FloatInput, TableInput, DataFrameInput
from langflow.schema import DataFrame
from langflow.schema.table import EditMode
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from typing import cast
from langflow.components.forecasting.common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.components.forecasting.common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc



# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime
from typing import List
import numpy as np
from langflow.components.forecasting.common.date_utils import gen_dates


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastEpidemiology
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastSegment(Component):

    # COMPONENT META-DATA
    # -------------------
    display_name = "Segment"
    description = "Take one or more streams of patients (aggregated if multple), then apply split critera each branch of which can be linked to a different flow."
    icon = "Puzzle"
    name = "Segment"

    # COMPONENT INPUTS
    # ----------------
    inputs = [
        # DataFrame inputs
        DataFrameInput(
            name="dfs_in",
            display_name="Patient flow data",
            info="One or more input flows.  If multiple provided, it will automatically sum up all the flows.",
            is_list = True,
        ),

        #
        # TODO:  Add Input fields here
        #
    ]


    # COMPONENT OUTPUTS
    # -----------------
    outputs = [
        Output(
            name="df_out_1",
            display_name="Unsegments patients",
            info="This Output returns all patients who where NOT captured in the segment filters above (i.e. if the total of all segment filters is less than 100% or 1.0).",
            method="apply_segment_filter"),

        #
        # TODO:  Add Output fields here
        #
    ]


    # FORM UPDATE RULES
    # -----------------
    
    #
    # TODO:  Add Form update and calculated field rules here
    #



    # UPDATE_BUILD_CONFIG (update dynamically changing fields in the form of the component)
    # -------------------
    def update_build_config(self, build_config, field_value, field_name = None):

        #
        # TODO:  Add update_build_config logic here
        #

         # return updated config         
        return(build_config)



    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        #
        # TODO:  Add validation logic here
        #
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
        
    # aggregate_patient_flows
    # Merge a list of patient flows into one a single dataframe.
    # 
    # INPUTS:
    #   None
    # OUTPUTS:
    #   List[DataFrame] - A single merged dataframe with a total and line item of the input dataframes
    def aggregate_patient_flows(self) -> List[DataFrame]:
        self.validate_inputs()
        dataframes_to_combine = ForecastDataModel.convert_dfs_to_forecast_models(self.dfs_in)

        return ForecastDataModel.combine_forecast_models(datas=dataframes_to_combine,
                                                         agg_col_funct="sum",
                                                         agg_col_name=f"Total_{self.name}")     
    
    
    # apply_segment_filter
    # Perform all the merge and split logic of the segment component
    # 
    # INPUTS:
    #   None
    # OUTPUTS:
    #   List[DataFrame] - A list of dataframes split by the filter applied
    def apply_segment_filter(self) -> List[DataFrame]:
        return(self.aggregate_patient_flows())