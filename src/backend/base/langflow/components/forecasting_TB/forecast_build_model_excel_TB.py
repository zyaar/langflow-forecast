#####################################################################
# forecast_build_model_excel_TB.py
#
# Takes a model and renders it to an excel file
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  Message confirmation
#
#####################################################################

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities
from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket
from langflow.schema import Data, DataFrame


# COMPONENT SPECIFIC IMPORTS
# ==========================
from enum import Enum
import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
import pickle

import pandas as pd

from langflow.custom import Component
from langflow.io import (
    DataInput,
    Output,
    StrInput,
)



# CLASSES
# =======

# ForecastBuildModelExcel
# This class takes a ForecastDataModel and exports it to an excel file Player
class ForecastBuildModelExcel(Component):

    # CONSTANTS
    # =========
    

    # COMPONENT META-DATA
    # ===================

    display_name = "Build Model - Excel TB"
    description = "Generate an excel forecasting model"
    icon = "save"
    name = "BuildModelExcelTB"


    # COMPONENT INPUTS
    # ================

    inputs = [
        DataInput(
            name=f"forecast_in",
            display_name=f"Forecast",
            info=f"Time-based forecast Data",
            dynamic=True,
            real_time_refresh=True,
            is_list = True,
        ),
        StrInput(
            name="file_path",
            display_name="File Path (including filename)",
            info="The full file path (including filename and extension).",
            value="./output/output",
        ),
    ]


    # COMPONENT OUTPUTS
    # =================

    outputs = [
        Output(
            name="confirmation",
            display_name="Confirmation",
            method="save_to_file",
            info="Confirmation message after saving the file.",
        ),
    ]



    # FORM UPDATE RULES
    # =================
    form_update_rules = {}
    form_trigger_rules = []


    # update_build_config
    # Updates real_time_refreshing INPUTS fields whenever an update happens from a dynamic field
    def update_build_config(self, build_config, field_value, field_name=None):

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
                                                               field_name=field_name,)

        # return updated config         
        return(build_config)
    
    

    # OUTPUT FUNCTIONS
    # ================

    # save_to_file
    # Generate the forecast player for excel and save it to a file
    # 
    # INPUTS:
    # OUTPUTS:
    #   Message with confirmation of save
    def save_to_file(self) -> str:
        self.validate_inputs()

        file_path = Path(self.file_path).expanduser()

        # Ensure the directory exists
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)

        file_path = self._adjust_file_path_with_format(file_path)

        data_packet = self.forecast_in
        return self._save_data_packet(data_packet, file_path) 



    # INPUT VALIDATION
    # ================
    def validate_inputs(self):
        msg = ""

        # TODO:  ADD COMPONENT SPECIFIC CODE HERE
            
        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)






    # HELPER FUNCTIONS
    # ================

    # _adjust_file_path_with_format
    # HELPER FUNCTION:  create the right format save file with path
    # 
    # INPUTS:
    #   path - relative path to save file
    # OUTPUTS:
    #   Path - PurePath class to save file
    def _adjust_file_path_with_format(self, path: Path) -> Path:
        file_extension = path.suffix.lower().lstrip(".")
        return Path(f"{path}.pickle").expanduser() if file_extension not in ["pickle"] else path
    


    # _save_dataframe
    # HELPER FUNCTION:  create the right format save file with path
    # 
    # INPUTS:
    #   dataframe - ForecastDateModel to use to generate model
    #   path - file path to save model location
    # OUTPUTS:
    #   Confirmation message
    def _save_dataframe(self, dataframe: DataFrame, path: Path) -> str:
        dataframe.to_excel(path, index=False, engine="openpyxl")
        return f"DataFrame saved successfully as '{path}'"
    
    # _save_data_packet
    # HELPER FUNCTION:  save a data_packet to a pickle file
    # 
    # INPUTS:
    #   data_packet - data_packet used to generate the forecast
    #   path - file path to save the data_packet
    #
    # OUTPUTS:
    #   Confirmation message
    def _save_data_packet(self, data_packet: Data, path: Path) -> str:
        #dataframe.to_excel(path, index=False, engine="openpyxl")
        ForecastDataPacket.pickle_data_packet(data_packet = data_packet, path = path)
        return f"DataFrame saved successfully as '{path}'"
