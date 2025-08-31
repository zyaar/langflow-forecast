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
# from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
# from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
# from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
# from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
# from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities
# from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket
from langflow.base.forecasting_common.components.forecast_component import ForecastComponent
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

from langflow.base.forecasting_common.builders.forecast_builder_excel_TB import ForecastBuilderExcelTB


# CLASSES
# =======

# ForecastBuildModelExcel
# This class takes a ForecastDataModel and exports it to an excel file Player
class ForecastBuildModelExcelView(ForecastComponent):

    # CONSTANTS
    # =========
    

    # COMPONENT META-DATA
    # ===================

    display_name = "Build Model - Excel TB"
    description = "Generate an excel forecasting model"
    icon = "save"
    name = "BuildModelExcelTBView"


    # GENERATE INPUTS / OUTPUTS
    # =========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            # parent attributes
            *super()._gen_inputs(),

            DataInput(
                name=f"forecasts_in",
                display_name=f"Forecast",
                info=f"Time-based forecast Data",
                dynamic=True,
                real_time_refresh=True,
                is_list = True,
            ),
            
            StrInput(
                name="template_file",
                display_name="Excel template File Path (including filename)",
                info="The full file path (including filename and extension).",
                value="./output/excel_player_template.xlsx",
            ),
            
            StrInput(
                name="file_path",
                display_name="File Path (including filename)",
                info="The full file path (including filename and extension).",
                value="./output/output",
            ),
        ]

        return(inputs_list)
    
    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),

            # output which generates the model and passes through the existing data for use later
            Output(
                name="forecast",
                display_name="Forecast",
                method="gen_excel_model",
                info="The forecast model going on as a pass through",
            ),

            # output (for debugging) which saves the existing data that gets fed to the generator
            Output(
                name="confirmation",
                display_name="Confirmation",
                method="save_to_file",
                info="Confirmation message after saving the file.",
            ),
        ]

        return(outputs_list)



    # OUTPUT FUNCTIONS
    # ================

    # _forecast_model_common_input(self)
    def _forecast_model_common_input(self):
        super()._forecast_model_common_input()

        # unpack the data packet into lists of data, meta_data, and ids
        (updated_models, updated_meta_datas, totals_ids, display_names) = self._unpack_data_packets(self.forecasts_in)

        if len(updated_models) != 1:
            raise ValueError(f"\n*  save_to_file:  required 1 and only 1 updated_models in input, {len(updated_models)} were provided")
        
        if len(updated_meta_datas) != 1:
            raise ValueError(f"\n*  save_to_file:  required 1 and only 1 updated_meta_datas in input, {len(updated_meta_datas)} were provided")
        
        if len(totals_ids) != 1:
            raise ValueError(f"\n*  save_to_file:  required 1 and only 1 totals_ids in input, {len(totals_ids)} were provided")


        return(updated_models[0], updated_meta_datas[0], totals_ids[0], display_names[0])
    

    # gen_excel_model
    def gen_excel_model(self) -> Data:
        (updated_model, updated_meta_data, total_id, display_name)  = self._forecast_model_common_input()

        file_path = Path(self.file_path).expanduser()
        file_path_xlsx = Path(self.file_path + ".xlsx").expanduser()

        # Ensure the directory exists
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)

        file_path = self._adjust_file_path_with_format(file_path)

        # build the excel model and save it
        render_excel = ForecastBuilderExcelTB(data_frame = updated_model, meta_data = updated_meta_data, output_location = file_path_xlsx, template_location = self.template_file)
        render_excel.build_player()

        # # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, check_ids = False)




    # save_to_file
    # Generate the forecast player for excel and save it to a file
    # 
    # INPUTS:
    # OUTPUTS:
    #   Message with confirmation of save
    def save_to_file(self) -> str:
        (updated_model, updated_meta_data, total_id, display_name)  = self._forecast_model_common_input()

        file_path = Path(self.file_path).expanduser()
        file_path_json = Path(self.file_path + ".json").expanduser()

        # Ensure the directory exists
        if not file_path.parent.exists():
            file_path.parent.mkdir(parents=True, exist_ok=True)

        file_path = self._adjust_file_path_with_format(file_path)

        data_packet = self._gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data, last_id = total_id, check_ids = False)
        self._pickle_and_save_data_packet(data_packet = data_packet, path = file_path)

        # Quick and dirty dump of the json file as well
        with open(file_path_json, "w") as f:
            f.write(updated_meta_data.to_json())

        return f"DataFrame and ForecastMetaDataFrame saved successfully as '{file_path}'"



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
