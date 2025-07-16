#####################################################################
# forecast_component.py
#
# Abstract class that handles common boilerplate for all forecast components
# 
#
#####################################################################

from langflow.custom import Component
from langflow.io import TableInput, IntInput, StrInput
from langflow.schema import DataFrame, Data
from langflow.schema.table import EditMode
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
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

from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket


# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import List, Dict, Any
import pandas as pd
import copy


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastComponent
# This abstract class provides common functionality for all forecasting components, including a mechanism
# to ensure that the key ForecastDataModel shared variables are always appending to the input
class ForecastComponent(Component):

    # COMPONENT META-DATA
    # -------------------

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
    ]



    # COMPONENT OUTPUTS
    # -----------------


    # COMPONENT FORM UPDATE RULES
    # ---------------------------

    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing fields whenever an update happens from a dynamic field
    # -------------------


    # INPUT VALIDATION
    # ----------------



    # COMMON HELPER FUNCTIONS
    # -----------------------

    # builds on top of helper functions already provided Langflow's component class:
    #
    # UI
    #   Get the display name of an input:  get_input_display_name(self, input_name: str) -> str
    #   Get the display name of an output:  get_output_display_name(self, output_name: str) -> str
    #
    # ERROR HANDLING
    #   Build an error message for an input:  build_input_error_message(self, input_name: str, message: str) -> str
    #   Build an error message for an output: build_output_error_message(self, output_name: str, message: str) -> str
    #   Build an error message for the component:  build_component_error_message(self, message: str) -> str
    #   In CustomComponent:  update_frontend_node


    # get_input_table_col_display_name
    # Convenience function to get the display name of a column in an input_table
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_name = name of the column as defined in the TableSchema
    # 
    def get_input_table_col_display_name(self, table_name: str,  col_name: str) -> str:
        if table_name in self._inputs:
             if col_name in self._inputs[table_name]:
                  return getattr(self.inputs[table_name][col_name], "display_name", col_name)
        return col_name
    

    # unpack_data_packet
    def unpack_data_packets(self, data_packet: list[Data]) -> tuple[DataFrame, ForecastMetaDataFrame]:
        (dataframe, meta_data) = ForecastDataPacket.unpack_data_packets(data_packet)
        return(dataframe, meta_data)

    def unpack_data_packet(self, data_packet: Data) -> tuple[DataFrame, ForecastMetaDataFrame]:
        (dataframe, meta_data) = ForecastDataPacket.unpack_data_packet(data_packet)
        return(dataframe, meta_data)

    def gen_data_packet(self, dataframe: DataFrame | pd.DataFrame, meta_data: ForecastMetaDataFrame, check_ids: bool = True) -> Data:
         data_packet = ForecastDataPacket.gen_data_packet(dataframe = dataframe, meta_data = meta_data, check_ids = check_ids)
         return(data_packet)


    

    # check_and_combine_forecasts
    # Handles all the forecast_in data unpacking, DataFrame and ForecastMetaDataFrame conslidation
    # 
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   DataFrame
    def check_and_combine_forecasts(self, totals_id: str, totals_display_name: str, step_type: ForecastDataSeriesMetaDataStepTypes) -> tuple[DataFrame, ForecastMetaDataFrame]:
        (dataframes_in, meta_datas_in) = ForecastDataPacket.unpack_data_packets(self.forecasts_in)

        # combine data frames and add a totals line if multiple are being added
        updated_model = ForecastDataModel.concat_and_sum(datas=dataframes_in, new_col_name = totals_id, skip_total_if_one=True)

        # combine meta_datas and add a total instruction if multiple frames are being added
        updated_meta_data = ForecastMetaDataFrame.concat_and_sum(datas = meta_datas_in, 
                                                                 series_id = totals_id, 
                                                                 step_type = step_type, 
                                                                 display_name = totals_display_name, 
                                                                 verify_integrity = False, 
                                                                 drop_dups = True)
        return (updated_model, updated_meta_data)




    # add_col_data_meta
    # Handles the addition of a action to both the DataFrame and the ForecastMetaDataFrame
    # NOTE:  Due to desire for strict typing, this takes explicity arguments that go into
    #        generating a new ForecastMetaDataSeries.  This means that if ForecastMetaDataSeriesSchema
    #        is updated, it will need to be reflected here.
    # 
    # INPUTS:
    #   dataframe = existing dataframe to update
    #   meta_data = existing ForecastMetaDataFrame to update
    #   id = unique id for the new action column to be added
    #   display_name = user friendly name for the action column
    #   data_values = the data values to add to the dataframe (as a pandas Series)
    #   [next several arguments are all taken from the Schema definition for ForecastMetaDataSeries, see forecast_meta_data.py for more information]
    #   vertify_integrity (optional) = (default: True) set True if you want the function to raise an error if the id being added already exists in the dataframe/meta_data
    #   drop_dups (optional) = (default: False) set True if you want the function to automatically discard any a new column if it's id already exists in the dataframe / meta_data, if this is set, vertify_integrity setting is ignored 
    #   
    # OUTPUTS:
    #   DataFrame = updated DataFrame with the new action column
    #   ForecastMetaDataFrame = updated ForecastMetaDataFrame with the new action column

    def add_col_data_meta(self,
                          dataframe: DataFrame | pd.DataFrame,
                          meta_data: ForecastMetaDataFrame,
                          id: str,
                          display_name: str,
                          data_values: pd.Series,
                          step_type: ForecastDataSeriesMetaDataStepTypes,
                          action: ForecastDataSeriesMetaDataAction,
                          data_type: ForecastDataSeriesMetaDataDataType,
                          display_type: ForecastDataSeriesMetaDataDataType,
                          validation: List[Dict[ForecastDataSeriesMetaDataValidationSchema, Any]],
                          pred: List[str | int | float] = None,
                          args: Dict = None,
                          objs: List = None,
                          verify_integrity: bool = True,
                          drop_dups: bool = False) -> tuple[DataFrame, ForecastMetaDataFrame]:
           
           # add col to meta_data
           new_meta_col = ForecastMetaDataSeries(id = id,
                                                 step_type = step_type,
                                                 action = action,
                                                 data_type = data_type,
                                                 display_type = display_type,
                                                 display_name = display_name,
                                                 validation = validation,
                                                 pred = pred,
                                                 args = args,
                                                 objs = objs)
           updated_meta_data = ForecastMetaDataFrame.concat([meta_data, new_meta_col], verify_integrity = verify_integrity, drop_dups = drop_dups)

           # add col to data
           updated_dataframe = ForecastDataModel.add_col_to_model(dataframe, data_values.to_list(), new_col_name=id)

           return(updated_dataframe, updated_meta_data)
