#####################################################################
# forecast_sum_input_TB.py
#
# Implements the summation of the inputs that all components (except epi)
# have.
# 
# INPUTS:  DataFrame
# OUTPUTS:  DataFrame
#
#####################################################################


from langflow.custom import Component
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

from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket
from langflow.base.forecasting_common.controllers.forecast_sum_input_TB_controller import ForecastSumInputTBController



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import List, Tuple
import pandas as pd
import nanoid


# CLASSES
# =======

# ForecastSumInputTB
# Adds all the input streams together and results a new row with a total
class ForecastSumInputTB(ForecastComponent):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"Sum Input TB"
    description: str = f"Abstract base class for components that sum up all the inputs provided and create a new totals line in the output."
    icon: str = f""
    name: str = f"SumInputTB"

    # INPUT INFO
    VAR_IN_DISPLAY_NAME = "Forecast(s)"
    VAR_IN_INFO = "Time-based forecast Data"



    # INIT
    # ====
    def __init__(self, **kwargs) -> None:

        # set-up a controller if needed
        if not hasattr(self, "controller"):
            self.controller = ForecastSumInputTBController()

        super().__init__(**kwargs)




    # GENERATE INPUTS / OUTPUTS
    # =========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            # parent attributes
            *super()._gen_inputs(),

            # dataframes in List[DataFrame]
            DataInput(
                name=f"forecasts_in",
                display_name=f"{self.VAR_IN_DISPLAY_NAME}",
                info=f"{self.VAR_IN_INFO}",
                dynamic=True,
                real_time_refresh=True,
                is_list = True,
            ),
        ]

        return(inputs_list)
    

    # def _gen_outputs(self) -> list:
    #     outputs_list = [
    #         *super()._gen_outputs(),

    #         # add additional outputs here          
    #     ]

    #     return(outputs_list)


    # INPUT/OUTPUT VALIDATIONS
    # ========================
    # def validate_inputs(self):
    #     super().validate_inputs()

    # def validate_outputs(self):
    #     super().validate_outputs()



    # INPUT/OUTPUTS CALCULATIONS
    # ==========================

    def _forecast_model_common_input(self):
        super()._forecast_model_common_input()

        if(self.forecasts_in is None):
            raise ValueError(f"\n*  _forecast_model_common_input:  input '{self.get_input_display_name("forecasts_in")}' is not connected")

        # unpack the data packet into lists of data, meta_data, and ids
        (updated_models, updated_meta_datas, totals_ids, display_names) = self._unpack_data_packets(self.forecasts_in, convert_to_pandas = True)


        # combine all data_sets and return a single one
        (updated_model, updated_meta_data, totals_id, new_display_name) = self.controller.combine_and_sum(updated_models, updated_meta_datas, totals_ids, display_names)

        return(updated_model, updated_meta_data, totals_id, new_display_name)

        # # combine data frames and add a totals line if multiple are being added
        # (new_summation_created, updated_model, new_summation_id, new_total_line_id) = ForecastDataModel.concat_and_sum(datas = updated_models,
        #                                                                                                                drop_dups = True, 
        #                                                                                                                skip_total_if_one = True)
        # # get the totals_id for returning (we may have had to create one, or not, depending on how many inputs there were)
        # totals_id = updated_model.columns[-1]
        # totals_values = updated_model[totals_id]

        # # combine meta_datas and add a total instruction if multiple frames are being added
        # updated_meta_data = ForecastMetaDataFrame.concat_and_sum(datas = updated_meta_datas,
        #                                                          display_name = f"Total ({", ".join(display_names)})", # TODO get display names, not id's
        #                                                          new_summation_id = new_summation_id,
        #                                                          new_total_line_id = new_total_line_id,
        #                                                          new_total_values = totals_values,
        #                                                          is_total = True,
        #                                                          verify_integrity = False, 
        #                                                          drop_dups = True)
        # return(updated_model, updated_meta_data, totals_id)
    



    # def _combine_and_sum(updated_models: list[DataFrame], 
    #                      updated_meta_datas: list[ForecastMetaDataFrame], 
    #                      totals_ids: list[str], 
    #                      display_names: list[str]) -> Tuple[DataFrame, ForecastMetaDataFrame, str, str]:

    #     if len(updated_meta_datas) == 1:
    #         last_value_id = updated_meta_datas[0].get_last_value_id()
    #         last_display_name = updated_meta_datas[0].get_series(last_value_id).get_display_name()
    #         return(updated_models[0], updated_meta_datas[0], last_value_id, last_display_name)

    #     # get the list of the last_value_ids for all meta_datas
    #     list_of_last_value_ids = []
    #     list_of_display_names = []

    #     for updated_meta_data in updated_meta_datas:
    #         last_value_id = updated_meta_data.get_last_value_id()

    #         if(last_value_id in list_of_last_value_ids):
    #             raise ValueError(f"\n* _combine_and_sum:  error, attempting to add duplicate last_value_id '{last_value_id}'.")

    #         list_of_last_value_ids.append(last_value_id)
    #         list_of_display_names.append(updated_meta_data.get_series(last_value_id).get_display_name())



    #     # create new last_id
    #     new_totals_id = "_Total"

    #     # calculate display_name
    #     new_display_name = f"Total ({", ".join(list_of_display_names)})"

    #     # combine all columns in DATA - removing duplicate column names
    #     updated_model = ForecastDataModel.concat(datas = updated_models, drop_dups = True)        

    #     # combine all series in META-DATA - removing duplicate series names
    #     updated_meta_data = ForecastMetaDataFrame.concat(updated_meta_datas, drop_dups = True)

    #     # In the DATA calculations: create new total line
    #     updated_model[new_totals_id] = updated_model[list_of_last_value_ids].sum(axis = 1)


    #     # In the META-DATA calculations:  create new total line
    #     updated_meta_data = 



    #     # return values
    #     return(updated_model, updated_meta_data, new_last_id, new_display_name)

	# Children MUST PROVIDES
    # ======================
    #     Component:  display_name, description, icon, name
    #
	# 	Functions:
	# 		_gen_inputs - if need additional inputs (inherit from super() at the start, then add your stuff)
	# 		_gen_outputs - if need additional outputs (inherit from super() at the start, then add your stuff)
    #
	# 		_validate_inputs() - if validation is required
	# 		_validate_outputs() - if validation is required
    #
    #         _forecast_model_common_input() - UPDATE if additional steps are needed
    #         _forecast_model_common_output() - UPDATE if additional steps are needed
    #
	# 		Add output_functions:
    #             - call _forecast_model_common_input() FIRST
    #
    #             # write custom code here

	# 			- call _forecast_model_common_output() LAST 
	# 			  (data: DataFrame | pd.DataFrame, meta_data: ForecastMetaDataFrame, check_ids: bool = True)


