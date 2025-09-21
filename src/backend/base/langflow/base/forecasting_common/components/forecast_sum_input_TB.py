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
    VAR_IN_NAME = "forecasts_in"
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
                #name=f"forecasts_in",
                name=f"{self.VAR_IN_NAME}",
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


