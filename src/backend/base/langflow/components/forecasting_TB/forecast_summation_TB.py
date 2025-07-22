#####################################################################
# forecast_summation_TB.py
#
# Implements the a summation component.  It's already implemented everywhere
# this just makes it explicit (for visual presentation purposes)
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

from langflow.base.forecasting_common.components.forecast_sum_input_TB import ForecaseSumInputTB

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import List
import pandas as pd
import nanoid


# CLASSES
# =======

# ForecastSummationTB
# Adds all the input streams together and results a new row with a total
class ForecastSummationTB(ForecaseSumInputTB, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"Summation TB"
    description: str = f"Sum up all the inputs provided and create a new totals line in the output."
    icon: str = f"Sigma"
    name: str = f"SummationTB"

    # OUTPUT INFO
    VAR_OUT_DISPLAY_NAME = "Total"
    VAR_OUT_INFO = "Total of all incoming streams"
    VAR_OUT_HIDDEN = False

    # BUILDER INFO
    VAR_STEP_TYPE = ForecastDataSeriesMetaDataStepTypes.SUMMATION
    VAR_ACTION_FUNCT = ForecastDataSeriesMetaDataAction.SUM
    VAR_VALIDATION_FUNCTS = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}]
    VAR_PRED = []
    VAR_ARGS = None
    VAR_OBJS = None


    # GENERATE INPUTS / OUTPUTS
    # =========================
    # def gen_inputs(self) -> list:
    #     inputs_list = [
    #         *super().gen_inputs(),

    #         # add any custom inputs here if needed
    #     ]

    #     return(inputs_list)
    

    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),

            # add any custom outputs here if needed
            Output(display_name=f"{self.VAR_OUT_DISPLAY_NAME}", info = f"{self.VAR_OUT_INFO}", name=f"var_out", method=f"calc_var_out", hidden=f"{self.VAR_OUT_HIDDEN}"),
        ]

        return(outputs_list)



    # INPUT/OUTPUT VALIDATIONS
    # ========================
    # def validate_inputs(self):
    #     super().validate_inputs()

    # def validate_outputs(self):
    #     super().validate_outputs()




    # OUTPUT FUNCTIONS
    # ================

    # calc_var_out
    # run the calss specific calcs, then bundle the results into a data packet and return it
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_out(self) -> Data:
        # call common functions
        (updated_model, updated_meta_data, col_total_in_id) = self._forecast_model_common_input()

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, col_total_in_id)
