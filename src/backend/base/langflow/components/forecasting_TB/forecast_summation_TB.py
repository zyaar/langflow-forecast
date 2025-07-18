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
class ForecastSummationTB(ForecastComponent):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"Summation TB"
    description: str = f"Sum up all the inputs provided and create a new totals line in the output."
    icon: str = f"Sigma"
    name: str = f"SummationTB"

    #INPUT / OUTPUT INFO
    VAR_IN_DISPLAY_NAME = "Forecast(s)"
    VAR_IN_INFO = "Time-based forecast Data"

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


    # COMPONENT INPUTS
    # ----------------
    inputs = []



    # COMPONENT OUTPUTS
    # -----------------
    outputs = []



    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    form_trigger_rules = []


    # INSTANCE ATTRIBUTES
    # generated during the __init__
    # -----------------------------
    # inputs - (list) InputTypes for the component
    # outputs - (list) OutputTypes for the component


    # __init__
    # --------
    def __init__(self, **kwargs) -> None:
        # generates some instance variables instead of using class variables, this allows us to customize
        # this instance variables in the children of this abstract class without having to rewrite all the

        # set-up inputs and outputs with the child class's configuration variables
        self.inputs = self.gen_inputs()
        self.outputs = self.gen_outputs()

        super().__init__(**kwargs)
    

    # GENERATE INPUTS / OUTPUTS
    # -------------------------
    def gen_inputs(self) -> list:
        inputs_list = [
            # common forecast inputs
            *ForecastComponent.inputs,

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
    

    def gen_outputs(self) -> list:
        outputs_list = [
            Output(display_name=f"{self.VAR_OUT_DISPLAY_NAME}", info = f"{self.VAR_OUT_INFO}", name=f"var_out", method=f"calc_var_out", hidden=f"{self.VAR_OUT_HIDDEN}"),        
        ]

        return(outputs_list)


    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        pass


    # OUTPUT FUNCTIONS
    # ----------------

    # calc_var_out
    # run the summation function and return the results
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_out(self) -> Data:
        self.validate_inputs()

        # sum up all the inputs to create a single total line and add it to the output model
        (updated_model, updated_meta_data, col_total_in_id) = self.check_and_combine_forecasts(totals_id = f"{self._id}_Total", 
                                                                              totals_display_name = f"{self.VAR_IN_DISPLAY_NAME}", 
                                                                              step_type = self.VAR_STEP_TYPE)

        # Add a treatment set-up instructions for a treatment section to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{self._id}_Init",
                                                                    display_name = self.display_name,
                                                                    data_values = None,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.SUMMATION,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [col_total_in_id])
                                                                                                                                                                                    
        # bundle the packet together for forwarding to next component(s)
        data_packet = self.gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data)
        return(data_packet)
    