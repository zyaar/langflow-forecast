#####################################################################
# forecast_population_cut_TB.py
#
# Implements the population cut component of the forecasting in a TIME BASED model.
# The population cut component applies one timescale based percentage retention
# to the incoming flow and the remainer is passed through
# 
# INPUTS:  DataFrame
# OUTPUTS:  DataFrame
#
#####################################################################

from langflow.custom import Component
from langflow.io import StrInput, DataInput, IntInput, TableInput
from langflow.schema import DataFrame
from langflow.schema.table import EditMode
from langflow.template import Output
from langflow.field_typing.range_spec import RangeSpec

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.components.forecast_single_fix_col_transformer_TB import ForecastSingleFixedColTransformerTB
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
from typing import Any, List
from langflow.base.forecasting_common.controllers.forecast_population_cut_TB_controller import ForecastPopulationCutTBController


# CLASSES
# =======

# ForecastPopulationCutsTB
# This class represents dividing a stream of patients into a fixed number of segments, based on percentages of the total assigned at
# each time period of the forecast
class ForecastPopulationCutTB(ForecastSingleFixedColTransformerTB, Component):
    # COMPONENT INFO
    display_name: str = f"Population Cut TB"
    description: str = f"Apply a timescale specific % decrease criteria from the population flow input."
    icon: str = f"Scissors"
    name: str = f"PopulationCutTB"

    # VAR NAME
    VAR_NAME = "population_cut"

    # DATA TYPES
    VAR_IN_TYPE = ForecastDataSeriesMetaDataDataType.PCT
    VAR_IN_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.PCT

    VAR_OUT_TYPE = ForecastDataSeriesMetaDataDataType.FLOAT
    VAR_OUT_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.INT

    # INPUTS / OUTPUTS INFO
    VAR_IN_DISPLAY_NAME = "Forecast(s)"
    VAR_IN_INFO = "Time-based forecast Data"

    VAR_OUT_DISPLAY_NAME = "Cut patients"
    VAR_OUT_INFO = "Patients cut by the parameter"
    VAR_OUT_HIDDEN = True

    VAR_REMAINDER_OUTPUT = True
    VAR_OUT_REMAINDER_DISPLAY_NAME = "Remaining patients"
    VAR_OUT_REMAINDER_INFO = "Patients not cut by the parameter"
    VAR_REMAINDER_OUT_HIDDEN = False

    # INPUTTABLE INFO
    VAR_TABLE_DISPLAY_NAME = "Population cut"
    VAR_TABLE_INFO = f"{description}"
    VAR_TABLE_COL_VAR_NAME_POSTFIX = "Percent"
    VAR_TABLE_COL_DISPLAY_NAME = "Population cut"
    VAR_TABLE_COL_INFO = "% of total incoming population which is reduced in this time period"
    VAR_TABLE_COL_DATA_TYPE = "float"

    # BUILDER INFO
    VAR_STEP_TYPE = ForecastDataSeriesMetaDataStepTypes.POPULATION_CUT
    VAR_ACTION_FUNCT = ForecastDataSeriesMetaDataAction.PROD
    VAR_VALIDATION_FUNCTS = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}]
    VAR_PRED = []
    VAR_ARGS = None
    VAR_OBJS = None


    # INIT
    # ====
    def __init__(self, **kwargs) -> None:
        # set-up a controller if needed
        if not hasattr(self, "controller"):
            self.controller = ForecastPopulationCutTBController()

        super().__init__(**kwargs)
    
    
    # INPUT VALIDATION
    # ================
    def validate_inputs(self):
        pass
