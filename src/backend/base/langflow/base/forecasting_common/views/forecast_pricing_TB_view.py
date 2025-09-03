#####################################################################
# forecast_pricing_TB.py
#
# Implements the pricing component of the forecasting in a TIME BASED model.
# The pricing component applies a TIME-BASED price on a Product / SKU to
# return a revenue stream
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
from langflow.base.forecasting_common.controllers.forecast_pricing_TB_controller import ForecastPricingTBController


# CLASSES
# =======

# ForecastPricingTB
# This class represents converting a series of Product Rx / SKU orders in to a revenue stream by applying price
class ForecastPricingTBView(ForecastSingleFixedColTransformerTB, Component):
    # COMPONENT INFO
    display_name: str = f"Pricing TB"
    description: str = f"Apply a timescale specific price to a series of Product/SKU Rx/orders to return a revenue stream."
    icon: str = f"DollarSign"
    name: str = f"PricingTBView"

    # VAR NAME
    VAR_NAME = "pricing"

    # DATA TYPES
    VAR_IN_TYPE = ForecastDataSeriesMetaDataDataType.CURRENCY
    VAR_IN_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.CURRENCY

    VAR_OUT_TYPE = ForecastDataSeriesMetaDataDataType.FLOAT
    VAR_OUT_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.CURRENCY

    # INPUTS / OUTPUTS INFO
    VAR_IN_DISPLAY_NAME = "Forecast(s)"
    VAR_IN_INFO = "Time-based forecast Data"

    VAR_OUT_DISPLAY_NAME = "SKU revenue"
    VAR_OUT_INFO = "SKU revenue"
    VAR_OUT_HIDDEN = False

    # VAR_REMAINDER_OUTPUT = False
    # VAR_OUT_REMAINDER_DISPLAY_NAME = None
    # VAR_OUT_REMAINDER_INFO = None
    # VAR_REMAINDER_OUT_HIDDEN = True

    # INPUTTABLE INFO
    VAR_TABLE_DISPLAY_NAME = "Price"
    VAR_TABLE_INFO = f"{description}"
    VAR_TABLE_COL_VAR_NAME_POSTFIX = "Per_SKU"
    VAR_TABLE_COL_DISPLAY_NAME = "Price per SKU"
    VAR_TABLE_COL_INFO = "Price per SKU for each time period"
    VAR_TABLE_COL_DATA_TYPE = "float"

    # BUILDER INFO
    VAR_STEP_TYPE = ForecastDataSeriesMetaDataStepTypes.PRICING
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
            self.controller = ForecastPricingTBController()

        super().__init__(**kwargs)
    
    
    
    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        pass
