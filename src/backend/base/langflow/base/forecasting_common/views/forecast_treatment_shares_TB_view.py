#####################################################################
# forecast_treatment_shares_TB.py
#
# Implements the the share of each treatment in the of the forecasting in a TIME BASED model.
# The this component applies one timescale based percentage to the incoming flow
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  DataFrame (ForecastDataModel format)
#
#####################################################################

from langflow.custom import Component
from langflow.io import StrInput, DataInput, IntInput, TableInput, NestedDictInput
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

from langflow.base.forecasting_common.views.forecast_segment_TB_view import ForecastSegmentTBView

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
                                                                        ForecastDataSeriesMetaDataComparisonType)



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List
from langflow.base.forecasting_common.controllers.forecast_segment_TB_controller import ForecastSegmentTBController



# CLASSES
# =======

# ForecastTreatmentSharesTB
# This class represents dividing a stream of patients into a fixed number of segments, each segment is assigned to a different treatment
class ForecastTreatmentSharesTBView(ForecastSegmentTBView, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT
    display_name: str = "Treatment Shares TB"
    description: str = "Apply a timescale specific % split critera for each branch which represents the % share of patients treated with a downstream treatment component"
    icon = "ChartPie"
    name: str = "TreatmentSharesTBView"

    # COL_SET_VAR
    COL_PREFIX = "segment_"
    MAX_SEGMENTS = 100

    # INPUTS / OUTPUTS
    NUM_STATIC_COLS = 1 # one static columns in 'treatment_share_table' ('Date' is static, rest is segment specific)
    NUM_STATIC_OUTPUTS = 1 # only 'Remainder Patient Flow'
    STATIC_OUTPUTS_AT_START = False


