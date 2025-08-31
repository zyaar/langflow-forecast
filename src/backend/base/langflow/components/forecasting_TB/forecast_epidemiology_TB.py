#####################################################################
# forecast_epidemiology_TB.py
#
# Implements the segment component of the forecasting in a TIME BASED model.
# The segment component applies one timescale based count of patients as a new line
# in the model
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  DataFrame (ForecastDataModel format)
#
#####################################################################

from langflow.custom import Component
from langflow.base.forecasting_common.views.forecast_epidemiology_TB_view import ForecastEpidemiologyTBView


# CLASSES
# =======

# ForecastEpidemiologyTB
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastEpidemiologyTB(ForecastEpidemiologyTBView, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = "Epidemiology TB"
    description: str = "Build an epidemiology stream of patients using a TIME BASED model."
    icon = "Globe"
    name: str = "EpidemiologyTB"
