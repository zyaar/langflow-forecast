#####################################################################
# forecast_segment_TB.py
#
# Implements the segment component of the forecasting in a TIME BASED model.
# The segment component applies one timescale based percentage to the incoming flow
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  DataFrame (ForecastDataModel format)
#
#####################################################################

from langflow.custom import Component
from langflow.base.forecasting_common.views.forecast_treatment_shares_TB_view import ForecastTreatmentSharesTBView

# CLASSES
# =======

# ForecastTreatmentSharesTB
# This class represents dividing a stream of patients into a fixed number of segments, each segment is assigned to a different treatment

class ForecastTreatmentSharesTB(ForecastTreatmentSharesTBView, Component):


    # CONFIG CONSTANTS
    # ================

    # COMPONENT
    display_name: str = "Treatment Shares TB"
    description: str = "Apply a timescale specific % split critera for each branch which represents the % share of patients treated with a downstream treatment component"
    icon = "ChartPie"
    name: str = "TreatmentSharesTB"
