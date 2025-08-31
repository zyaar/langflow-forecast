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
from langflow.base.forecasting_common.views.forecast_segment_TB_view import ForecastSegmentTBView

# CLASSES
# =======

# ForecastSegmentTB
# This class represents dividing a stream of patients into a fixed number of segments, based on percentages of the total assigned at
# each time period of the forecast

class ForecastSegmentTB(ForecastSegmentTBView, Component):

    # CONFIG CONSTANTS
    # ================
    
    # COMPONENT
    display_name: str = "Segment TB"
    description: str = "Apply a timescale specific % split critera each branch (segement, remainder) of which can be linked to a different flow."
    icon = "Puzzle"
    name: str = "SegmentTB"
