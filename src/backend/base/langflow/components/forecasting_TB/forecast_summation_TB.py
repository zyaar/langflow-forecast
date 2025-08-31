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
from langflow.base.forecasting_common.views.forecast_summation_TB_view import ForecastSummationTBView

# CLASSES
# =======

# ForecastSummationTB
# Adds all the input streams together and results a new row with a total
class ForecastSummationTB(ForecastSummationTBView, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"Summation TB"
    description: str = f"Sum up all the inputs provided and create a new totals line in the output."
    icon: str = f"Sigma"
    name: str = f"SummationTB"
