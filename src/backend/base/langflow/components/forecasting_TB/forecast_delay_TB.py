#####################################################################
# forecast_delay_TB.py
#
# Implements the a summation component.  It's already implemented everywhere
# this just makes it explicit (for visual presentation purposes)
# 
# INPUTS:  DataFrame
# OUTPUTS:  DataFrame
#
#####################################################################

from langflow.custom import Component
from langflow.base.forecasting_common.views.forecast_delay_TB_view import ForecastDelayTBView

# CLASSES
# =======

# ForecastDelayTB
# Adds all the input streams together and results a new row with a total
class ForecastDelayTB(ForecastDelayTBView, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"Delay TB"
    description: str = f"Add a delay (in months) to any stream."
    icon: str = f"clock-arrow-up"
    name: str = f"DelayTB"

