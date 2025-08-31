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
from langflow.base.forecasting_common.views.forecast_population_cut_TB_view import ForecastPopulationCutTBView


# CLASSES
# =======

# ForecastPopulationCutsTB
# This class represents dividing a stream of patients into a fixed number of segments, based on percentages of the total assigned at
# each time period of the forecast

class ForecastPopulationCutTB(ForecastPopulationCutTBView, Component):

    # CONFIG CONSTANTS
    # ================
    
    # COMPONENT INFO
    display_name: str = f"Population Cut TB"
    description: str = f"Apply a timescale specific % decrease criteria from the population flow input."
    icon: str = f"Scissors"
    name: str = f"PopulationCutTB"
