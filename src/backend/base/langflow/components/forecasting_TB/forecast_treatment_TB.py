#####################################################################
# forecast_treatment_TB.py
#
# Implements the treatment component of the forecasting in a TIME BASED model.
# This component manages the progression curve (in months) for patients in a specific treatment
# as well as the product Rx provided at each step
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  DataFrame (ForecastDataModel format)
#
#####################################################################

from langflow.custom import Component
from langflow.base.forecasting_common.views.forecast_treatment_TB_view import ForecastTreatmentTBView

# CLASSES
# =======

# ForecastTreatmentTB
# This class represents applying a treatment regiment of products to an incoming patient flow
class ForecastTreatmentTB(ForecastTreatmentTBView, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT
    display_name: str = "Treatment TB"
    description: str = "Apply a treatment regiment of products to an incoming patient flow"
    icon = "Syringe"
    name: str = "TreatmentTB"
