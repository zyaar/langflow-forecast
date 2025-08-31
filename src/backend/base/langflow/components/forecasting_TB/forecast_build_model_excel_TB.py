#####################################################################
# forecast_build_model_excel_TB.py
#
# Takes a model and renders it to an excel file
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  Message confirmation
#
#####################################################################

from langflow.custom import Component
from langflow.base.forecasting_common.views.forecast_build_model_excel_TB_view import ForecastBuildModelExcelView

# CLASSES
# =======

# ForecastBuildModelExcel
# This class takes a ForecastDataModel and exports it to an excel file Player
class ForecastBuildModelExcel(ForecastBuildModelExcelView, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name = "Build Model - Excel TB"
    description = "Generate an excel forecasting model"
    icon = "save"
    name = "BuildModelExcelTB"
