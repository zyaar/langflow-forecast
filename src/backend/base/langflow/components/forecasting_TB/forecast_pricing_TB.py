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
from langflow.base.forecasting_common.views.forecast_pricing_TB_view import ForecastPricingTBView


# CLASSES
# =======

# ForecastPricingTB
# This class represents converting a series of Product Rx / SKU orders in to a revenue stream by applying price
class ForecastPricingTB(ForecastPricingTBView, Component):

    # CONFIG CONSTANTS
    # ================
    
    # COMPONENT INFO
    display_name: str = f"Pricing TB"
    description: str = f"Apply a timescale specific price to a series of Product/SKU Rx/orders to return a revenue stream."
    icon: str = f"DollarSign"
    name: str = f"PricingTB"
