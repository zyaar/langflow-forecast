import pandas as pd
from langflow.schema.data import Data
from langflow.schema.dataframe import DataFrame

from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.components.forecasting.common.date_utils import gen_dates


# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastDataModel
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastDataModel(DataFrame):
    RESERVED_COLUMN_INDEX_NAME = "dates" # name of the dates column for the forecasting model.  This must be unique

    def __init__(
                self,
                data: list[dict] | list[Data] | pd.DataFrame | None = None,
                text_key: str = "text",
                default_value: str = "",
                start_year: int = datetime.now().year + 1,
                num_years: int = 5,
                input_type: ForecastModelInputTypes = None,
                start_month: int = FORECAST_COMMON_MONTH_NAMES_AND_VALUES["January"], # default to January start date
                timescale: ForecastModelTimescale = None,
                **kwargs,
                ):
            
            # call the parent method
            super().__init__(data, text_key, default_value, **kwargs)

            # set-up the additional forecasting-specific attributes
            self.start_year = start_year
            self.num_years = num_years
            self.input_type = input_type
            self.start_month = start_month
            self.timescale = timescale

            
