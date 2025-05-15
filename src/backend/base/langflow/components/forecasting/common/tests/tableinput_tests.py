from langflow.base.data.utils import TEXT_FILE_TYPES, parallel_load_data, parse_text_file_to_data, retrieve_file_paths
from langflow.custom import Component
from langflow.io import DropdownInput, IntInput, FloatInput, TableInput
from langflow.schema import DataFrame
from langflow.schema.table import EditMode
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from typing import cast
from langflow.components.forecasting.common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecatModelTimescale
from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime
from typing import List
import numpy as np
from langflow.components.forecasting.common.date_utils import gen_dates

# Patient Count Table
testTable = TableInput(
    name="patient_count_table",
    display_name="Patient Counts (by Month)",
    info="For each time period, enter the expected number of patients entering the forecast.",
    required=False,
    show=False,
    dynamic=True,
    table_schema=[
        {
            "name": "date",
            "display_name": "Date",
            "type": "date",
            "description": "Date of patient count",
            #"edit_mode": EditMode.INLINE,
            "disable_edit": True,
        },
        {
            "name": "patient_count_table",
            "display_name": "Patient Count",
            "type": "int",
            "description": "Patient count",
            #"edit_mode": EditMode.INLINE,
        },
    ],
    value=[
        {"date": "2020-01-01", "patient_count": 10},
        {"date": "2020-02-01", "patient_count": 20},
        {"date": "2020-03-01", "patient_count": 30},
    ],
)

print(testTable)
print(type(testTable))
print(testTable.display_name)
dataFrameTest = DataFrame(data=testTable)
print(dataFrameTest)
