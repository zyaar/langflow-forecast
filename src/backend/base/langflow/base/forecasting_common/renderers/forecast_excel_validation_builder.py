#####################################################################
# forecast_excel_validation_builder.py
#
# Helper object which builds an Excel DataValidation rule 
# using openpyxl which can be added to a cell
#
#####################################################################


from typing import List, Dict, Tuple, Any
from datetime import datetime
import pandas as pd
import numpy as np
from langflow.schema.dataframe import DataFrame, Data


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
                                                                        ForecastDataSeriesMetaDataValidateValueChecks)


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime
from enum import Enum
import shutil
from openpyxl import Workbook, worksheet, cell, load_workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Protection


# GLOBAL CONST
# ============

ForecastExcelDataTypeToValidationMap = {ForecastDataSeriesMetaDataDataType.INT: "whole",
                                        ForecastDataSeriesMetaDataDataType.FLOAT: "decimal",
                                        ForecastDataSeriesMetaDataDataType.DATE: "date",
                                        ForecastDataSeriesMetaDataDataType.CURRENCY: "decimal",
                                        ForecastDataSeriesMetaDataDataType.PCT: "decimal"}




# CLASSES
# =======


class ForecastExcelValidationRanges(str, Enum):
    GT = "greaterThan"
    GE = "greaterThanOrEqualTo"
    EQ = "equalTo"
    NE = "notEqualTo"
    LE = "lessThanOrEqualTo"
    LT = "lessThan"
    BETWEEN = "between"
    NOT_BETWEEN = "notBetween"


class ForecastExcelValidationRuleBuilder:

    data_type: str = None
    error_message: str = None
    error_title: str = "Invalid entry"
    default_value = None
    prompt: str = None
    prompt_title:str = None
    range_check: ForecastExcelValidationRanges = None
    allow_blank: bool = True
    target_value: int | float = None
    min_value: int | float = None
    max_value: int | float = None

    # def generate_data_entry_rule(self,
    #                              data_type: ForecastDataSeriesMetaDataDataType,
    #                              error_message: str,
    #                              error_title: str = "Invalid entry",
    #                              default_value: Any = None,
    #                              prompt: str = None,
    #                              prompt_title: str = None,
    #                              range_check: ForecastExcelValidationRanges = None, 
    #                              allow_blank: bool = True, 
    #                              target_value: int | float = None,
    #                              min_value: int | float = None,
    #                              max_value: int | float = None) -> DataValidation:

    def generate_data_entry_rule(self) -> DataValidation:
        # determine if we are using ValidationRanges or NOT
        if self.range_check is None:
            rule = DataValidation(type = self.data_type, allow_blank = self.allow_blank)
        else:
            match self.range_check:
                case ForecastExcelValidationRanges.GT:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.GT.value, formula1 = self.min_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.GE:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.GE.value, formula1 = self.min_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.EQ:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.EQ.value, formula1 = self.target_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.NE:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.NE.value, formula1 = self.target_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.LE:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.LE.value, formula1 = self.max_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.LT:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.LT.value, formula1 = self.max_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.BETWEEN:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.BETWEEN.value, formula1 = self.min_value, formula2 = self.max_value, allow_blank = self.allow_blank)
                case ForecastExcelValidationRanges.NOT_BETWEEN:
                    rule = DataValidation(type = self.data_type, operator = ForecastExcelValidationRanges.NOT_BETWEEN.value, formula1 = self.min_value, formula2 = self.max_value, allow_blank = self.allow_blank)
                case _:
                    raise ValueError(f"\n*  generate_data_entry_rule:  Invalid range_check value:  {self.range_check}")

        # add error message (required)
        rule.error = self.error_message
        rule.errorTitle = self.error_title

        # add prompt (optional)
        if(self.prompt is not None):
            rule.prompt = self.prompt
            rule.promptTitle = self.prompt_title

        return(rule)
