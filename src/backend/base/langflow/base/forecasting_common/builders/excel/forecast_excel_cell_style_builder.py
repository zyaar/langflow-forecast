#####################################################################
# forecast_excel_cell_style_builder.py
#
# Helper object which builds an Excel DataValidation rule 
# using openpyxl which can be added to a cell
#
#####################################################################


# from typing import List, Dict, Tuple, Any
# from datetime import datetime
# import pandas as pd
# import numpy as np
# from langflow.schema.dataframe import DataFrame, Data


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.models.forecast_meta_data import ForecastDataSeriesMetaDataDataType, ForecastDataSeriesMetaDataValidateInputRestrictions, ForecastMetaDataSeries


# COMPONENT SPECIFIC IMPORTS
# ==========================
from openpyxl.cell.cell import Cell
from openpyxl.styles import Protection



# CLASSES
# =======
class ForecastExcelCellStyleBuilder:
    # MAP to data_type to excel_styles READ ONLY
    ForecastExcelDataTypeToCellStyleMapReadOnly = {ForecastDataSeriesMetaDataDataType.INT: "Read Only Int",
                                                   ForecastDataSeriesMetaDataDataType.FLOAT: "Read Only Float",
                                                   ForecastDataSeriesMetaDataDataType.DATE: "Read Only Date",
                                                   ForecastDataSeriesMetaDataDataType.CURRENCY: "Read Only Currency",
                                                   ForecastDataSeriesMetaDataDataType.PCT: "Read Only Percent"}
    
    # MAP to data_type to excel_styles READ/WRITE
    ForecastExcelDataTypeToCellStyleMapReadWrite = {ForecastDataSeriesMetaDataDataType.INT: "Input Int",
                                                    ForecastDataSeriesMetaDataDataType.FLOAT: "Input Float",
                                                    ForecastDataSeriesMetaDataDataType.DATE: "Input Date",
                                                    ForecastDataSeriesMetaDataDataType.CURRENCY: "Input Currency",
                                                    ForecastDataSeriesMetaDataDataType.PCT: "Input Percent"}
    
    # EXCEL STYLES
    # ============
    EXCEL_STYLES_WORKSHEET_HEADER = "Headline 1"
    EXCEL_ROW_HEADER_LABEL = "Row Header Label"
    EXCEL_STYLES_DEFAULT_INIT_STEP_HEADER = "Headline 3"
    EXCEL_STYLES_DEFAULT_INIT_GROUP_HEADER = "Headline 4"




    # CLASS ATTRIBUTES
    # ================

    # generate_cell_data_type_style
    # generate the cell style and protection based on the data type and restriction
    # INPUTS:
    #   curr_cell: Cell - the current cell to apply the style to
    #   display_type: ForecastDataSeriesMetaDataDataType - the data type to apply
    #   restriction: ForecastDataSeriesMetaDataValidateInputRestrictions - the restriction to apply
    #   curr_cell_meta_data: ForecastMetaDataSeries - the meta data for the current cell
    
    # OUTPUTS:
    #   NA - the cell is modified in place

    @staticmethod
    def generate_cell_data_type_style(curr_cell: Cell,
                                      display_type: ForecastDataSeriesMetaDataDataType,
                                      restriction: ForecastDataSeriesMetaDataValidateInputRestrictions,
                                      curr_cell_meta_data: ForecastMetaDataSeries = None):
            match restriction:
                case ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY:
                    curr_cell.style = ForecastExcelCellStyleBuilder.ForecastExcelDataTypeToCellStyleMapReadOnly[display_type]
                    curr_cell.protection = Protection(locked = True, hidden = False)

                case ForecastDataSeriesMetaDataValidateInputRestrictions.READ_WRITE:
                    curr_cell.style = ForecastExcelCellStyleBuilder.ForecastExcelDataTypeToCellStyleMapReadWrite[display_type]
                    curr_cell.protection = Protection(locked = False, hidden = False)

                case _:
                    raise ValueError(f"\n* generate_cell_data_type_style:  invalid Data Model restriction '{restriction}' requested for cell at '{curr_cell.coordinate}'")
                 
    @staticmethod
    def generate_ws_header(cell: Cell):
         cell.style = ForecastExcelCellStyleBuilder.EXCEL_STYLES_WORKSHEET_HEADER

    @staticmethod
    def generate_init_step_header(cell: Cell):
         cell.style = ForecastExcelCellStyleBuilder.EXCEL_STYLES_DEFAULT_INIT_STEP_HEADER
         
    @staticmethod
    def generate_init_group_header(cell: Cell):
         cell.style = ForecastExcelCellStyleBuilder.EXCEL_STYLES_DEFAULT_INIT_GROUP_HEADER
         
    @staticmethod
    def generate_row_header_label(cell: Cell):
        cell.style = ForecastExcelCellStyleBuilder.EXCEL_ROW_HEADER_LABEL