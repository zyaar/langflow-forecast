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
from langflow.io import StrInput, DataInput, IntInput, TableInput
from langflow.schema import DataFrame, Data
from langflow.schema.table import EditMode
from langflow.template import Output
from langflow.field_typing.range_spec import RangeSpec

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.components.forecast_single_fix_col_transformer_TB import ForecastSingleFixedColTransformerTB
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List
from langflow.base.forecasting_common.controllers.forecast_population_cut_TB_controller import ForecastPopulationCutTBController, ForecastPopulationCutTBOutputCalc
import pandas as pd


# CLASSES
# =======

# ForecastPopulationCutsTB
# This class represents dividing a stream of patients into a fixed number of segments, based on percentages of the total assigned at
# each time period of the forecast
class ForecastPopulationCutTBView(ForecastSingleFixedColTransformerTB, Component):
    # COMPONENT INFO
    display_name: str = f"Population Cut TB"
    description: str = f"Apply a timescale specific % decrease criteria from the population flow input."
    icon: str = f"Scissors"
    name: str = f"PopulationCutTBView"

    # VAR NAME
    VAR_NAME = "population_cut"
    VAR_REMAINDER_NAME = "population_cut_remainder"

    # DATA TYPES
    VAR_IN_TYPE = ForecastDataSeriesMetaDataDataType.PCT
    VAR_IN_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.PCT

    VAR_OUT_TYPE = ForecastDataSeriesMetaDataDataType.FLOAT
    VAR_OUT_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.INT

    # INPUTS / OUTPUTS INFO
    VAR_IN_DISPLAY_NAME = "Forecast(s)"
    VAR_IN_INFO = "Time-based forecast Data"

    VAR_OUT_DISPLAY_NAME = "# of population cut"
    VAR_OUT_INFO = "Patients cut by the parameter"
    VAR_OUT_HIDDEN = True

    VAR_REMAINDER_OUTPUT = True
    VAR_OUT_PCT_REMAINDER_DISPLAY_NAME = "% of population remaining"
    VAR_OUT_REMAINDER_DISPLAY_NAME = "# of population remaining"
    VAR_OUT_REMAINDER_INFO = "Patients not cut by the parameter"
    VAR_REMAINDER_OUT_HIDDEN = False

    # INPUTTABLE INFO
    VAR_TABLE_DISPLAY_NAME = "Population cut"
    VAR_TABLE_INFO = f"{description}"
    VAR_TABLE_COL_VAR_NAME_POSTFIX = "Percent"
    VAR_TABLE_COL_DISPLAY_NAME = "% of population cut"
    VAR_TABLE_COL_INFO = "% of total incoming population which is reduced in this time period"
    VAR_TABLE_COL_DATA_TYPE = "float"

    # BUILDER INFO
    VAR_STEP_TYPE = ForecastDataSeriesMetaDataStepTypes.POPULATION_CUT
    VAR_ACTION_FUNCT = ForecastDataSeriesMetaDataAction.PROD
    VAR_VALIDATION_FUNCTS = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}]
    VAR_PRED = []
    VAR_ARGS = None
    VAR_OBJS = None


    # INIT
    # ====
    def __init__(self, **kwargs) -> None:
        # set-up a controller if needed
        if not hasattr(self, "controller"):
            self.controller = ForecastPopulationCutTBController()

        super().__init__(**kwargs)




    # GENERATE INPUTS / OUTPUTS
    # -========================

    # def _gen_inputs(self) -> list:
    #     inputs_list = [
    #         *super()._gen_inputs(),
    #     ]

    #     return(inputs_list)


    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),
            Output(display_name=f"{self.VAR_OUT_REMAINDER_DISPLAY_NAME}", name=f"var_remainder_out", method=f"calc_var_remainder_out", hidden=f"{self.VAR_REMAINDER_OUT_HIDDEN}"),
        ]
        
        return(outputs_list)


    
    
    # INPUT/OUTPUT VALIDATIONS
    # ========================
    # def validate_inputs(self):
    #     super().validate_inputs()

    # def validate_outputs(self):
    #     super().validate_outputs()




    # OUTPUT FUNCTIONS
    # ================

    # calc_var_out
    # Calcuate the variable action out (i.e. total_in * variable)
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_out(self) -> Data:
        (updated_model, updated_meta_data, last_id) = self._forecast_model_common_input(output_type = ForecastPopulationCutTBOutputCalc.VAR)

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, last_id)


    # calc_var_remainder_out
    # Calcuate the variable remainder action out (i.e. total_in * (1 - variable))
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_remainder_out(self) -> Data:
        (updated_model, updated_meta_data, last_id) = self._forecast_model_common_input(output_type = ForecastPopulationCutTBOutputCalc.VAR_REMAINDER)

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, last_id)
    




    # _forecast_model_common_input
    # common code for all 'both var and var_remainder output functions
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   updated_model = the updated ForecastDataModel
    #   updated_meta_data = the updated ForecastMetaDataFrame
    #   col_total_in_id = the name of the totals_in columns
    #   col_var_id = the name of the var input values columns
    #   col_total_in_values = (pd.Series) the values in the totals in column
    #   col_var_values = (pd.Series) the values in the var input values column

    def _forecast_model_common_input(self, output_type: ForecastPopulationCutTBOutputCalc) -> tuple[DataFrame, ForecastMetaDataFrame, str, pd.Series, pd.Series, pd.Series]:
        (updated_model, updated_meta_data, col_total_in_id, var_col_input_id) = super()._forecast_model_common_input()

        (updated_model, updated_meta_data, var_col_remainder_pct_id) = self.controller.calc_remainder_common(var_col_input_id = var_col_input_id,
                                                                                                             var_col_remainder_pct_id = self.var_col_remainder_pct_id,
                                                                                                             var_out_pct_remainder_display_name = self.VAR_OUT_PCT_REMAINDER_DISPLAY_NAME,
                                                                                                             var_step_type = self.VAR_STEP_TYPE,
                                                                                                             var_in_type = self.VAR_IN_TYPE,
                                                                                                             var_in_display_type = self.VAR_IN_DISPLAY_TYPE,
                                                                                                             updated_model = updated_model,
                                                                                                             updated_meta_data = updated_meta_data)
        

        (updated_model, updated_meta_data, last_id)  = self.controller.component_specific_calcs(output_type = output_type,
                                                                                                var_col_calc_out_id = self.var_col_calc_id,
                                                                                                var_remainder_col_calc_out_id = self.var_col_remainder_calc_id,
                                                                                                var_out_display_name = self.VAR_OUT_DISPLAY_NAME,
                                                                                                var_remainder_out_display_name = self.VAR_OUT_REMAINDER_DISPLAY_NAME,
                                                                                                var_step_type = self.VAR_STEP_TYPE,
                                                                                                var_out_type = self.VAR_OUT_TYPE,
                                                                                                var_out_display_type = self.VAR_OUT_DISPLAY_TYPE,
                                                                                                var_col_input_id = var_col_input_id,
                                                                                                var_remainder_input_id = var_col_remainder_pct_id,
                                                                                                updated_model = updated_model, 
                                                                                                updated_meta_data = updated_meta_data, 
                                                                                                col_total_in_id = col_total_in_id)

        return(updated_model, updated_meta_data, last_id)
    


