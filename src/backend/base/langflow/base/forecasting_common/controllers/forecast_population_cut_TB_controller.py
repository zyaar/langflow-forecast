from langflow.schema import DataFrame
from langflow.base.forecasting_common.constants import ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeries,
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
                                                                        ForecastMetaDataRange,
                                                                        ForecastMetaDataRangeSchema,
                                                                        ForecastDataSeriesMetaDataComparisonType)

from langflow.base.forecasting_common.controllers.forecast_single_fix_col_transformer_TB_controller import ForecastSingleFixedColTransformerTBController


# COMPONENT SPECIFIC IMPORTS
# ==========================
from enum import Enum
from typing import Any, List, Tuple
import copy
import pandas as pd
import nanoid


# ENUMERATIONS
# ============

# Enum of ForecastMetaDataFrame
# The data storage attributes of this object / structure
class ForecastPopulationCutTBOutputCalc(str, Enum):
    VAR = "var"
    VAR_REMAINDER = "var_remainder"


# CLASSES
# =======
class ForecastPopulationCutTBController(ForecastSingleFixedColTransformerTBController):


    def calc_remainder_common(self,
                              var_col_input_id: str,
                              var_col_remainder_pct_id: str,
                              var_out_pct_remainder_display_name: str,
                              var_step_type: ForecastDataSeriesMetaDataStepTypes,
                              var_in_type: ForecastDataSeriesMetaDataDataType,
                              var_in_display_type: ForecastDataSeriesMetaDataDataType,
                              updated_model: DataFrame,
                              updated_meta_data: ForecastMetaDataFrame):
        
        # % REMAINDER
        
        # for DATA calculations
        col_var_remainder_values = 1 - updated_model[var_col_input_id]

        # for META-DATA calculation
        col_var_remainder_pred = [1, var_col_input_id]

        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = var_col_remainder_pct_id,
                                                                     display_name = f"{var_out_pct_remainder_display_name}",
                                                                     data_values = col_var_remainder_values.to_list(),
                                                                     step_type = var_step_type,
                                                                     action = ForecastDataSeriesMetaDataAction.SUB,
                                                                     data_type = var_in_type,
                                                                     display_type = var_in_display_type,
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     pred = col_var_remainder_pred,
                                                                     update_last_id = True,
                                                                     args = None,
                                                                     objs = None)

        #return everything
        return(updated_model, updated_meta_data, var_col_remainder_pct_id)
    

   
   # component_specific_calcs
   # this is where this class and all it's childer do their specific calculations
   # and return the updated model, meta-data and the id of the output column
   # INPUTS:
   #    output_type: ForecastPopulationCutTBOutputCalc,
   #    var_col_calc_out_id: str,
   #    var_remainder_col_calc_out_id: str,
   #    var_out_display_name: str,
   #    var_remainder_out_display_name: str,
   #    var_step_type: ForecastDataSeriesMetaDataStepTypes,
   #    var_out_type: ForecastDataSeriesMetaDataDataType,
   #    var_out_display_type: ForecastDataSeriesMetaDataDataType,
   #    var_col_input_id: str,
   #    var_remainder_input_id: str,
   #    updated_model: DataFrame | pd.DataFrame, 
   #    updated_meta_data: ForecastMetaDataFrame, 
   #    col_total_in_id: str
   #
   # OUTPUTS:
   #   (updated_model, updated_meta_data, col_total_in_id) - the updated model, meta-data and the id of the output column
    
    def component_specific_calcs(self,
                                 output_type: ForecastPopulationCutTBOutputCalc,
                                 var_col_calc_out_id: str,
                                 var_remainder_col_calc_out_id: str,
                                 var_out_display_name: str,
                                 var_remainder_out_display_name: str,
                                 var_step_type: ForecastDataSeriesMetaDataStepTypes,
                                 var_out_type: ForecastDataSeriesMetaDataDataType,
                                 var_out_display_type: ForecastDataSeriesMetaDataDataType,
                                 var_col_input_id: str,
                                 var_remainder_input_id: str,
                                 updated_model: DataFrame | pd.DataFrame, 
                                 updated_meta_data: ForecastMetaDataFrame, 
                                 col_total_in_id: str) -> tuple[DataFrame | pd.DataFrame, ForecastMetaDataFrame, str]:
        
        # GET THE % INPUT * TOTAL_IN FROM THE PARENT COMPONENT
        (updated_model, 
         updated_meta_data,
         var_col_calc_out_id) = super().component_specific_calcs(var_col_calc_out_id = var_col_calc_out_id,
                                                                 var_out_display_name = var_out_display_name,
                                                                 var_step_type = var_step_type,
                                                                 var_out_type = var_out_type,
                                                                 var_out_display_type = var_out_display_type,
                                                                 var_col_input_id = var_col_input_id,
                                                                 updated_model = updated_model, 
                                                                 updated_meta_data = updated_meta_data,
                                                                 col_total_in_id = col_total_in_id)
        

        # ADD THE REMAINDER (1 - % INPUT) * TOTAL_IN ROW
        # In the DATA calculations:
        col_var_remainder_action_values = updated_model[col_total_in_id] * updated_model[var_remainder_input_id]

        # In the META-DATA calculation:
        prod_remainder_preds = [col_total_in_id, var_remainder_input_id]

        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = var_remainder_col_calc_out_id,
                                                                     display_name = var_remainder_out_display_name,
                                                                     data_values = col_var_remainder_action_values.to_list(),
                                                                     step_type = var_step_type,
                                                                     action = ForecastDataSeriesMetaDataAction.PROD,
                                                                     data_type = var_out_type,
                                                                     display_type = var_out_display_type,
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     pred = prod_remainder_preds,
                                                                     update_last_id = False,
                                                                     args = None,
                                                                     objs = None)
        
        if output_type == ForecastPopulationCutTBOutputCalc.VAR:
            updated_meta_data.set_last_id(var_col_calc_out_id)
            return(updated_model, updated_meta_data, var_col_calc_out_id)
        else:
            updated_meta_data.set_last_id(var_remainder_col_calc_out_id)
            return(updated_model, updated_meta_data, var_remainder_col_calc_out_id)
        

