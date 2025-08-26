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

from langflow.base.forecasting_common.controllers.forecast_sum_input_TB_controller import ForecastSumInputTBController


# COMPONENT SPECIFIC IMPORTS
# ==========================
from enum import Enum
from typing import Any, List, Tuple
import copy
import pandas as pd


# ENUMERATIONS
# ============

# Enum of ForecastMetaDataFrame
# The data storage attributes of this object / structure
class ForecastSingleFixedColTransformerTBOutputCalc(str, Enum):
    VAR = "var"
    VAR_REMAINDER = "var_remainder"


# CLASSES
# =======
class ForecastSingleFixedColTransformerTBController(ForecastSumInputTBController):
   
   # calc_single_fix_col_transform
   # [TODO]
   #
   # INPUTS:
   # id
   # display_name
   # var_args
   # var_col_input_id
   # var_col_name
   # var_col_remainder_pct_id
   # var_in_display_type
   # var_in_type
   # var_objs
   # var_out_remainder_display_name
   # var_pred
   # var_remainder_output
   # var_step_type
   # var_table
   # var_table_col_display_name
   # var_validation_functs
   #
   # OUTPUTS:
   #    updated_model
   #    updated_meta_data
   #    total_values_id

   def calc_single_fix_col_transform(self,
                                     id :str,
                                     display_name: str,
                                     var_args: dict,
                                     var_col_input_id: str,
                                     var_col_name: str,
                                     var_col_remainder_pct_id: str,
                                     var_in_display_type: ForecastDataSeriesMetaDataDataType,
                                     var_in_type: ForecastDataSeriesMetaDataDataType,
                                     var_objs: dict,
                                     var_out_remainder_display_name: str,
                                     var_pred: list,
                                     var_remainder_output: bool,
                                     var_step_type: ForecastDataSeriesMetaDataStepTypes,
                                     var_table: DataFrame,
                                     var_table_col_display_name: str,
                                     var_validation_functs: dict,
                                     col_total_in_id: str,
                                     updated_model: DataFrame,
                                     updated_meta_data: ForecastMetaDataFrame) -> Tuple[DataFrame, ForecastMetaDataFrame, str, pd.Series, pd.Series, pd.Series]:
        
        # Add a step set-up instructions to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{id}_Init",
                                                                    display_name = display_name,
                                                                    data_values = None,
                                                                    step_type = var_step_type,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    update_last_id = True,
                                                                    pred = [col_total_in_id])

        # get the values of the totals_in column
        col_total_in_values = updated_model[col_total_in_id]

        # get the var table data and make sure it's data types are set correctly (date fields and float fields)
        var_table = ForecastDataModel.astype_first_all_cols(var_table)

        # get var input col values
        col_var_values = var_table[var_col_name]
        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = var_col_input_id,
                                                                     display_name = f"{var_table_col_display_name}",
                                                                     data_values = col_var_values.to_list(),   # this is a DataFrame, so don't need to convert to
                                                                     step_type = var_step_type,
                                                                     action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                     data_type = var_in_type,
                                                                     display_type = var_in_display_type,
                                                                     validation = var_validation_functs,
                                                                     update_last_id = True,
                                                                     pred = var_pred,
                                                                     args = var_args,
                                                                     objs = var_objs)
        
        # calcuate remainder percent col values
        if(var_remainder_output):         
            # calculate the percent of the remainder
            col_var_remainder_values = 1 - col_var_values
            (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                         updated_meta_data,
                                                                         id = var_col_remainder_pct_id,
                                                                         display_name = f"{var_out_remainder_display_name}",
                                                                         data_values = col_var_remainder_values.to_list(),
                                                                         step_type = var_step_type,
                                                                         action = ForecastDataSeriesMetaDataAction.SUB,
                                                                         data_type = var_in_type,
                                                                         display_type = var_in_display_type,
                                                                         validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                         pred = [1, var_col_input_id],
                                                                         update_last_id = True,
                                                                         args = None,
                                                                         objs = None)
            
        else:
            col_var_remainder_values = None


        # return everything
        return(updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values)



   # _component_specific_calcs
   # this is where this class and all it's childer do their specific calculations
   # and return the updated model, meta-data and the id of the output column
   # INPUTS:
   #    var_col_calc_id: str,
   #    var_out_display_name: str,
   #    var_step_type: ForecastDataSeriesMetaDataStepTypes,
   #    var_out_type: ForecastDataSeriesMetaDataDataType,
   #    var_out_display_type: ForecastDataSeriesMetaDataDataType,
   #    var_col_input_id: str,
   #    updated_model: DataFrame | pd.DataFrame, 
   #    updated_meta_data: ForecastMetaDataFrame, 
   #    col_total_in_id: str,
   #    col_total_in_values: pd.Series, 
   #    col_var_values: pd.Series, 
   #    col_var_remainder_values: pd.Series
   #
   # OUTPUTS:
   #   (updated_model, updated_meta_data, col_total_in_id) - the updated model, meta-data and the id of the output column
    
   def component_specific_calcs(self,
                                 calc_type : ForecastSingleFixedColTransformerTBOutputCalc,
                                 var_col_calc_id: str,
                                 var_col_remainder_calc_id: str,
                                 var_out_display_name: str,
                                 var_out_remainder_display_name: str,
                                 var_step_type: ForecastDataSeriesMetaDataStepTypes,
                                 var_out_type: ForecastDataSeriesMetaDataDataType,
                                 var_out_display_type: ForecastDataSeriesMetaDataDataType,
                                 var_col_input_id: str,
                                 var_col_remainder_pct_id: str,
                                 updated_model: DataFrame | pd.DataFrame, 
                                 updated_meta_data: ForecastMetaDataFrame, 
                                 col_total_in_id: str,
                                 col_total_in_values: pd.Series, 
                                 col_var_values: pd.Series, 
                                 col_var_remainder_values: pd.Series) -> tuple[DataFrame | pd.DataFrame, ForecastMetaDataFrame]:
        
        # In the DATA calculations:
        col_var_action_values = updated_model[col_total_in_id] * updated_model[var_col_input_id]
        col_var_action_remainder_values = updated_model[col_total_in_id] * updated_model[var_col_remainder_pct_id]

        # In the META-DATA calculation:
        prod_preds = [col_total_in_id, var_col_input_id]
        prod_remainder_preds = [col_total_in_id, var_col_remainder_pct_id]

        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = var_col_calc_id,
                                                                     display_name = f"{var_out_display_name}",
                                                                     data_values = col_var_action_values.to_list(),
                                                                     step_type = var_step_type,
                                                                     action = ForecastDataSeriesMetaDataAction.PROD,
                                                                     data_type = var_out_type,
                                                                     display_type = var_out_display_type,
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     pred = prod_preds,
                                                                     update_last_id = True,
                                                                     args = None,
                                                                     objs = None)
        
        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = var_col_remainder_calc_id,
                                                                     display_name = f"{var_out_remainder_display_name}",
                                                                     data_values = col_var_action_values.to_list(),
                                                                     step_type = var_step_type,
                                                                     action = ForecastDataSeriesMetaDataAction.PROD,
                                                                     data_type = var_out_type,
                                                                     display_type = var_out_display_type,
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     pred = prod_preds,
                                                                     update_last_id = True,
                                                                     args = None,
                                                                     objs = None)
        
        if calc_type == ForecastSingleFixedColTransformerTBOutputCalc.VAR:
            updated_meta_data.set_last_id(var_col_calc_id)
        else:
            updated_meta_data.set_last_id(var_col_remainder_calc_id)


        return(updated_model, updated_meta_data)




    # ================
    # HELPER FUNCTIONS
    # ================

