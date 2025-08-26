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
from typing import Any, List, Tuple
import copy
import pandas as pd

# CLASSES
# =======
class ForecastSegmentTBController(ForecastSumInputTBController):

    # calc_segment_values
    # [TODO]
    #
    # INPUTS:
    #    seg_num
    #    id
    #    display_name
    #    segment_table
    #    col_prefix
    #    num_static_cols
    #    curr_total_values_id
    #    updated_model
    #    updated_meta_data
    #
    # OUTPUTS:
    #    updated_model
    #    updated_meta_data
    #    total_values_id
   def calc_segment_values(self,
                           seg_num: int,
                           id: str, 
                           display_name: str, 
                           segment_table: DataFrame, 
                           col_prefix: str, 
                           num_static_cols: int,
                           curr_total_values_id: str, 
                           updated_model: DataFrame, 
                           updated_meta_data: ForecastMetaDataFrame) -> Tuple[DataFrame, ForecastMetaDataFrame, str]:
        

        # placeholder for total_values_id (the id of the column with the total number of patients for the seg_num segment)
        total_values_id = None
        
        # Add a treatment set-up instructions for a treatment section to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{id}_Init",
                                                                    display_name = display_name,
                                                                    data_values = None,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [curr_total_values_id],
                                                                    update_last_id=True)
        
        # get the incoming patient flow (id and values)
        total_incoming_id = updated_meta_data.get_last_value_id()
        total_incoming_values = updated_meta_data.get_series(total_incoming_id)

        # get the segment table data
        segment_table = ForecastDataModel.astype_first_all_cols(segment_table)

    
        # create a segment group id, this is an ID prefix for all columns related to this segment group
        seg_group_id = f"{id}_{col_prefix}"
    

        # create the input columns for entering all the segment percentages, same as the InputTable in this component
        # we're going to generate and output these same rows for every segment output in the component, this way, the input
        # rows for the segments are all preserved, even if you don't combine all the outputs at the end in a summation component,
        # however, if you do combine multiple segment outputs, these input fields will be kept once and all other duplicate fields will be
        # removed as part of the drop-dups that happens whenever we combine multiple components.
        num_cols = len(segment_table.columns)
        pct_col_pred = []



        # ===============================================
        # INPUT:  % OF TOTAL INBOUND PATIENTS PER SEGMENT
        # ===============================================
        for i in range(num_static_cols, num_cols):       
            col_seg_name = segment_table.columns[i] # TODO:  Fix when we have a better way of setting names
            col_seg_values = segment_table[col_seg_name]

            # add segment's percent to data/model and meta-data
            col_seg_pct_id = f"{seg_group_id}{i}_Percent_Input"
            (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                        updated_meta_data,
                                                                        id = col_seg_pct_id,
                                                                        display_name = f"% of {col_seg_name} patients in {display_name}",
                                                                        data_values = col_seg_values,
                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                        action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                        data_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                        display_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}],
                                                                        update_last_id = True,)
            pct_col_pred.append(col_seg_pct_id)
            


        # ===============================================
        # TOTAL PATIENTS COVERED BY ANY/ALL SEGMENTATIONS
        # ===============================================
        # add total percent covered by all segments to data/model and meta-data
        col_seg_total_pct_id = f"{id}_Total_Percent"


        # In the DATA calculations:
        updated_model = ForecastDataModel.to_pandas(updated_model)
        col_seg_total_pct_values = updated_model[pct_col_pred].sum(axis=1)
        updated_model = ForecastDataModel.to_langflow(updated_model)

        # In the META-DATA calculation:
        # list of ids:  pct_col_pred

        # NOTE:  we do not check to see if the total percentages of all segments all up to 100% of less here, because we already did that when validating the table input data (see function: check_segment_pcts_add_up() in this file)
        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = col_seg_total_pct_id,
                                                                    display_name = f"% of patients covered in {display_name}",
                                                                    data_values = col_seg_total_pct_values,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.SUM,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}, 
                                                                                  {ForecastDataSeriesMetaDataValidationSchema.VALUE_CHECK: ForecastDataSeriesMetaDataComparisonType.LE}],
                                                                    pred = pct_col_pred, # this is already a list
                                                                    update_last_id = True,
                                                                    args = {ForecastDataSeriesMetaDataComparisonType.LE: 1}) # add argument with the value for LESS_EQUAL_THAN validation



        # ==========================================
        # TOTAL PATIENTS NOT COVERED BY SEGMENTATION
        # ==========================================
        col_seg_remainder_pct_id = f"{id}_Remainder_Percent"
        
        # In the DATA calculations:
        #col_seg_remainder_pct_values = 1 - col_seg_total_pct_values
        col_seg_remainder_pct_values = 1 - updated_model[col_seg_total_pct_id]

        # In the META-DATA calculation:
        pred_not_covered = [1, col_seg_total_pct_id]
        

        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = col_seg_remainder_pct_id,
                                                                    display_name = f"% of patients not covered in {display_name}",
                                                                    data_values = col_seg_remainder_pct_values.to_list(),
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                    action = ForecastDataSeriesMetaDataAction.SUB,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.PCT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = pred_not_covered,
                                                                    update_last_id = True)


        # NEW:
        # ==============================
        # TOTAL PATIENT FOR EACH SEGMENT
        # ==============================
        for i in range(num_static_cols, num_cols):       
            col_seg_name = segment_table.columns[i] # TODO:  Fix when we have a better way of setting names
            curr_seg_pct_id = f"{seg_group_id}{i}_Percent_Input"
            curr_seg_total_id = f"{seg_group_id}{i}_Total"

            # In the DATA calculations:
            col_curr_seg_total_values = updated_model[total_incoming_id] * updated_model[curr_seg_pct_id]

            # In the META-DATA calculation:
            preds_seg_num = [total_incoming_id, curr_seg_pct_id]


            (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                         updated_meta_data,
                                                                         id = curr_seg_total_id,
                                                                         display_name = f"# of {col_seg_name} patients in {display_name}",
                                                                         data_values = col_curr_seg_total_values.to_list(),
                                                                         step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                         action = ForecastDataSeriesMetaDataAction.PROD,
                                                                         data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                         display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                         validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                         pred = preds_seg_num,
                                                                         update_last_id = True)
            
            # if we're calculating totals for the seg_num, then save the total patients for this segment id
            # pass back to the output function to set as the last id
            if(i == seg_num):
                total_values_id = curr_seg_total_id

            
        # ===================================
        # TOTAL PATIENT FOR ALL SEGMENTS FLOW
        # ===================================
        #total_incoming_patients_value = updated_model[curr_total_values_id]
        total_pct_id = f"{id}_Total_Percent"
        total_total_id = f"{id}_Total_Total"

        # In the DATA calculations:
        total_total_values = updated_model[total_incoming_id] * updated_model[total_pct_id]

        # In the META-DATA calculation:
        preds_total_total = [total_incoming_id, total_pct_id]

        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = total_total_id,
                                                                     display_name = f"# of patients covered in {display_name}",
                                                                     data_values = total_total_values.to_list(),
                                                                     step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                     action = ForecastDataSeriesMetaDataAction.PROD,
                                                                     data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                     display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     pred = preds_total_total,
                                                                     update_last_id = True)

            
        # ================================
        # TOTAL PATIENT FOR REMAINDER FLOW
        # ================================
        #total_incoming_patients_value = updated_model[curr_total_values_id]
        remainder_pct_id = f"{id}_Remainder_Percent"
        remainder_total_id = f"{id}_Remainder_Total"

        # In the DATA calculations:
        remainder_total_values = updated_model[total_incoming_id] * updated_model[remainder_pct_id]

        # In the META-DATA calculation:
        preds_remainder_total = [total_incoming_id, remainder_pct_id]

        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = remainder_total_id,
                                                                     display_name = f"# of patients not covered in {display_name}",
                                                                     data_values = remainder_total_values.to_list(),
                                                                     step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT,
                                                                     action = ForecastDataSeriesMetaDataAction.PROD,
                                                                     data_type = ForecastDataSeriesMetaDataDataType.FLOAT,
                                                                     display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     pred = preds_remainder_total,
                                                                     update_last_id = True)
        
        # if we're calculating totals for the seg_num, then save the total patients for this segment id
        # pass back to the output function to set as the last id
        if(seg_num is None):
            total_values_id = remainder_total_id

        # finalize
        updated_model = ForecastDataModel.to_langflow(updated_model)
        updated_meta_data.set_last_id(total_values_id)

        return(updated_model, updated_meta_data, total_values_id)







    # ================
    # HELPER FUNCTIONS
    # ================
              