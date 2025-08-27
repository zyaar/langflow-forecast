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

from langflow.base.forecasting_common.controllers.forecast_controller import ForecastController


# COMPONENT SPECIFIC IMPORTS
# ==========================
from enum import Enum
from typing import Any, List, Tuple
import copy
import pandas as pd
import nanoid


# ENUMERATIONS
# ============

# CLASSES
# =======
class ForecastSumInputTBController(ForecastController):
   
    def combine_and_sum(self,
                        updated_models: list[DataFrame], 
                        updated_meta_datas: list[ForecastMetaDataFrame], 
                        totals_ids: list[str], 
                        display_names: list[str]) -> Tuple[DataFrame, ForecastMetaDataFrame, str, str]:

        if len(updated_meta_datas) == 1:
            last_value_id = updated_meta_datas[0].get_last_value_id()
            last_display_name = updated_meta_datas[0].get_series(last_value_id).get_display_name()

            # NOTE:  added a deep copy here since the objects seem to be getting passed as pointers and that's screwing
            # up the fact that we call this funciton multiple times, once per output
            return(copy.deepcopy(updated_models[0]), copy.deepcopy(updated_meta_datas[0]), last_value_id, last_display_name)

        # get the list of the last_value_ids for all meta_datas
        list_of_last_value_ids = []
        list_of_display_names = []

        for updated_meta_data in updated_meta_datas:
            last_value_id = updated_meta_data.get_last_value_id()

            if(last_value_id in list_of_last_value_ids):
                raise ValueError(f"\n* _combine_and_sum:  error, attempting to add duplicate last_value_id '{last_value_id}'.")

            list_of_last_value_ids.append(last_value_id)
            list_of_display_names.append(updated_meta_data.get_series(last_value_id).get_display_name())


        # create new last_id
        new_total_group_id = f"SummationTB_{nanoid.generate(size=5)}"
        new_totals_step_init_id = f"{new_total_group_id}_Init"
        new_totals_id = f"{new_total_group_id}_Total"

        # calculate display_name
        new_display_name = f"Total ({", ".join(list_of_display_names)})"

        # combine all columns in DATA - removing duplicate column names
        updated_model = ForecastDataModel.concat(datas = updated_models, drop_dups = True)        

        # combine all series in META-DATA - removing duplicate series names
        updated_meta_data = ForecastMetaDataFrame.concat(updated_meta_datas, drop_dups = True)

        # In the DATA calculations: create new total line
        new_totals_values = updated_model[list_of_last_value_ids].sum(axis = 1)

        # In the META-DATA calculations:  create a STEP_INIT
        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = new_totals_step_init_id,
                                                                     display_name = new_display_name,
                                                                     data_values = None,
                                                                     step_type = ForecastDataSeriesMetaDataStepTypes.SUMMATION,
                                                                     action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                     data_type = updated_meta_datas[0].get_last_data_type(),
                                                                     display_type = updated_meta_datas[0].get_last_display_type(),
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     update_last_id = True)
        
        # In the META-DATA calculations:  create new total line
        (updated_model, updated_meta_data) = self._add_col_data_meta(updated_model,
                                                                     updated_meta_data,
                                                                     id = new_totals_id,
                                                                     display_name = new_display_name,
                                                                     data_values = new_totals_values.to_list(),   # this is a DataFrame, so don't need to convert to
                                                                     step_type = ForecastDataSeriesMetaDataStepTypes.SUMMATION,
                                                                     action = ForecastDataSeriesMetaDataAction.TOTAL,
                                                                     data_type = updated_meta_datas[0].get_last_data_type(),
                                                                     display_type = updated_meta_datas[0].get_last_display_type(),
                                                                     validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                     update_last_id = True,
                                                                     pred = list_of_last_value_ids,
                                                                     args = None,
                                                                     objs = None)

        # return values
        return(updated_model, updated_meta_data, new_totals_id, new_display_name)




    # ================
    # HELPER FUNCTIONS
    # ================

