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
                                                                        ForecastMetaDataRangeSchema)

# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List, Tuple, Dict
import copy
import pandas as pd

# CLASSES
# =======
class ForecastController():

    # ================
    # HELPER FUNCTIONS
    # ================

    # add_col_data_meta
    # Handles the addition of a action to both the DataFrame and the ForecastMetaDataFrame
    # NOTE:  Due to desire for strict typing, this takes explicity arguments that go into
    #        generating a new ForecastMetaDataSeries.  This means that if ForecastMetaDataSeriesSchema
    #        is updated, it will need to be reflected here.
    # 
    # INPUTS:
    #   dataframe = existing dataframe to update
    #   meta_data = existing ForecastMetaDataFrame to update
    #   id = unique id for the new action column to be added
    #   display_name = user friendly name for the action column
    #   data_values = the data values to add to the dataframe (as a pandas Series)
    #   [next several arguments are all taken from the Schema definition for ForecastMetaDataSeries, see forecast_meta_data.py for more information]
    #   vertify_integrity (optional) = (default: True) set True if you want the function to raise an error if the id being added already exists in the dataframe/meta_data
    #   drop_dups (optional) = (default: False) set True if you want the function to automatically discard any a new column if it's id already exists in the dataframe / meta_data, if this is set, vertify_integrity setting is ignored 
    #   
    # OUTPUTS:
    #   DataFrame = updated DataFrame with the new action column
    #   ForecastMetaDataFrame = updated ForecastMetaDataFrame with the new action column

    @staticmethod
    def _add_col_data_meta(dataframe: DataFrame | pd.DataFrame,
                           meta_data: ForecastMetaDataFrame,
                           id: str,
                           display_name: str,
                           step_type: ForecastDataSeriesMetaDataStepTypes,
                           action: ForecastDataSeriesMetaDataAction,
                           data_type: ForecastDataSeriesMetaDataDataType,
                           display_type: ForecastDataSeriesMetaDataDataType,
                           validation: List[Dict[ForecastDataSeriesMetaDataValidationSchema, Any]],
                           data_values: pd.Series | list = None,
                           pred: List[str | int | float] = None,
                           args: Dict = None,
                           objs: List = None,
                           update_last_id = False,                           
                           verify_integrity: bool = True,
                           drop_dups: bool = False) -> tuple[DataFrame, ForecastMetaDataFrame]:
          
          # make sure data_values is a list (empty or not)
          if(data_values is None):
              data_values = []
          elif(not isinstance(data_values, list)):
              data_values = data_values.to_list()


          # create a data values holder for meta_data
          if(len(data_values) == 0):
              data_values_meta_data = []
          else:
              data_values_meta_data = data_values
          
          # add col to meta_data
          new_meta_col = ForecastMetaDataSeries(id = id,
                                                step_type = step_type,
                                                action = action,
                                                data_type = data_type,
                                                display_type = display_type,
                                                display_name = display_name,
                                                data_values = data_values_meta_data,
                                                validation = validation,
                                                pred = pred,
                                                args = args,
                                                objs = objs)
          updated_meta_data = ForecastMetaDataFrame.concat([meta_data, new_meta_col], verify_integrity = verify_integrity, drop_dups = drop_dups)
          
          # add col to data
          updated_dataframe = ForecastDataModel.add_col_to_model(dataframe, data_values, new_col_name = id)

          # update last id
          if update_last_id:
              updated_meta_data.set_last_id(id = id)


          return(updated_dataframe, updated_meta_data)
