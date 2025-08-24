#####################################################################
# forecast_delay_TB.py
#
# Implements the a summation component.  It's already implemented everywhere
# this just makes it explicit (for visual presentation purposes)
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
from langflow.base.forecasting_common.components.forecast_component import ForecastComponent
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities

from langflow.base.forecasting_common.components.forecast_single_var_col_transformer_TB import ForecastSingleVarColTransformerTB

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
from typing import List
import pandas as pd
import numpy as np


# CLASSES
# =======

# ForecastDelayTB
# Adds all the input streams together and results a new row with a total
class ForecastDelayTB(ForecastSingleVarColTransformerTB, Component):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"Delay TB"
    description: str = f"Add a delay (in months) to any stream."
    icon: str = f"clock-arrow-up"
    name: str = f"DelayTB"

    # OUTPUT INFO
    VAR_OUT_DISPLAY_NAME = "Delayed stream"
    VAR_OUT_INFO = "Stream with delay applied"

    # ROW_SET VAR
    ROW_SET_DISPLAY_NAME = "Delay (in months)"
    ROW_SET_INFO = "How many months to delay the input stream."
    ROW_SET_DEFAULT = 0
    ROW_SET_MIN = 0
    ROW_SET_MAX = 120
    ROW_SET_STEP = 1

    # TABLE VARS
    TABLE_DISPLAY_NAME = "Delay Details"
    TABLE_INFO = "Table of delay details."
    COL_1_CONFIG = {"display_name": "Month", "description": "Months after treatment start"}
    COL_2_CONFIG = {"display_name": "Number of patients", "description": "Number of patients in this month.", "type": "int", "edit_mode": EditMode.INLINE}



    # GENERATE INPUTS / OUTPUTS
    # -------------------------
    # def gen_inputs(self) -> list:
    #     inputs_list = [
    #         *super().gen_inputs(),

    #         # additional inputs for this component
    #     ]

    #     return(inputs_list)
    

    # def gen_outputs(self) -> list:
    #     outputs_list = [
    #         *super().gen_outputs(),

    #         # additional outputs for this component
    #     ]

    #     return(outputs_list)


    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        super().validate_inputs()

        # additional validation for this component
        if(self.row_set_var < 1):
            raise ValueError(f"\n*  validate_inputs:  '{self.get_input_display_name("row_set_var")}' must be greater than zero.")

        # TODO:  make sure we aren't shifting more than the entire forecast length



    # OUTPUT FUNCTIONS
    # ================

    # _component_specific_calcs
    # this is where this class and all it's childer do their specific calculations
    # and return the updated model, meta-data and the id of the output column
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   (updated_model, updated_meta_data, col_total_in_id) - the updated model, meta-data and the id of the output column
    
    def _component_specific_calcs(self, 
                                  updated_model: DataFrame, 
                                  updated_meta_data: ForecastMetaDataFrame, 
                                  last_col_id: str) -> tuple[pd.DataFrame, ForecastMetaDataFrame, str]:
        
        (updated_model, updated_meta_data, col_total_in_id) = super()._component_specific_calcs(updated_model = updated_model, 
                                                                                                updated_meta_data=updated_meta_data,
                                                                                                last_col_id = last_col_id)
        
        # add custom calculation code here
        # --------------------------------

        # add additional calculations for this component
        last_series = updated_meta_data.get_last_series()
        last_series_id = updated_meta_data.get_last_id()
        last_series_values = updated_model[last_series_id]
        data_type = last_series.get_data_type()
        display_type = last_series.get_display_type()


        # INIT
        # Add a treatment set-up instructions for a treatment section to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{self._id}_Init",
                                                                    display_name = self.display_name,
                                                                    data_values = None,
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.DELAY,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = data_type,
                                                                    display_type = display_type,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [last_series_id],
                                                                    update_last_id = True)
        
        
        # YEAR_TO_MONTH
        # if the forecast timescale is set to year, we need to convert the last data values to months, then shift
        if(self.timescale == ForecastModelTimescale.YEAR):
            last_series_values = ForecastDataModel.yearly_to_monthly(last_series_values)
            new_last_series_id = f"{self._id}_Yearly_to_Monthly"
            
            # Add a treatment set-up instructions for a treatment section to meta_data table
            updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                        id = new_last_series_id,
                                                                        display_name = f"Convert {self.display_name} from yearly to monthly",
                                                                        data_values = last_series_values,
                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.DELAY,
                                                                        action = ForecastDataSeriesMetaDataAction.YEAR_TO_MONTH,
                                                                        data_type = data_type,
                                                                        display_type = display_type,
                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                        pred = [last_series_id],
                                                                        update_last_id=True)
            last_series_id = new_last_series_id

        # DELAY
        # Add the delay action
        delayed_col_id = f"{self._id}_Delay"
        num_months_to_roll = int(self.row_set_var)
        table_inputs_df = DataFrame(self.table_inputs)
        fill_values = table_inputs_df["col_2"].values

        delayed_values = last_series_values.shift(periods = num_months_to_roll, fill_value = ForecastDataModel.EDITABLE_VALUES_TOKEN)
        delayed_values.loc[0:(num_months_to_roll-1)] = list(fill_values)

        # Add a treatment set-up instructions for a treatment section to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = delayed_col_id,
                                                                    display_name = f"{self.display_name}",
                                                                    data_values = delayed_values.to_list(),
                                                                    step_type = ForecastDataSeriesMetaDataStepTypes.DELAY,
                                                                    action = ForecastDataSeriesMetaDataAction.SHIFT,
                                                                    data_type = data_type,
                                                                    display_type = display_type,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [last_series_id],
                                                                    update_last_id = True,
                                                                    args = {ForecastDataSeriesMetaDataAction.SHIFT: num_months_to_roll},
                                                                    objs = {ForecastDataSeriesMetaDataAction.SHIFT: fill_values})
        
        # MONTH_TO_YEAR
        # if the forecast timescale is set to year, we need to convert the shifted values back to years
        if(self.timescale == ForecastModelTimescale.YEAR):
            new_last_series_id = f"{self._id}_Monthly_to_Yearly"
            new_last_series_values = ForecastDataModel.monthly_to_yearly(delayed_values)
             
            # Add a treatment set-up instructions for a treatment section to meta_data table
            updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                        id = new_last_series_id,
                                                                        display_name = f"Convert {self.display_name} from monthly to yearly",
                                                                        data_values = new_last_series_values.to_list(),
                                                                        step_type = ForecastDataSeriesMetaDataStepTypes.DELAY,
                                                                        action = ForecastDataSeriesMetaDataAction.MONTH_TO_YEAR,
                                                                        data_type = data_type,
                                                                        display_type = display_type,
                                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                        pred = [delayed_col_id],
                                                                        update_last_id=True)
            updated_model[new_last_series_id] = new_last_series_values
            last_col_id = new_last_series_id
        
        # otherwise, no changes needed, just update the model with the values
        else:
            updated_model[delayed_col_id] = delayed_values.to_list()
            last_col_id = delayed_col_id
        
        return(updated_model, updated_meta_data, last_col_id)
