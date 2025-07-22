#####################################################################
# forecast_single_var_col_transformer_TB.py
#
# Abstract class to implement a component which takes one or more
# forecasts, and applies a math transform on them the a single variable
# length column of data entered in a table.
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

from langflow.base.forecasting_common.components.forecast_sum_input_TB import ForecaseSumInputTB

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
import nanoid


# CLASSES
# =======

# ForecastDelayTB
# Adds all the input streams together and results a new row with a total
class ForecastSingleVarColTransformerTB(ForecaseSumInputTB):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"DISPLAY_NAME"
    description: str = f"DESCRIPTION"
    icon: str = f"ICON"
    name: str = f"NAME"

    # OUTPUT INFO
    VAR_OUT_DISPLAY_NAME = "VAR_OUT_DISPLAY_NAME"
    VAR_OUT_INFO = "VAR_OUT_INFO"
    VAR_OUT_HIDDEN = False

    # ROW_SET VAR
    ROW_SET_DISPLAY_NAME = "ROW_SET_DISPLAY_NAME"
    ROW_SET_INFO = "ROW_SET_INFO"
    ROW_SET_DEFAULT = 0
    ROW_SET_MIN = 0
    ROW_SET_MAX = 120
    ROW_SET_STEP = 1

    # TABLE VARS
    TABLE_DISPLAY_NAME = "TABLE_DISPLAY_NAME"
    TABLE_INFO = "TABLE_INFO"
    COL_1_CONFIG = {"display_name": "Month", "description": "Months after treatment start"}
    COL_2_CONFIG = {"display_name": "Number of patients", "description": "Number of patients in this month.", "type": "int", "edit_mode": EditMode.INLINE}



    # GENERATE INPUTS / OUTPUTS
    # =========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            *super()._gen_inputs(),
            
            # Variable which controls the number of rows in the table
            IntInput(
                name = "row_set_var",
                display_name = self.ROW_SET_DISPLAY_NAME,
                info = self.ROW_SET_INFO,
                value = self.ROW_SET_DEFAULT,
                dynamic = True,
                real_time_refresh = True,
                show = True,
                required = True,
                range_spec = RangeSpec(min = self.ROW_SET_MIN, max = self.ROW_SET_MAX, step = self.ROW_SET_STEP),
            ),

            # Table to enter values for each row that component does "something" with
            TableInput(
                name="table_inputs",
                display_name=self.TABLE_DISPLAY_NAME,
                info=self.TABLE_INFO,
                required=True,
                show=True,
                dynamic=True,
                real_time_refresh=True,
                table_schema=[
                    {
                        "name": "col_1",
                        "display_name": self.COL_1_CONFIG["display_name"],
                        "type": "int",
                        "description": self.COL_1_CONFIG["description"],
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": True,
                    },
                    {
                        "name": "col_2",
                        "display_name": self.COL_2_CONFIG["display_name"],
                        "type": self.COL_2_CONFIG.get("type"),
                        "description": self.COL_2_CONFIG["description"],
                        "edit_mode": self.COL_2_CONFIG.get("edit_mode"),
                        "disable_edit": False,
                    },
                ],
                value=[],
            ),
        ]

        return(inputs_list)
    

    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),
            Output(display_name=f"{self.VAR_OUT_DISPLAY_NAME}", info = f"{self.VAR_OUT_INFO}", name=f"var_out", method=f"calc_var_out", hidden=f"{self.VAR_OUT_HIDDEN}"),
        ]

        return(outputs_list)


    # INPUT/OUTPUT VALIDATIONS
    # ========================
    # def validate_inputs(self):
    #     super().validate_inputs()

    # def validate_outputs(self):
    #     super().validate_outputs()


    # FORM UPDATE RULES
    # =================
    form_update_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("table_inputs", "generate_table_values", ["row_set_var"])),
    ]

    def update_build_config(self, build_config, field_value, field_name = None):
        # update the fields in the form to show/hide, based on the field updated
        forecastFormUpdater = ForecastFormUpdater()
        build_config = forecastFormUpdater.forecast_update_fields(build_config, 
                                                                  self.form_update_rules,
                                                                  field_value = field_value,
                                                                  field_name = field_name,
                                                                  only_shown_fields=True)
        
        # update the calculated values of fields in the form based on the field updated        
        forecastFormTriggerCalc = ForecastFormTriggerCalc()
        build_config = forecastFormTriggerCalc.execute_trigger(build_config=build_config,
                                                               form_trigger_rules=self.form_trigger_rules,
                                                               field_value=field_value,
                                                               field_name=field_name,
                                                               
                                                               # list of all the updater functions for calculated fields
                                                                generate_table_values=self.generate_table_values)


        # return updated config         
        return(build_config)





    # OUTPUT FUNCTIONS
    # ================

    # calc_var_out
    # run the calss specific calcs, then bundle the results into a data packet and return it
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_out(self) -> Data:
        # call common functions
        (updated_model, updated_meta_data, col_total_in_id) = self._forecast_model_common_input()

        # calculate component specific stuff
        (updated_model, updated_meta_data, col_total_in_id) = self._component_specific_calcs(updated_model = updated_model, 
                                                                                             updated_meta_data = updated_meta_data, 
                                                                                             last_col_id = col_total_in_id)
        
        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, col_total_in_id)


    # OUTPUT HELPERS
    # ==============

    # generate_table_values
    # Based on the latest schema, generates the values for the table
    # 
    # INPUTS:
    #   build_config
    #   field_value
    #   field_name
    #
    # OUTPUTS:
    #   build_config

    def generate_table_values(self, field_value: str, field_name: str) -> List[dict]:
        
        # determine how many rows we need
        if(field_name == "row_set_var"):
            num_rows = int(field_value)
        else:
            num_rows = self.row_set_var

        # Check if we have existing data
        old_values = self.table_inputs
        if(old_values is not None and isinstance(old_values, list) and len(old_values) > 0):
            new_df = ForecastFormModelUtilities.fill_drataframe(new_dim_rows =num_rows,
                                                                new_dim_cols = 2,
                                                                prev_data  = old_values, 
                                                                default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                                col_name_prefix = None, 
                                                                num_static_cols = 2, 
                                                                col_1 = list(range(1, num_rows+1)))
        else:
            new_df = ForecastFormModelUtilities.fill_drataframe(new_dim_rows = num_rows,
                                                                new_dim_cols = 2,
                                                                set_col_names = ["col_1", "col_2"],  
                                                                default_col_value = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                                                                col_name_prefix = None, 
                                                                num_static_cols = 2, 
                                                                col_1 = list(range(1, num_rows+1)))
        
        return(new_df.to_data_list())
    



    # TEMPLATE FUNCTIONS
    # ==================

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
        
        # add custom calculation code here

        return(updated_model, updated_meta_data, last_col_id)
    


    # Children MUST PROVIDES
    # ======================
    # Component:  display_name, description, icon, name
    # Output:  VAR_OUT_DISPLAY_NAME, VAR_OUT_INFO
    # Row Set Var (sets number of rows):  ROW_SET_DISPLAY_NAME, ROW_SET_INFO, ROW_SET_DEFAULT, ROW_SET_MIN, ROW_SET_MAX, ROW_SET_STEP
    # Table:  TABLE_DISPLAY_NAME, TABLE_INFO, COL_1_CONFIG, COL_2_CONFIG
    #
    # Functions:
    #   _validate_inputs() - if validation is required
    #   _validate_outputs - if validation is required
    #
    #   _forecast_model_common_input() - UPDATE if additional steps are needed
    #   _forecast_model_common_output() - UPDATE if additional steps are needed
    #
    #   _component_specific_calcs() - specific transformation done in this component as part of the output step
