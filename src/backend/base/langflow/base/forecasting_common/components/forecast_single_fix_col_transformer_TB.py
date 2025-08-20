#####################################################################
# forecast_single_fixed_col_transformer_TB.py
#
# Abstract class to implement a component which takes one or more
# forecasts, and applies a math transform on them the a single fixed
# length column of data entered in a table.
# 
# INPUTS:  Data (ForecastDataModel format)
# OUTPUTS:  Data (ForecastDataModel format)
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

# ForecastSingleFixedColTransformerTB
# Abstract class to implement a component which takes one or more
# forecasts, and applies a math transform on then and a single variable
# defined in the component.  Used to very simply implement things like:
# population cut, pricing, etc.
class ForecastSingleFixedColTransformerTB(ForecaseSumInputTB):

    # CONFIG CONSTANTS
    # ================

    # COMPONENT INFO
    display_name: str = f"DISPLAY_NAME"
    description: str = f"DESCRIPTION"
    icon: str = f"ICON"
    name: str = f"NAME"

    # VAR NAME
    VAR_NAME = "VAR_NAME"
    VAR_REMAINDER_NAME = "remainder"
    VAR_CALC_POSTFIX = "Total"

    # DATA TYPE
    VAR_IN_TYPE = ForecastDataSeriesMetaDataDataType.PCT
    VAR_IN_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.PCT

    VAR_OUT_TYPE = ForecastDataSeriesMetaDataDataType.FLOAT
    VAR_OUT_DISPLAY_TYPE = ForecastDataSeriesMetaDataDataType.INT

    #INPUT / OUTPUT INFO
    VAR_IN_DISPLAY_NAME = "VAR_IN_DISPLAY_NAME"
    VAR_IN_INFO = "VAR_IN_INFO"

    VAR_OUT_DISPLAY_NAME = "VAR_OUT_DISPLAY_NAME"
    VAR_OUT_INFO = "VAR_OUT_INFO"
    VAR_OUT_HIDDEN = False

    VAR_REMAINDER_OUTPUT = True
    VAR_OUT_REMAINDER_DISPLAY_NAME = "VAR_OUT_REMAINDER_DISPLAY_NAME"
    VAR_OUT_REMAINDER_INFO = "VAR_OUT_REMAINDER_INFO"
    VAR_REMAINDER_OUT_HIDDEN = False

    # INPUTTABLE INFO
    VAR_TABLE_DISPLAY_NAME = "VAR_TABLE_DISPLAY_NAME"
    VAR_TABLE_INFO = "VAR_TABLE_INFO"
    VAR_TABLE_COL_VAR_NAME_POSTFIX = "VAR_TABLE_COL_VAR_NAME_POSTFIX"
    VAR_TABLE_COL_DISPLAY_NAME = "VAR_TABLE_COL_DISPLAY_NAME"
    VAR_TABLE_COL_INFO = "VAR_TABLE_COL_INFO"
    VAR_TABLE_COL_DATA_TYPE = "float"


    # BUILDER INFO
    VAR_STEP_TYPE = ForecastDataSeriesMetaDataStepTypes.SEGMENT
    VAR_ACTION_FUNCT = ForecastDataSeriesMetaDataAction.PROD
    VAR_VALIDATION_FUNCTS = {ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}
    VAR_PRED = []
    VAR_ARGS = {}
    VAR_OBJS = {}


    # INSTANCE ATTRIBUTES
    # ===================

    # var_col_id - (str) the id for the var to be added to generate a bunch of ids (see below)
    # var_col_name - (str) ???
    # var_col_input_id - (str) based on the var_col_id the id for the INPUT column of var_col
    # var_col_calc_id - (str) based on the var_col_id the id for the CALCULATED (product) column of var_col
    #
    # var_col_remainder_id - (str) the id for the remainder (if used) to be added to generate a bunch of ids (see below)
    # var_col_remainder_pct_id - (str) ???
    # var_col_remainder_calc_id - (str) based on the var_col_remainder_id the id for the CALCULATED (Total - var_Total) column of var_col
    #
    # inputs - (list) InputTypes for the component
    # outputs - (list) OutputTypes for the component




    # __init__
    # ========
    def __init__(self, **kwargs) -> None:
        # generates some instance variables instead of using class variables, this allows us to customize
        # this instance variables in the children of this abstract class without having to rewrite all the

        # we need an id ahead of creating some of these instance variables, since the _id attributes gets set-up in the
        # somewhere up the parent chain of constructors, we need to run that code AHEAD of calling the parent's
        # constructor so that we can create theses instances
        # per: https://github.com/langflow-ai/langflow/blob/a0c00c015f4a4572cf15e195bb8e6bec37ce72f2/src/backend/base/langflow/custom/custom_component/component.py
        # the way to do this is to generate the _id value, and then add it as a kwarg into the __init__ for the parent where it gets added to the __config attribute for the instance

        # check if the __init__ is already
        if hasattr(self, "_id"):
            has_self_id = True
            has_kwarg_id = False
            id = self._id
        elif "_id" in kwargs:
            has_self_id = False
            has_kwarg_id = True
            id = kwargs["_id"]
        else:
            has_self_id = False
            has_kwarg_id = False
            id = f"{self.__class__.__name__}-{nanoid.generate(size=5)}"
            kwargs["_id"] = id            


        # class variables

        # generate column id for var (since it gets reused in a bunch of places)
        self.var_col_id = f"{id}_{self.VAR_NAME}"
        self.var_col_name = f"{self.VAR_NAME}"
        self.var_col_input_id = f"{self.var_col_id}_{self.VAR_TABLE_COL_VAR_NAME_POSTFIX}"
        self.var_col_calc_id = f"{self.var_col_id}_{self.VAR_CALC_POSTFIX}"

        self.var_col_remainder_id = f"{id}_{self.VAR_REMAINDER_NAME}"
        self.var_col_remainder_pct_id = f"{self.var_col_remainder_id}_Percent"
        self.var_col_remainder_calc_id = f"{self.var_col_remainder_id}_{self.VAR_CALC_POSTFIX}"

        super().__init__(**kwargs)




    # GENERATE INPUTS / OUTPUTS
    # -========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            *super()._gen_inputs(),

            # var_table
            TableInput(
                name=f"var_table",
                display_name=f"{self.VAR_TABLE_DISPLAY_NAME}",
                info=f"{self.VAR_TABLE_INFO}",
                required=True,
                show=True,
                dynamic=True,
                real_time_refresh=True,
                table_schema=[
                    {
                        "name": ForecastDataModel.RESERVED_COLUMN_INDEX_NAME,
                        "display_name": "Date",
                        "type": "date",
                        "description": f"End date of {self.VAR_TABLE_DISPLAY_NAME}",
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": True,
                    },
                    {
                        "name": f"{self.var_col_name}",
                        "display_name": f"{self.VAR_TABLE_COL_DISPLAY_NAME}",
                        "type": f"{self.VAR_TABLE_COL_DATA_TYPE}",
                        "description": f"{self.VAR_TABLE_COL_INFO}",
                        "edit_mode": EditMode.INLINE,
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

        # if it's selected, we also add an output to show remainders
        if (self.VAR_REMAINDER_OUTPUT):
            outputs_list.append(
                Output(display_name=f"{self.VAR_OUT_REMAINDER_DISPLAY_NAME}", name=f"var_remainder_out", method=f"calc_var_remainder_out", hidden=f"{self.VAR_REMAINDER_OUT_HIDDEN}"),
            )
        
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
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, (f"var_table", "generate_var_table_values", ["var_table"])),
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
                                                               generate_var_table_values=self.generate_var_table_values)

        # return updated config         
        return(build_config)
    
    


    # OUTPUT FUNCTIONS
    # ================

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

    def _forecast_model_common_input(self) -> tuple[DataFrame, ForecastMetaDataFrame, str, pd.Series, pd.Series, pd.Series]:
        (updated_model, updated_meta_data, col_total_in_id) = super()._forecast_model_common_input()

        # Add a step set-up instructions to meta_data table
        updated_meta_data = ForecastMetaDataFrame.add_col_meta_data(frame = updated_meta_data,
                                                                    id = f"{self._id}_Init",
                                                                    display_name = self.display_name,
                                                                    data_values = None,
                                                                    step_type = self.VAR_STEP_TYPE,
                                                                    action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                                    data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [col_total_in_id])

        # get the values of the totals_in column
        # col_total_in_id = updated_model.columns[-1]
        col_total_in_values = updated_model[col_total_in_id]

        # get the var table data and make sure it's data types are set correctly (date fields and float fields)
        var_table = ForecastDataModel.astype_first_all_cols(self.var_table)

        # get var input col values
        col_var_values = var_table[self.var_col_name]
        (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                                    updated_meta_data,
                                                                                    id = self.var_col_input_id,
                                                                                    display_name = f"{self.VAR_TABLE_COL_DISPLAY_NAME}",
                                                                                    data_values = col_var_values.to_list(),   # this is a DataFrame, so don't need to convert to
                                                                                    step_type = self.VAR_STEP_TYPE,
                                                                                    action = ForecastDataSeriesMetaDataAction.INPUT,
                                                                                    data_type = self.VAR_IN_TYPE,
                                                                                    display_type = self.VAR_IN_DISPLAY_TYPE,
                                                                                    validation = self.VAR_VALIDATION_FUNCTS,
                                                                                    pred = self.VAR_PRED,
                                                                                    args = self.VAR_ARGS,
                                                                                    objs = self.VAR_OBJS)
        
        # calcuate remainder percent col values
        if(self.VAR_REMAINDER_OUTPUT):         
            # calculate the percent of the remainder
            col_var_remainder_values = 1 - col_var_values
            (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                                      updated_meta_data,
                                                                                      id = self.var_col_remainder_pct_id,
                                                                                      display_name = f"{self.VAR_OUT_REMAINDER_DISPLAY_NAME}",
                                                                                      data_values = col_var_remainder_values,
                                                                                      step_type = self.VAR_STEP_TYPE,
                                                                                      action = ForecastDataSeriesMetaDataAction.SUB,
                                                                                      data_type = self.VAR_IN_TYPE,
                                                                                      display_type = self.VAR_IN_DISPLAY_TYPE,
                                                                                      validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                      pred = [1, self.var_col_input_id],
                                                                                      args = None,
                                                                                      objs = None)
            
        else:
            col_var_remainder_values = None
            
        # return everything
        return(updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values)



    # calc_var_out
    # Calcuate the variable action out (i.e. total_in * variable)
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_out(self) -> Data:
        # (updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values) = self.calc_var_common()
        (updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values) = self._forecast_model_common_input()

        # component specific remainder calculations
        (updated_model, updated_meta_data, col_total_in_id) = self._component_specific_calcs(updated_model, 
                                                                                             updated_meta_data, 
                                                                                             col_total_in_id, 
                                                                                             col_total_in_values, 
                                                                                             col_var_values, 
                                                                                             col_var_remainder_values)

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, updated_meta_data, col_total_in_id)



    
    # calc_var_remainder_out
    # Calcuate the remainder action out (i.e. product of total_in and (1 - var percent values))
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_remainder_out(self) -> Data:
        # (updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values) = self.calc_var_common()
        (updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values) = self._forecast_model_common_input()

        # component specific remainder calculations
        (updated_model, updated_meta_data, col_total_in_id) = self._component_specific_remainder_calcs(updated_model, 
                                                                                                       updated_meta_data, 
                                                                                                       col_total_in_id, 
                                                                                                       col_total_in_values, 
                                                                                                       col_var_values, 
                                                                                                       col_var_remainder_values)

        return self._forecast_model_common_output(updated_model, updated_meta_data, col_total_in_id)



    # HELPER FUNCTIONS
    # ----------------

    # generate_var_table_values
    # Generate the default values for the table (dates and ForecastDataModel.EDITABLE_VALUES_TOKEN for the var)
    # 
    # INPUTS:
    # OUTPUTS:
    #   List of dictionaries / one dictionary per row, looking like this:
    def generate_var_table_values(self, field_value: str, field_name: str) -> List[dict]:

        # get the current values in the var_table
        old_values = self.var_table

        # generate the dates needed (we'll need this regardless of whether we have old values or not)
        dates = ForecastDataModel.gen_forecast_dates(start_year = int(self.start_year),
                                                     start_month = int(self.start_month),
                                                     num_years = int(self.num_years),
                                                     timescale = ForecastModelTimescale(self.timescale))
        num_rows = len(dates)

        # if there are no old values, generate a brand list of dicts for the table
        if(old_values is None or not old_values):
            return [{ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: dates[i], f"{self.var_col_name}": ForecastDataModel.EDITABLE_VALUES_TOKEN} for i in range(num_rows)]
        
        # otherwise, resize the exist values into the new size (note:  always add the dates in)
        else:
            new_df = ForecastFormModelUtilities.refill_dataframe(new_dim_rows=num_rows, new_dim_cols=2, prev_data=DataFrame(old_values), col_name_prefix="", dates=dates)
            return new_df.to_data_list()
        




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
                                  updated_model: DataFrame | pd.DataFrame, 
                                  updated_meta_data: ForecastMetaDataFrame, 
                                  col_total_in_id: str,
                                  col_total_in_values: pd.Series, 
                                  col_var_values: pd.Series, 
                                  col_var_remainder_values: pd.Series) -> tuple[DataFrame | pd.DataFrame, ForecastMetaDataFrame, str]:
        
        # calculate the var action values (which is the product of total_in and var percent values)
        col_var_action_values = col_total_in_values * col_var_values

        (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                                  updated_meta_data,
                                                                                  id = self.var_col_calc_id,
                                                                                  display_name = f"{self.display_name}",
                                                                                  data_values = col_var_action_values.to_list(),
                                                                                  step_type = self.VAR_STEP_TYPE,
                                                                                  action = ForecastDataSeriesMetaDataAction.PROD,
                                                                                  data_type = self.VAR_OUT_TYPE,
                                                                                  display_type = self.VAR_OUT_DISPLAY_TYPE,
                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                  pred = [col_total_in_id, self.var_col_input_id],
                                                                                  args = None,
                                                                                  objs = None)
        
        return(updated_model, updated_meta_data, self.var_col_calc_id)
    


    # _component_specific_remainder_calcs
    # this is where this class and all it's childer do their specific REMAINDER calculations
    # and return the updated model, meta-data and the id of the output column
    # INPUTS:
    #   NA
    # OUTPUTS:
    #   (updated_model, updated_meta_data, col_total_in_id) - the updated model, meta-data and the id of the output column
    
    def _component_specific_remainder_calcs(self, 
                                            updated_model: DataFrame | pd.DataFrame, 
                                            updated_meta_data: ForecastMetaDataFrame, 
                                            col_total_in_id: str,
                                            col_total_in_values: pd.Series, 
                                            col_var_values: pd.Series, 
                                            col_var_remainder_values: pd.Series) -> tuple[DataFrame | pd.DataFrame, ForecastMetaDataFrame, str]:
        
        # calculate the var action values (which is the product of total_in and var percent values)
        col_var_action_values = col_total_in_values * col_var_remainder_values

        (updated_model, updated_meta_data) = ForecastComponent._add_col_data_meta(updated_model,
                                                                                  updated_meta_data,
                                                                                  id = self.var_col_remainder_calc_id,
                                                                                  display_name = f"{self.display_name}",
                                                                                  data_values = col_var_action_values.to_list(),
                                                                                  step_type = self.VAR_STEP_TYPE,
                                                                                  action = ForecastDataSeriesMetaDataAction.PROD,
                                                                                  data_type = self.VAR_OUT_TYPE,
                                                                                  display_type = self.VAR_OUT_DISPLAY_TYPE,
                                                                                  validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                                  pred = [col_total_in_id, self.var_col_remainder_pct_id],
                                                                                  args = None,
                                                                                  objs = None)
        
        return(updated_model, updated_meta_data, self.var_col_remainder_calc_id)
    


    # Children MUST PROVIDES
    # ======================
    # Component:  display_name, description, icon, name, 
    # Name:  VAR_NAME, VAR_REMAINDER_NAME, VAR_CALC_POSTFIX
    # Data Type:  VAR_IN_TYPE, VAR_IN_DISPLAY_TYPE, VAR_OUT_TYPE, VAR_OUT_DISPLAY_TYPE
    # Input / Output:  VAR_IN_DISPLAY_NAME, VAR_IN_INFO, VAR_OUT_DISPLAY_NAME, VAR_OUT_INFO, VAR_OUT_HIDDEN, VAR_REMAINDER_OUTPUT, VAR_OUT_REMAINDER_DISPLAY_NAME, VAR_OUT_REMAINDER_INFO, VAR_REMAINDER_OUT_HIDDEN
    # Inputtable:  VAR_TABLE_DISPLAY_NAME, VAR_TABLE_INFO, VAR_TABLE_COL_VAR_NAME_POSTFIX, VAR_TABLE_COL_DISPLAY_NAME, VAR_TABLE_COL_INFO, VAR_TABLE_COL_DATA_TYPE
    # Builder:  VAR_STEP_TYPE, VAR_ACTION_FUNCT, VAR_VALIDATION_FUNCT


    # Functions:
    #     _validate_inputs() - if validation is required
    #     _validate_outputs - if validation is required

    #     _forecast_model_common_input() - UPDATE if additional steps are needed
    #     _forecast_model_common_output() - UPDATE if additional steps are needed

    #     _component_specific_calcs() - specific transformation done in this component as part of the output step
    #     _component_specific_remainder_calcs() - specific transformation done in this component to generated the remainder step

