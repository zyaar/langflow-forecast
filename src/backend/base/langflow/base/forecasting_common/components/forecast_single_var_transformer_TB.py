#####################################################################
# forecast_single_var_transformer_TB.py
#
# Abstract class to implement a component which takes one or more
# forecasts, and applies a math transform on then and a single variable
# defined in the component.  Used to very simply implement things like:
# population cut, pricing, etc.
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

# ForecastSingleVarTransformerTB
# Abstract class to implement a component which takes one or more
# forecasts, and applies a math transform on then and a single variable
# defined in the component.  Used to very simply implement things like:
# population cut, pricing, etc.
class ForecastSingleVarTransformerTB(ForecastComponent):

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





    # COMPONENT INPUTS
    # ----------------
    inputs = []

    # COMPONENT OUTPUTS
    # -----------------
    outputs = []



    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    form_trigger_rules = [
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, (f"var_table", "generate_var_table_values", ["var_table"])),
    ]


    # INSTANCE ATTRIBUTES
    # generated during the __init__
    # -----------------------------

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
    # --------
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

        # set-up inputs and outputs with the child class's configuration variables
        self.inputs = self.gen_inputs()
        self.outputs = self.gen_outputs()

        super().__init__(**kwargs)
    

    # GENERATE INPUTS / OUTPUTS
    # -------------------------
    def gen_inputs(self) -> list:
        inputs_list = [
            # common forecast inputs
            *ForecastComponent.inputs,

            # dataframes in List[DataFrame]
            DataInput(
                name=f"forecasts_in",
                display_name=f"{self.VAR_IN_DISPLAY_NAME}",
                info=f"{self.VAR_IN_INFO}",
                dynamic=True,
                real_time_refresh=True,
                is_list = True,
            ),

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
    

    def gen_outputs(self) -> list:
        outputs_list = [
            Output(display_name=f"{self.VAR_OUT_DISPLAY_NAME}", info = f"{self.VAR_OUT_INFO}", name=f"var_out", method=f"calc_var_out", hidden=f"{self.VAR_OUT_HIDDEN}"),        
        ]

        if (self.VAR_REMAINDER_OUTPUT):
            outputs_list.append(
                Output(display_name=f"{self.VAR_OUT_REMAINDER_DISPLAY_NAME}", name=f"var_remainder_out", method=f"calc_var_remainder_out", hidden=f"{self.VAR_REMAINDER_OUT_HIDDEN}"),
            )
        
        return(outputs_list)



    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing INPUTS fields whenever an update happens from a dynamic field
    # -------------------
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
    
    
    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        pass


    # OUTPUT FUNCTIONS
    # ----------------

    # calc_var_common
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

    def calc_var_common(self) -> tuple[DataFrame, ForecastMetaDataFrame, str, pd.Series, pd.Series, pd.Series]:
        self.validate_inputs()

        # sum up all the inputs to create a single total line and add it to the output model
        (updated_model, updated_meta_data) = self.check_and_combine_forecasts(totals_id = f"{self._id}_Total_In", 
                                                                              totals_display_name = f"{self.VAR_IN_DISPLAY_NAME}", 
                                                                              step_type = self.VAR_STEP_TYPE)                                                                                                                                                                                    
                                                                                                                                                                                            # get the id for the curr_totals row (the total patients coming into the segment component, whether it was just generated above or not), 
        # we may or may not have generated a totals column (if there was only one input, we don't, if there was >1 we do), so grab the
        # last ID in the updated_model so that we are pointing to the right totals column (new one or not)
        col_total_in_id = updated_model.columns[-1]
        col_total_in_values = updated_model[col_total_in_id]

        # get the var table data and make sure it's data types are set correctly (date fields and float fields)
        var_table = ForecastDataModel.astype_first_all_cols(self.var_table)

        # get var input col values
        col_var_values = var_table[self.var_col_name]
        (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = self.var_col_input_id,
                                                                    display_name = f"{self.VAR_TABLE_COL_DISPLAY_NAME}",
                                                                    data_values = col_var_values,
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
            (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
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
        (updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values) = self.calc_var_common()

        # calculate the var action values (which is the product of total_in and var percent values)
        col_var_action_values = col_total_in_values * col_var_values

        (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = self.var_col_calc_id,
                                                                    display_name = f"{self.display_name}",
                                                                    data_values = col_var_action_values,
                                                                    step_type = self.VAR_STEP_TYPE,
                                                                    action = ForecastDataSeriesMetaDataAction.PROD,
                                                                    data_type = self.VAR_OUT_TYPE,
                                                                    display_type = self.VAR_OUT_DISPLAY_TYPE,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [col_total_in_id, self.var_col_input_id],
                                                                    args = None,
                                                                    objs = None)
        
        # bundle the packet together for forwarding to next component(s)
        data_packet = self.gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data)
        return(data_packet)    



    
    # calc_var_remainder_out
    # Calcuate the remainder action out (i.e. product of total_in and (1 - var percent values))
    #
    # INPUTS:
    #   NA
    #
    # OUTPUTS:
    #   data_packet - Data with dataframe and meta-data

    def calc_var_remainder_out(self) -> Data:
        (updated_model, updated_meta_data, col_total_in_id, col_total_in_values, col_var_values, col_var_remainder_values) = self.calc_var_common()

        # calculate the var action values (which is the product of total_in and percentage of remainder)
        col_var_remainder_action_values = col_total_in_values * col_var_remainder_values

        (updated_model, updated_meta_data) = self.add_col_data_meta(updated_model,
                                                                    updated_meta_data,
                                                                    id = self.var_col_remainder_calc_id,
                                                                    display_name = f"{self.display_name}",
                                                                    data_values = col_var_remainder_action_values,
                                                                    step_type = self.VAR_STEP_TYPE,
                                                                    action = ForecastDataSeriesMetaDataAction.PROD,
                                                                    data_type = self.VAR_OUT_TYPE,
                                                                    display_type = self.VAR_OUT_DISPLAY_TYPE,
                                                                    validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],
                                                                    pred = [col_total_in_id, self.var_col_remainder_pct_id],
                                                                    args = None,
                                                                    objs = None)
        

        
        # bundle the packet together for forwarding to next component(s)
        data_packet = self.gen_data_packet(dataframe = updated_model, meta_data = updated_meta_data)

        return(data_packet)    



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
            new_df = ForecastFormModelUtilities.refill_drataframe(new_dim_rows=num_rows, new_dim_cols=2, prev_data=DataFrame(old_values), col_name_prefix="", dates=dates)
            return new_df.to_data_list()
