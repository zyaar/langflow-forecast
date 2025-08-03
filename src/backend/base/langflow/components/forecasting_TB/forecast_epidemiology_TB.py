#####################################################################
# forecast_epidemiology_TB.py
#
# Implements the segment component of the forecasting in a TIME BASED model.
# The segment component applies one timescale based count of patients as a new line
# in the model
# 
# INPUTS:  DataFrame (ForecastDataModel format)
# OUTPUTS:  DataFrame (ForecastDataModel format)
#
#####################################################################

from langflow.custom import Component
from langflow.io import TableInput, IntInput, StrInput
from langflow.schema import DataFrame, Data
from langflow.schema.table import EditMode
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.components.forecast_component import ForecastComponent
from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
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
import copy


# CONSTANTS
# =========
FORECAST_EPIDEMIOLOGY_DATES_LABEL = "Dates (end-of)"


# CLASSES
# =======

# ForecastEpidemiologyTB
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastEpidemiologyTB(ForecastComponent):

    # COMPONENT META-DATA
    # ===================
    display_name: str = "Epidemiology TB"
    description: str = "Build an epidemiology stream of patients using a TIME BASED model."
    icon = "Globe"
    name: str = "EpidemiologyTB"



    # GENERATE INPUTS / OUTPUTS
    # =========================
    def _gen_inputs(self) -> list:
        inputs_list = [
            *super()._gen_inputs(),

            # patient_count
            TableInput(
                name="patient_count",
                display_name="Patient Count",
                info="Total patients at each time period based on epidemiological data",
                required=False,
                show=True,
                dynamic=True,
                real_time_refresh=True,
                refresh_button=True,
                table_schema=[
                    {
                        "name": "dates",
                        "display_name": "Date",
                        "type": "date",
                        "description": "Date of patient count",
                        "edit_mode": EditMode.INLINE,
                        "disable_edit": True,
                    },
                    {
                        "name": "patient_counts",
                        "display_name": "Patient Counts",
                        "type": "int",
                        "description": "Patient count",
                        "edit_mode": EditMode.INLINE,
                    },
                ],
                value=[],
            ),
        ]

        return(inputs_list)
    

    def _gen_outputs(self) -> list:
        outputs_list = [
            *super()._gen_outputs(),

            # additional outputs for this component
            Output(display_name="Epidemiology Patient Flow", name="epi_forecast_model", method="update_forecast_model"),
        ]

        return(outputs_list)




    # COMPONENT FORM UPDATE RULES
    # ===========================
    form_update_rules = {}
    form_trigger_rules = [
        #(ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("generate_table_values", ["patient_count"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("patient_count", "generate_table_values", ["patient_count"])),
    ]


    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing fields whenever an update happens from a dynamic field
    # ===================
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
        build_config = forecastFormTriggerCalc.execute_trigger(build_config,
                                                               self.form_trigger_rules,
                                                               field_value=field_value,
                                                               field_name=field_name,
                                                               
                                                               generate_table_values=self.generate_table_values)
        
        # return updated config         
        return(build_config)
    


    # INPUT VALIDATION
    # ================
    def validate_inputs(self):
        super().validate_inputs()

        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        # patient_count
        if(self.patient_count is None or not isinstance(self.patient_count, list) or len(self.patient_count) < 1):
            msg += f"\n*  validate_inputs:  Missing values for '{self.get_input_display_name("patient_count")}'."
                    

        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # ====================
        
    # generate_forecast_model
    # Output function epi_forecast_model end-point
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model(self) -> Data:
        self._forecast_model_common_input()

        input_col_id = f"{self._id}_Input"

        # generate the dataframe
        updated_model = DataFrame(self.patient_count).rename(columns={"patient_counts": input_col_id})

        # NOTE:  Since EPI is the origination of a forecast, we need to add a lot of meta-data here, specifically:
        # create the meta-dataframe, create the dates line, and the epi line

        # generate the meta-dataframe
        meta_data = ForecastMetaDataFrame(input_type = ForecastModelInputTypes(self.input_type),
                                          timescale = ForecastModelTimescale(self.timescale),
                                          start_year = int(self.start_year),
                                          start_month = int(self.start_month),
                                          num_periods = int(len(updated_model)),)
        
        # generate the meta data instructions for the dates line
        meta_data_series_dates = ForecastMetaDataSeries(id = ForecastDataModel.RESERVED_COLUMN_INDEX_NAME,
                                                        step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                                        action = ForecastDataSeriesMetaDataAction.DATES,
                                                        data_type = ForecastDataSeriesMetaDataDataType.DATE,
                                                        display_type = ForecastDataSeriesMetaDataDataType.DATE,
                                                        display_name = FORECAST_EPIDEMIOLOGY_DATES_LABEL,
                                                        data_values = updated_model[ForecastDataModel.RESERVED_COLUMN_INDEX_NAME].to_list(),
                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],)
        
        # Add a step set-up instructions for a treatment section to meta_data table
        meta_data_series_step_init = ForecastMetaDataSeries(id = f"{self._id}_Init",
                                                        step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                                        action = ForecastDataSeriesMetaDataAction.STEP_INIT,
                                                        data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                        display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                        display_name = self.display_name,
                                                        validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],)  
        
        # generate the meta data instructions for the epi line
        meta_data_series_epi = ForecastMetaDataSeries(id = input_col_id,
                                                      step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY,
                                                      action = ForecastDataSeriesMetaDataAction.INPUT,
                                                      data_type = ForecastDataSeriesMetaDataDataType.INT,
                                                      display_type = ForecastDataSeriesMetaDataDataType.INT,
                                                      display_name = self.display_name,
                                                      data_values = updated_model[input_col_id].to_list(),
                                                      validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.TOKEN_CHECK}],)
        
        # merge all the meta-data instructions together to form the meta_data frame we will forward
        meta_data = ForecastMetaDataFrame.concat([meta_data, meta_data_series_dates, meta_data_series_step_init, meta_data_series_epi], verify_integrity = True, drop_dups = False)

        # final common checks and output generation
        return self._forecast_model_common_output(updated_model, meta_data, input_col_id)
    


    # generate_table_values
    # Generate the default values for the patient_count_table (dates and zeros for counts)
    # 
    # INPUTS:
    # OUTPUTS:
    #   List of dictionaries / one dictionary per row, looking like this:
    #  value=[{"dates": "2010-01-01", "patient_counts": 10,},
    #         {"dates": "2010-02-01", "patient_counts": 20,}],
    def generate_table_values(self, field_value: str, field_name: str) -> List[dict]:
        # get the current values in the patient_counts table
        old_values = self.patient_count

        # generate the dates needed (we'll need this regardless of whether we have old values or not)
        dates = ForecastDataModel.gen_forecast_dates(start_year = int(self.start_year),
                                                     start_month = int(self.start_month),
                                                     num_years = int(self.num_years),
                                                     timescale = ForecastModelTimescale(self.timescale))
        num_rows = len(dates)

        # if there are no old values, generate a brand list of dicts for the table
        if(old_values is None or not old_values):
            return [{ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: dates[i], "patient_counts": ForecastDataModel.EDITABLE_VALUES_TOKEN} for i in range(num_rows)]
        
        # otherwise, resize the exist values into the new size (note:  always add the dates in)
        else:
            new_df = ForecastFormModelUtilities.refill_drataframe(new_dim_rows=num_rows, new_dim_cols=2, prev_data=DataFrame(old_values), col_name_prefix="patient_counts", dates=dates)
            return new_df.to_data_list()
