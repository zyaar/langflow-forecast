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
from langflow.schema import DataFrame
from langflow.schema.table import EditMode
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities



# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import List, Any
import pandas as pd
import copy


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastEpidemiologyTB
# This class set-up up the model of the forecast to be used and the initial numbers that all others will filter down or compute from
class ForecastEpidemiologyTB(Component):

    # COMPONENT META-DATA
    # -------------------
    display_name: str = "Epidemiology TB"
    description: str = "Build an epidemiology stream of patients using a TIME BASED model."
    icon = "Globe"
    name: str = "EpidemiologyTB"


    # COMPONENT INPUTS
    # ----------------
    inputs = [
        # Number of Years in Forecast
        StrInput(
            name="num_years",
            display_name="# of Years to Forecast",
            info="The number of years to include in the forecast.",
            required=True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Start Year
        StrInput(
            name="start_year",
            display_name="Start Year",
            info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
            required=True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Time Scale
        StrInput(
            name = "timescale",
            display_name = "Time-Scale",
            info = "The granularity of the time scale for the forecast.",
            required = True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Month Start of Fiscal Year
        StrInput(
            name="start_month",
            display_name="Month Start of Fiscal Year",
            info="For fiscal years which do not start in January, allows you the option of specifying the start month.",
            required = True,
            show = True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

        # Input Type
        StrInput(
            name = "input_type",
            display_name = "Input Type",
            info = "Determines the type of forecast to generate.  'Time Based Input' generates which allows for individual values to be entered at the time-scale chosen.  'Single Input' uses a base value and growth/shrink rate at the time-scale chosen.",
            required = True,
            show = True,
            dynamic = True,
            real_time_refresh = True,
            advanced=True,
        ),

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


    # COMPONENT OUTPUTS
    # -----------------
    outputs = [
        Output(display_name="Epidemiology Patient Flow", name="epi_forecast_model", method="update_forecast_model"),
    ]


    # COMPONENT FORM UPDATE RULES
    # ---------------------------
    form_update_rules = {}
    #form_trigger_rules = []
    form_trigger_rules = [
        #(ForecastFormTriggerCalc.TriggerType.RUN_FUNCT, ("generate_table_values", ["patient_count"])),
        (ForecastFormTriggerCalc.TriggerType.UPDATE_VALUE, ("patient_count", "generate_table_values", ["patient_count"])),
    ]


    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing fields whenever an update happens from a dynamic field
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
        build_config = forecastFormTriggerCalc.execute_trigger(build_config,
                                                               self.form_trigger_rules,
                                                               field_value=field_value,
                                                               field_name=field_name,
                                                               
                                                               generate_table_values=self.generate_table_values)
        
        # return updated config         
        return(build_config)
    


    # INPUT VALIDATION
    # ----------------
    def validate_inputs(self):
        msg = ""

        # CHECK FOR REQUIRED INPUTS:
        # patient_count
        if(self.patient_count is None or not isinstance(self.patient_count, list) or len(self.patient_count) < 1):
            msg += f"\n* Missing values for '{self.get_input_display_name("patient_count")}'."
                    

        # if any errors occurred during validation, stop everything and raise an error
        if(msg != ""):
            self.status = msg
            self.stop
            raise ValueError(msg)



    # ASSOCIATED FUNCTIONS (convert inputs to outputs, i.e. biz logic)
    # --------------------
        
    # generate_forecast_model
    # Output function epi_forecast_model end-point
    # 
    # INPUTS:
    # OUTPUTS:
    #   DataFrame
    def update_forecast_model(self) -> DataFrame:
        self.validate_inputs()
        updated_model = DataFrame(self.patient_count).rename(columns={"patient_counts":str(self._id)})
 
        return(updated_model)
    

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
