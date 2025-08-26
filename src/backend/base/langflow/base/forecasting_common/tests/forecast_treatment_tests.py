import pickle
from langflow.schema import DataFrame, Data
#from langflow.base.forecasting_common.components.forecast_component import ForecastComponent
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities

from langflow.base.forecasting_common.components.forecast_sum_input_TB import ForecastSumInputTB

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
                                                                        ForecastDataSeriesMetaDataComparisonType,
                                                                        ForecastMetaDataRange,
                                                                        ForecastMetaDataRangeSchema)

from langflow.base.forecasting_common.controllers.forecast_treatment_TB_Controller import ForecastTreatmentTBController

# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List, Tuple
from datetime import datetime
import copy
import pandas as pd


# CONFIG CONSTANTS
# ================

GENERATE_TEST_DATA = False
TEST_DIR = "output/tests/treatment_forecast/"
TEST_DIR_INPUTS = TEST_DIR + "inputs/"
TEST_DIR_RESULTS = TEST_DIR + "results/"

INPUT_FILENAME = TEST_DIR_INPUTS + "var_inputs.pickle"
PAT_TEST_RESULTS_FILENAME = TEST_DIR_RESULTS + "pat_treatment_test_results_data.pickle"
RX_TEST_RESULTS_FILENAME = TEST_DIR_RESULTS + "rx_treatment_test_results_data.pickle"




_id = "TreatmentTB-RNy5i"

# COMPONENT
display_name: str = "Treatment TB"
description: str = "Apply a treatment regiment of products to an incoming patient flow"
icon = "Syringe"
name: str = "TreatmentTB"

# OUTPUT INFO
NUM_STATIC_OUTPUTS = 2 # one static output (# patients leaving/month), rest is product

# ROW_SET VAR
MAX_TREATMENT_DURATION = 240 # max treatment duration supported is 20 years

# COL_SET VAR
MAX_PRODUCTS = 100
COL_PREFIX = "product"
MONTH_PREFIX = "month"

# TABLE
TABLE_NAME = "treatment_details"
TABLE_SCHEMA_INPUT_NAME = f"hidden_treatment_details"
NUM_STATIC_COLS = 2 # two static columns in table (month of pression, % of people progressing), rest is product
NUM_STATIC_IGNORE_COLS = 0 # of static columns to ignore
NUM_STATIC_INPUT_COLS = 2 # of static columsn to read as input (before the variable columns)

# MISC
CHECK_OUTPUT_ID = False

timescale = ForecastModelTimescale.YEAR


def main():

    with open(INPUT_FILENAME, "rb") as file:
        input_vars = pickle.load(file)

    id = input_vars["id"]
    display_name = input_vars["display_name"]
    month_prefix = input_vars["month_prefix"]

    # current forecast
    updated_data = input_vars["updated_data"]
    updated_meta_data = input_vars["updated_meta_data"]
    
    # treatment details table
    treatment_table_data = input_vars["treatment_table_data"]
    treatment_table_meta_data = input_vars["treatment_table_meta_data"]
    pc_col_id = input_vars["pc_col_id"]

    # pre-forecast input table
    pre_forecast_inputs_data = input_vars["pre_forecast_inputs_data"]
    pre_forecast_inputs_meta_data = input_vars["pre_forecast_inputs_meta_data"]
    pf_col_id = input_vars["pf_col_id"]

    # pre-forecast patient flow table
    pre_forecast_patient_flow_data = input_vars["pre_forecast_patient_flow_data"]
    pre_forecast_patient_flow_meta_data = input_vars["pre_forecast_patient_flow_meta_data"]
    pmpf_col_prefix = input_vars["pmpf_col_prefix"]

    #treatment_test = ForecastTreatmentTB()
    treatment_test = ForecastTreatmentTBController()
    results = treatment_test.calc_treatment_pat_forecast(# self variables
                                                         id = id,
                                                         display_name = display_name,
                                                         month_prefix = month_prefix,

                                                         # current forecast
                                                         updated_data = updated_data,
                                                         updated_meta_data = updated_meta_data,
                                                        
                                                         # treatment details table
                                                         treatment_table_data = treatment_table_data,
                                                         treatment_table_meta_data = treatment_table_meta_data,
                                                         pc_col_id = pc_col_id,

                                                         # pre-forecast input table
                                                         pre_forecast_inputs_data = pre_forecast_inputs_data,
                                                         pre_forecast_inputs_meta_data = pre_forecast_inputs_meta_data,
                                                         pf_col_id = pf_col_id,
                                                
                                                         # pre-forecast patient flow table
                                                         pre_forecast_patient_flow_data = pre_forecast_patient_flow_data,
                                                         pre_forecast_patient_flow_meta_data = pre_forecast_patient_flow_meta_data,
                                                         pmpf_col_prefix = pmpf_col_prefix)
    

    treatment_details_model = results["pat_on_treatment"]["treatment_table_data"]
    treatment_details_meta_data = results["pat_on_treatment"]["treatment_table_meta_data"]

    pat_by_treatment_month_data= results["pat_on_treatment"]["pat_by_treatment_month_data"]
    pat_by_treatment_month_meta_data = results["pat_on_treatment"]["pat_by_treatment_month_meta_data"]

    pat_leaving_by_treatment_month_data = results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_data"]
    pat_leaving_by_treatment_month_meta_data = results["pat_leaving_treatment"]["pat_leaving_by_treatment_month_meta_data"]

    updated_model = results["pat_on_treatment"]["updated_data"]


    # save test data for future run comparisons
    if(GENERATE_TEST_DATA):
        # save pickled objects for automated comparison
        with open(PAT_TEST_RESULTS_FILENAME, "wb") as f:
            pickle.dump({
                "treatment_details_model": treatment_details_model,
                "pat_by_treatment_month_data": pat_by_treatment_month_data,
                "pat_by_treatment_month_meta_data": pat_by_treatment_month_meta_data,
                "pat_leaving_by_treatment_month_data": pat_leaving_by_treatment_month_data,
                "pat_leaving_by_treatment_month_meta_data": pat_leaving_by_treatment_month_meta_data
            }, f)

        # save excel / json output for easier human review
        treatment_details_model.to_excel(TEST_DIR_RESULTS + "treatment_details_model.xlsx")
        pat_by_treatment_month_data.to_excel(TEST_DIR_RESULTS + "pat_by_treatment_month_data.xlsx")
        pat_leaving_by_treatment_month_data.to_excel(TEST_DIR_RESULTS + "pat_leaving_by_treatment_month_data.xlsx")
        
        with open(TEST_DIR_RESULTS + "treatment_details_meta_data.json", "w") as f:
            f.write(treatment_details_meta_data.to_json(indent=4))

        with open(TEST_DIR_RESULTS + "pat_by_treatment_month_meta_data.json", "w") as f:
            f.write(pat_by_treatment_month_meta_data.to_json(indent=4))

        with open(TEST_DIR_RESULTS + "pat_leaving_by_treatment_month_meta_data.json", "w") as f:
            f.write(pat_leaving_by_treatment_month_meta_data.to_json(indent=4))

        print(f"Generated test data and saved to: {TEST_DIR_RESULTS}")
    else:
        # load previous test data for comparison
        with open(PAT_TEST_RESULTS_FILENAME, "rb") as f:
            test_data = pickle.load(f)
            golden_treatment_details_model = test_data["treatment_details_model"]
            golden_pat_by_treatment_month_data = test_data["pat_by_treatment_month_data"]
            golden_pat_by_treatment_month_meta_data = test_data["pat_by_treatment_month_meta_data"]
            golden_pat_leaving_by_treatment_month_data = test_data["pat_leaving_by_treatment_month_data"]
            golden_pat_leaving_by_treatment_month_meta_data = test_data["pat_leaving_by_treatment_month_meta_data"]

        pd.testing.assert_frame_equal(treatment_details_model, golden_treatment_details_model, check_dtype = True, check_like=True)
        pd.testing.assert_frame_equal(pat_by_treatment_month_data, golden_pat_by_treatment_month_data, check_dtype = True, check_like=True)
        pd.testing.assert_frame_equal(pat_leaving_by_treatment_month_data, golden_pat_leaving_by_treatment_month_data, check_dtype = True, check_like=True)

        assert(pat_by_treatment_month_meta_data.to_json() == golden_pat_by_treatment_month_meta_data.to_json())
        assert(pat_leaving_by_treatment_month_meta_data.to_json() == golden_pat_leaving_by_treatment_month_meta_data.to_json())
        print("calc_treatment_pat_forecast:  Test data matches golden data!")    



    # get product information:  product_id, product_display_name
    product_id = f"{COL_PREFIX}_1"
    product_display_name = "Product 1"

    (updated_data, updated_meta_data) = treatment_test.calc_treatment_rx_forecast_for_product(# self variables passed in
                                                                                              treatment_id = id,
                                                                                              treatment_display_name = display_name,
                                                                                              product_id = product_id,
                                                                                              product_display_name = product_display_name,
                                                                                              month_prefix = month_prefix,

                                                                                              # current forecast
                                                                                              updated_data = pat_by_treatment_month_data,
                                                                                              updated_meta_data = pat_by_treatment_month_meta_data,

                                                                                              # treatment details table
                                                                                              treatment_table_data = treatment_details_model,
                                                                                              treatment_table_meta_data = treatment_details_meta_data)
    
    # save test data for future run comparisons
    if(GENERATE_TEST_DATA):
        with open(RX_TEST_RESULTS_FILENAME, "wb") as f:
            pickle.dump({
                "updated_data": updated_data,
                "updated_meta_data": updated_meta_data
            }, f)

        # save excel / json output for easier human review
        updated_data.to_excel(TEST_DIR_RESULTS + "updated_data.xlsx")
        
        with open(TEST_DIR_RESULTS + "updated_meta_data.json", "w") as f:
            f.write(updated_meta_data.to_json(indent=4))

        print(f"Generated test data and saved to: {TEST_DIR_RESULTS}")
    else:
        # load previous test data for comparison
        with open(RX_TEST_RESULTS_FILENAME, "rb") as f:
            test_data = pickle.load(f)
            golden_updated_data = test_data["updated_data"]
            golden_updated_meta_data = test_data["updated_meta_data"]

        pd.testing.assert_frame_equal(updated_data, golden_updated_data, check_dtype = True, check_like=True)
        assert(updated_meta_data.to_json() == golden_updated_meta_data.to_json())
        print("calc_treatment_rx_forecast_for_product:  Test data matches golden data!")

    
    # convert updated data back to original timescale if needed
    if(timescale == ForecastModelTimescale.YEAR):
        (updated_data, updated_meta_data, *_) = ForecastDataModel.convert_timescale(updated_data, updated_meta_data, target = ForecastModelTimescale.YEAR)


if __name__ == "__main__":
    main()
