import pickle
# from langflow.schema import DataFrame, Data
# #from langflow.base.forecasting_common.components.forecast_component import ForecastComponent
from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
# from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
# from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
# from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities

# from langflow.base.forecasting_common.components.forecast_sum_input_TB import ForecaseSumInputTB

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

# from langflow.base.forecasting_common.controllers.forecast_treatment_TB_Controller import ForecastTreatmentTBController

# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import Any, List, Tuple
from datetime import datetime
import copy
import pandas as pd


def main():
    output_dir = "output/tests/convert_timescale/"

    pickle_1 = output_dir + "treatment_details_model.pickle"
    pickle_2 = output_dir + "treatment_details_meta_data.pickle"
    pickle_3 = output_dir + "pat_on_treatment_data.pickle"
    pickle_4 = output_dir + "pat_on_treatment_meta_data.pickle"
    pickle_5 = output_dir + "updated_model.pickle"

    # treatment_details_model
    # treatment_details_meta_data
    # pat_on_treatment_data
    # pat_on_treatment_meta_data
    # updated_model


    with open(pickle_1, "rb") as file:
        treatment_details_model = pickle.load(file)

    with open(pickle_2, "rb") as file:
        treatment_details_meta_data = pickle.load(file)

    with open(pickle_3, "rb") as file:
        pat_on_treatment_data = pickle.load(file)

    with open(pickle_4, "rb") as file:
        pat_on_treatment_meta_data = pickle.load(file)

    with open(pickle_5, "rb") as file:
        updated_model = pickle.load(file)


    _id = "TreatmentTB-RNy5i"


    print(pat_on_treatment_data)

    (pat_on_treatment_data_yearly, pat_on_treatment_meta_data_yearly, new_last_id) = ForecastDataModel.convert_timescale(data_model = pat_on_treatment_data, 
                                                                                                                         meta_data = pat_on_treatment_meta_data, 
                                                                                                                         target = ForecastModelTimescale.YEAR, 
                                                                                                                         step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)
    
    print("\n\n")
    print(pat_on_treatment_data_yearly)

    (pat_on_treatment_data_monthly, pat_on_treatment_meta_data_monthly, new_last_id) = ForecastDataModel.convert_timescale(data_model = pat_on_treatment_data_yearly, 
                                                                                                                           meta_data = pat_on_treatment_meta_data_yearly, 
                                                                                                                           target = ForecastModelTimescale.MONTH, 
                                                                                                                           step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)

    print("\n\n")
    print(pat_on_treatment_data_monthly)



    


if __name__ == "__main__":
    main()

