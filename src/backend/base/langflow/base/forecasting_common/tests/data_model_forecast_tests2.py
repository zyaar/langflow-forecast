from langflow.schema.dataframe import DataFrame
import pandas as pd

from langflow.base.forecasting_common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.date_utils import gen_dates



def main():
    # TEST FORECASTING UTILITIES
    # ==========================

    # create treatment details
    progression_curve = [1.0, 0.75, 0.6, 0.5, 0.4, 0.3]
    product_use_matrix = [[4, 0, 1, 0],
                          [2, 1, 0, 1],
                          [2, 0, 1, 0],
                          [2, 1, 0, 1],
                          [2, 0, 1, 0],
                          [2, 1, 0, 1]]
    col_names = ["product_1", "product_2", "product_3", "product_4"]
    treatment_pat_by_month_forecast = pd.DataFrame(data=product_use_matrix, columns=col_names)
    month_of_treatment_pd = pd.Series(data = list(range(1, len(progression_curve)+1)), name = "month")
    progression_curve_pd = pd.Series(data = progression_curve, name=ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME)
    treatment_details =  pd.concat([month_of_treatment_pd, progression_curve_pd, treatment_pat_by_month_forecast], axis = 1)

    # =======================
    # Test a MONTHLY forecast
    # =======================
    print(f"\n\nTest MONTHLY forecast")
    print(     "---------------------\n")
    start_year = 2026
    num_years = 3
    start_month = 1
    forecast_period = num_years * 12

    dummy_patient_series_yearly = [1000] * forecast_period


    # generate patient forecast
    forecast_in = ForecastDataModel.init_forecast_data_model_single_series(data = dummy_patient_series_yearly,
                                                                           start_year = start_year,
                                                                           num_years = num_years,
                                                                           start_month = start_month,
                                                                           timescale = ForecastModelTimescale.MONTH,
                                                                           series_name = "epi_1")
    print(f"\nforecast_in:\n{forecast_in}")    
    
    # generate patient treatment forecast
    pat_by_treatment_month, pat_leaving_by_treatment_month = ForecastDataModel.calc_treatment_pat_forecast(col_prefix = "epi_1_treatment_1_",
                                                                                                           forecast_in = forecast_in,
                                                                                                           treatment_details = treatment_details,
                                                                                                           forecast_timescale = ForecastModelTimescale.MONTH)
    print(f"\npat_by_treatment_month:\n{pat_by_treatment_month}")
    print(f"\npat_leaving_by_treatment_month:\n{pat_leaving_by_treatment_month}")


    # generate product forecast
    
    # Test the calc_treatment_rx_forecast_for_product() which generates the Rx forecast for a product in the same timescale
    print("\n\nTest the calc_treatment_rx_forecast_for_product() which generates the Rx forecast for a product in the same timescale:")

    product_use_in_treatment_by_month = ForecastDataModel.calc_treatment_rx_forecast_for_product(product_name = "product_1",
                                                                                                 col_prefix = "epi_1_treatment_1_",
                                                                                                 forecast_in = forecast_in,
                                                                                                 treatment_details = treatment_details,
                                                                                                 forecast_timescale = ForecastModelTimescale.MONTH)
    
    print(f"\n\nproduct_use_in_treatment_by_month:\n{product_use_in_treatment_by_month}")


    # ======================
    # Test a YEARLY forecast
    # ======================
    print(f"\n\nTest YEARLY forecast")
    print(     "--------------------\n")
    start_year = 2026
    num_years = 3
    start_month = 1
    forecast_period = num_years

    dummy_patient_series_yearly = [12000] * forecast_period


    forecast_in = ForecastDataModel.init_forecast_data_model_single_series(data = dummy_patient_series_yearly,
                                                                           start_year = start_year,
                                                                           num_years = num_years,
                                                                           start_month = start_month,
                                                                           timescale = ForecastModelTimescale.YEAR,
                                                                           series_name = "epi_1")
    print(f"\nforecast_in:\n{forecast_in}")    
    
    # generate patient forecast
    pat_by_treatment_year, pat_leaving_by_treatment_year = ForecastDataModel.calc_treatment_pat_forecast(col_prefix = "epi_1_treatment_1_",
                                                                                                         forecast_in = forecast_in,
                                                                                                         treatment_details = treatment_details,
                                                                                                         forecast_timescale = ForecastModelTimescale.YEAR)
    print(f"\npat_by_treatment_year:\n{pat_by_treatment_year}")
    print(f"\npat_leaving_by_treatment_year:\n{pat_leaving_by_treatment_year}")
    
    
    product_use_in_treatment_by_year = ForecastDataModel.calc_treatment_rx_forecast_for_product(product_name = "product_1",
                                                                                                 col_prefix = "epi_1_treatment_1_",
                                                                                                 forecast_in = forecast_in,
                                                                                                 treatment_details = treatment_details,
                                                                                                 forecast_timescale = ForecastModelTimescale.YEAR)
    
    print(f"\n\nproduct_use_in_treatment_by_year:\n{product_use_in_treatment_by_year}")

    


if __name__ == "__main__":
    main()

