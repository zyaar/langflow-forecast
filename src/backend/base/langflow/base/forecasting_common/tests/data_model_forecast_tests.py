from langflow.schema.dataframe import DataFrame
import pandas as pd

from langflow.base.forecasting_common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.date_utils import gen_dates



def main():
    # TEST FORECASTING UTILITIES
    # ==========================

    # monthly_to_yearly (no date column)
    # ----------------------------------

    print(f"\n\nTest conversions NO DATE column")

    # test pd.Series conversion
    print(f"\n\ntest pd.Series conversion:\n")
    monthly_forecast_series_no_dates = pd.Series(data = [100.0] * 36, index = list(range(1,37)))
    print(f"monthly_forecast_series_no_dates:\n{monthly_forecast_series_no_dates}")
    yearly_forecast_series_conversion = ForecastDataModel.monthly_to_yearly(monthly_forecast_series_no_dates)
    print(f"\nyearly_forecast_series_conversion:\n{yearly_forecast_series_conversion}")


    # test pd.DataFrame conversion
    print(f"\n\ntest pd.DataFrame conversion:\n")
    monthly_forecast_df_no_dates = pd.DataFrame(data = {"var1": [100.0] * 36, "var2": [200] * 36, "var3": [30.0] * 36}, index = list(range(1,37)))
    print(f"\n\nmonthly_forecast_df_no_dates:\n{monthly_forecast_df_no_dates}")
    yearly_forecast_df_conversion = ForecastDataModel.monthly_to_yearly(monthly_forecast_df_no_dates)
    print(f"\n\nyearly_forecast_df_conversion:\n{yearly_forecast_df_conversion}")


    # yearly_to_monthly (no date column)
    # ----------------------------------

    # test pd.Series conversion
    print(f"\n\ntest pd.Series conversion:\n")
    yearly_forecast_series_no_dates = pd.Series(data = [100.0] * 3, index = [1, 2, 3])
    print(f"yearly_forecast_series_no_dates:\n{yearly_forecast_series_no_dates}")
    monthly_forecast_series_conversion = ForecastDataModel.yearly_to_monthly(yearly_forecast_series_no_dates)
    print(f"\nmonthly_forecast_series_conversion:\n{monthly_forecast_series_conversion }")

    # test pd.DataFrame conversion
    print(f"\n\ntest pd.DataFrame conversion:\n")
    monthly_forecast_df_no_dates = pd.DataFrame(data = {"var1": [100.0] * 36, "var2": [200] * 36, "var3": [30.0] * 36}, index = list(range(1,37)))
    print(f"\n\nmonthly_forecast_df_no_dates:\n{monthly_forecast_df_no_dates}")
    yearly_forecast_df_conversion = ForecastDataModel.monthly_to_yearly(monthly_forecast_df_no_dates)
    print(f"\n\nyearly_forecast_df_conversion:\n{yearly_forecast_df_conversion}")



    # monthly_to_yearly (with date column)
    # ----------------------------------

    print(f"\n\nTest conversions WITH DATE column")

    # test pd.DataFrame conversion
    print(f"\n\ntest pd.DataFrame conversion:\n")
    monthly_forecast_df_dates = pd.DataFrame(data = {ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: gen_dates(start_year = 2026, num_years = 3, start_month = 1, time_scale = ForecastModelTimescale.MONTH),
                                                         "var1": [100.0] * 36, 
                                                         "var2": [200] * 36, 
                                                         "var3": [30.0] * 36}, 
                                                         index = list(range(1,37)))
    print(f"\n\nmonthly_forecast_df_no_dates:\n{monthly_forecast_df_dates}")
    yearly_forecast_df_conversion = ForecastDataModel.monthly_to_yearly(monthly_forecast_df_dates)
    print(f"\n\nyearly_forecast_df_conversion:\n{yearly_forecast_df_conversion}")


    # yearly_to_monthly (with date column)
    # ----------------------------------

    # test pd.DataFrame conversion
    print(f"\n\ntest pd.DataFrame conversion:\n")
    yearly_forecast_df_dates = pd.DataFrame(data = {ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: gen_dates(start_year = 2026, num_years = 3, start_month = 1, time_scale = ForecastModelTimescale.YEAR), 
                                                       "var1": [100.0, 50.0, 25.0], 
                                                       "var2": [200, 100, 50], 
                                                       "var3": [30.0, 15.0, 5.0]}, 
                                                       index = list(range(1,4)))
    print(f"\n\nyearly_forecast_df_no_dates:\n{yearly_forecast_df_dates}")
    monthly_forecast_df_conversion = ForecastDataModel.yearly_to_monthly(yearly_forecast_df_dates)
    print(f"\n\nymonthly_forecast_df_conversion:\n{monthly_forecast_df_conversion}")






    # TEST FORECASTING FUNCTIONS
    # ==========================
    num_NTP_per = list(range(100, 1300, 100))
    treatment_duration = 6
    progression_curve = [1.0, 0.75, 0.6, 0.5, 0.4, 0.3]

    # Test generate patient treatment forecast table
    print("Test gen_pat_by_treatment_month()")
    print(f"num_NTP_per = {num_NTP_per}")
    print(f"treatment_duration = {treatment_duration}")
    print(f"{ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME} = {progression_curve}")

    pat_by_treatment_month1, pat_leaving_by_treatment_month1 = ForecastDataModel.gen_forecast_pat_by_treatment_month(treatment_name = "treatment_1_", 
                                                                                                                     num_NTP_per = num_NTP_per, 
                                                                                                                     treatment_duration = 6, 
                                                                                                                     progression_curve = progression_curve)

    print(f"\npat_by_treatment_month1:\n{pat_by_treatment_month1}\n")
    print(f"\npat_leaving_by_treatment_month1:\n{pat_leaving_by_treatment_month1}\n")


    # Test generate patient treatment forecast table with an initial state set
    initial_state = [100, 75, 50, 25, 10, 5]

    print(f"\n\nTest gen_pat_by_treatment_month() with an initial state set")
    print(f"num_NTP_per = {num_NTP_per}")
    print(f"treatment_duration = {treatment_duration}")
    print(f"{ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME} = {progression_curve}")
    print(f"initial_state = {initial_state}")

    pat_by_treatment_month2, pat_leaving_by_treatment_month2 = ForecastDataModel.gen_forecast_pat_by_treatment_month(treatment_name = "treatment_2_", 
                                                                                                                     num_NTP_per = num_NTP_per, 
                                                                                                                     treatment_duration = 6, 
                                                                                                                     progression_curve = progression_curve, 
                                                                                                                     pc_initial_state = initial_state)

    print(f"\npat_by_treatment_month2:\n{pat_by_treatment_month2}\n")
    print(f"\npat_leaving_by_treatment_month2:\n{pat_leaving_by_treatment_month2}\n")


    # Test generate product rx data
    product_use_matrix = [[4, 0, 1, 0],
                          [2, 1, 0, 1],
                          [2, 0, 1, 0],
                          [2, 1, 0, 1],
                          [2, 0, 1, 0],
                          [2, 1, 0, 1]]
    col_names = ["product_1", "product_2", "product_3", "product_4"]
    treatment_pat_by_month_forecast = pd.DataFrame(data=product_use_matrix, columns=col_names)

    print(f"\n\nTest gen_forecast_product_rx_by_prog_month()")
    print(f"treatment_pat_by_month_forecast=\n")
    print(treatment_pat_by_month_forecast)

    products_by_treatment_month1 = ForecastDataModel.gen_forecast_product_rx_by_prog_month(product_name = "product_1",
                                                                                           treatment_name = "treatment_1_",
                                                                                           treatment_pat_by_month_forecast = pat_by_treatment_month1, 
                                                                                           product_use_in_treatment_by_month = treatment_pat_by_month_forecast)
    
    print(f"\n{products_by_treatment_month1}")


     # Test the calc_treatment_pat_forecast() which is the WRAPPER around gen_forecast_pat_by_treatment_month()
    print("\n\nTest the calc_treatment_pat_forecast() which is the WRAPPER around gen_forecast_pat_by_treatment_month()")

    # treatment_details
    month_of_treatment_pd = pd.Series(data = list(range(1, len(progression_curve)+1)), name = "month")
    progression_curve_pd = pd.Series(data = progression_curve, name=ForecastDataModel.PATIENT_PROGRESSION_COLUMN_NAME)
    treatment_details =  pd.concat([month_of_treatment_pd, progression_curve_pd, treatment_pat_by_month_forecast], axis = 1)
    print(f"treatment_details:\n{treatment_details}")

    # forecast_in
    start_year = 2026
    start_month = 4
    forecast_period = 12
    num_years = 1

    dummy_patient_series_yearly = [1000] * forecast_period
    forecast_in = ForecastDataModel.init_forecast_data_model_single_series(data = dummy_patient_series_yearly,
                                                                           start_year = start_year,
                                                                           num_years = num_years,
                                                                           start_month = start_month,
                                                                           timescale = ForecastModelTimescale.MONTH,
                                                                           series_name = "epi_1")
    
    print(f"\n\nforecast_in:\n{forecast_in}")

    pat_by_treatment_month1, pat_leaving_by_treatment_month1 = ForecastDataModel.calc_treatment_pat_forecast(col_prefix = "epi_1_treatment_1_",
                                                                                                             forecast_in = forecast_in,
                                                                                                             treatment_details = treatment_details,
                                                                                                             forecast_timescale = ForecastModelTimescale.MONTH)

    print(f"\npat_by_treatment_month1:\n{pat_by_treatment_month1}\n")
    print(f"\npat_leaving_by_treatment_month1:\n{pat_leaving_by_treatment_month1}\n")


    # Test the calc_treatment_rx_forecast_for_product() which generates the Rx forecast for a product in the same timescale
    print("\n\nTest the calc_treatment_rx_forecast_for_product() which generates the Rx forecast for a product in the same timescale:")

    product_use_in_treatment_by_month = ForecastDataModel.calc_treatment_rx_forecast_for_product(product_name = "product_1",
                                                                                                 col_prefix = "epi_1_treatment_1_",
                                                                                                 forecast_in = forecast_in,
                                                                                                 treatment_details = treatment_details,
                                                                                                 forecast_timescale = ForecastModelTimescale.MONTH)
    
    print(f"\n\nproduct_use_in_treatment_by_month:\n{product_use_in_treatment_by_month}")



if __name__ == "__main__":
    main()

