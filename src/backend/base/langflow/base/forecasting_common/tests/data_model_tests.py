from langflow.schema.dataframe import DataFrame

from langflow.base.forecasting_common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.date_utils import gen_dates


def dump_attr(df):
    # test if attrbutes are retrievable
    print("start_year: ", df.attrs["start_year"])
    print("start_month: ", df.attrs["start_month"])
    print("num_years: ", df.attrs["num_years"])
    print("input_type: ", df.attrs["input_type"])
    print("timescale: ", df.attrs["timescale"])
    print()



def main():
    # test gen_dates
    start_year = 2026
    start_month = 4
    forecast_period = 3



    # TEST CREATING AN EMPTY DATASET
    # ------------------------------

    # create empty YEARLY
    print("Test creating an empty dataset YEARLY")
    empty_model_yearly = ForecastDataModel.generate_empty_forecast_data_model(start_year = 2000,
                                                                              num_years = 3,
                                                                              start_month = 1,
                                                                              timescale = ForecastModelTimescale.YEAR)
    print(f"empty_model_yearly:\n{empty_model_yearly}\n")


    # create empty MONTHLY
    print("Test creating an empty dataset MONTHLY")
    empty_model_monthly = ForecastDataModel.generate_empty_forecast_data_model(start_year = 2000,
                                                                              num_years = 3,
                                                                              start_month = 1,
                                                                              timescale = ForecastModelTimescale.MONTH)
    print(f"empty_model_monthly:\n{empty_model_monthly}\n")


    # TEST CREATING A DATASET WITH ONE COLUMN OF DATA
    # -----------------------------------------------

    # Test creating a YEARLY forecast model to easy way
    dummy_patient_series_yearly = [1000] * forecast_period

    print("Create Yearly ForecastDataModel - easiest way")
    forecast_3yr_by_year_easy = ForecastDataModel.init_forecast_data_model_single_series(data = dummy_patient_series_yearly,
                                                                                         start_year = start_year,
                                                                                         num_years = forecast_period,
                                                                                         start_month = start_month,
                                                                                         timescale=ForecastModelTimescale.YEAR,
                                                                                         series_name="epi1")
    print(f"forecast_3yr_by_year_easy:\n{forecast_3yr_by_year_easy}\n")


    # Test creating a MONTHLY forecast model to easy way
    print("Create Monthly ForecastDataModel - easiest way")
    dummy_patient_series_monthly = [200] * (forecast_period*12)

    forecast_3yr_by_month_easy = ForecastDataModel.init_forecast_data_model_single_series(data = dummy_patient_series_monthly,
                                                                                          start_year = start_year,
                                                                                          num_years = forecast_period,
                                                                                          start_month = start_month,
                                                                                          timescale=ForecastModelTimescale.MONTH,
                                                                                          series_name="epi2")
    print(f"forecast_3yr_by_month_easy:\n{forecast_3yr_by_month_easy}\n")


    # TEST TIMESCALE CONVERSION FUNCTIONS
    # -----------------------------------

    # Test converting a YEARLY forecast model to MONTHLY
    print("Test converting a YEARLY forecast model to MONTHLY")
    yearly_converted_to_monthly = ForecastDataModel.yearly_to_monthly(forecast_3yr_by_year_easy)
    print(f"yearly_converted_to_monthly:\n{yearly_converted_to_monthly}\n")


    # Test converting a MONTHLY forecast model to a YEARLY
    print("Test converting a MONTHLY forecast model to a YEARLY")
    monthly_converted_to_yearly = ForecastDataModel.monthly_to_yearly(yearly_converted_to_monthly)
    print(f"monthly_converted_to_yearly:\n{monthly_converted_to_yearly}\n")


    # TEST COMBINING DATA SETS
    # ------------------------

    # SINGLE COLUMN
    yearly_converted_to_monthly1 = yearly_converted_to_monthly
    yearly_converted_to_monthly2 = yearly_converted_to_monthly.rename(columns={"epi1": "epi2"})

    # Test concat, two different data sets MONTHLY
    print("Test 'concat' two different data sets MONTHLY")
    concat_monthly_forecast_model1 = ForecastDataModel.concat([yearly_converted_to_monthly1, yearly_converted_to_monthly2])
    print(f"concat_monthly_forecast_model:\n{concat_monthly_forecast_model1}\n")

    # Test concat_and_sum, two different data sets MONTHLY
    print("Test 'concat_and_sum' two different data sets MONTHLY")
    concat_and_sum_monthly_forecast_model2 = ForecastDataModel.concat_and_sum([yearly_converted_to_monthly1, yearly_converted_to_monthly2])
    print(f"concat_and_sume_monthly_forecast_model:\n{concat_and_sum_monthly_forecast_model2}\n")
    
    
    # DOUBLE COLUMN
    concat_and_sum_monthly_forecast_model3= concat_and_sum_monthly_forecast_model2.rename(columns={"epi1": "epi10", "epi2": "epi20", concat_and_sum_monthly_forecast_model2.columns[-1]: "Total_123"})

    # est concat, two different data sets MONTHLY
    print("Test 'concat' two different data sets, multi-column, MONTHLY, no agg function")
    double_combined_monthly_forecast_model = ForecastDataModel.concat([concat_and_sum_monthly_forecast_model2, concat_and_sum_monthly_forecast_model3])
    print(f"double_combined_monthly_forecast_model:\n{double_combined_monthly_forecast_model}\n")

    # Test combine MONTHLY, no agg function
    print("Test 'concat_and_sum' two different data sets, multi-column, MONTHLY, no agg function")
    double_combined_monthly_forecast_model = ForecastDataModel.concat_and_sum([concat_and_sum_monthly_forecast_model2, concat_and_sum_monthly_forecast_model3])
    print(f"double_combined_monthly_forecast_model:\n{double_combined_monthly_forecast_model}\n")



    # TEST ADDING COLUMN
    # ------------------

    print("Test adding column")
    model_w_new_col = ForecastDataModel.add_col_to_model(double_combined_monthly_forecast_model, new_col_values=[0] * len(double_combined_monthly_forecast_model.index), new_col_name="new_col_name")
    print(f"model_w_new_col:\n{model_w_new_col}\n")



if __name__ == "__main__":
    main()