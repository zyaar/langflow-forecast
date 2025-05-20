import pandas as pd
#from langflow.components.forecasting.common.constants import ForecastModelTimescale
from langflow.components.forecasting.common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale

from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.date_utils import gen_dates


def main():
    # test gen_dates
    start_year = 2026
    start_month = 4
    forecast_period = 3

    # Test creating a YEARLY forecast model
    dummy_patient_series_yearly = [1000] * forecast_period

    print("Create Yearly ForecastDataModel - easiest way")
    forecast_3yr_by_year_easy = ForecastDataModel.init_forecast_data_model_single_series(start_year=start_year,
                                                                                         start_month=start_month,
                                                                                         num_years=forecast_period,
                                                                                         input_type=ForecastModelInputTypes.TIME_BASED,
                                                                                         timescale=ForecastModelTimescale.YEAR,
                                                                                         data=dummy_patient_series_yearly,
                                                                                         series_name="epi2")
    print(forecast_3yr_by_year_easy)
    print()



    print("Create Yearly ForecastDataModel - more complicated way")
    time_series_yearly = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = forecast_period, timescale = ForecastModelTimescale.YEAR)
    forecast_3yr_by_year = ForecastDataModel.init_forecast_data_model(start_year=start_year,
                                                                      start_month=start_month,
                                                                      num_years=forecast_period,
                                                                      input_type=ForecastModelInputTypes.TIME_BASED,
                                                                      timescale=ForecastModelTimescale.YEAR,
                                                                      data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_yearly, "epi1": dummy_patient_series_yearly})
    print(forecast_3yr_by_year)
    print()

    # test if attrbutes are retrievable
    print("Test retrieving attributes:")
    print("start_year: ", forecast_3yr_by_year.attrs["start_year"])
    print("start_month: ", forecast_3yr_by_year.attrs["start_month"])
    print("num_years: ", forecast_3yr_by_year.attrs["num_years"])
    print("input_type: ", forecast_3yr_by_year.attrs["input_type"])
    print("timescale: ", forecast_3yr_by_year.attrs["timescale"])
    print()

    # test generating new pandas and see if attributes survive
    forecast_3yr_by_year_1 = forecast_3yr_by_year.loc[forecast_3yr_by_year.index < max(forecast_3yr_by_year.index)]

    print(forecast_3yr_by_year_1)
    print()

    print("start_year: ", forecast_3yr_by_year_1.attrs["start_year"])
    print("start_month: ", forecast_3yr_by_year_1.attrs["start_month"])
    print("num_years: ", forecast_3yr_by_year_1.attrs["num_years"])
    print("input_type: ", forecast_3yr_by_year_1.attrs["input_type"])
    print("timescale: ", forecast_3yr_by_year_1.attrs["timescale"])
    print()


    # Test creating a MONTHLY forecast model
    print("Create Monthly ForecastDataModel")
    time_series_monthly = gen_dates(start_year = start_year, start_month=start_month, num_years = forecast_period, time_scale = ForecastModelTimescale.MONTH)
    dummy_patient_series_monthly = [200] * (forecast_period*12)

    forecast_3yr_by_month = ForecastDataModel.init_forecast_data_model(start_year=start_year,
                                                                       start_month=start_month,
                                                                       num_years=forecast_period,
                                                                       input_type=ForecastModelInputTypes.SINGLE_INPUT,
                                                                       timescale=ForecastModelTimescale.MONTH,
                                                                       data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_monthly, "epi1": dummy_patient_series_monthly})
    print(forecast_3yr_by_month)
    print()

    # Test converting a YEARLY forecast model to MONTHLY
    print("Test converting Yearly to Monthly")
    yearly_converted_to_monthly = ForecastDataModel.upsample_yearly_to_monthly(forecast_3yr_by_year)
    print(yearly_converted_to_monthly)
    print()

    # Test converting a MONTHLY forecast model to a YEARLY
    print("Test converting Monthly to Yearly")
    monthly_converted_to_yearly = ForecastDataModel.downsample_monthly_to_yearly(yearly_converted_to_monthly)
    print(monthly_converted_to_yearly)
    print()

    # test combining two different data sets that are the same in every way
    combined_monthly_forecast_model = ForecastDataModel.combine_forecast_models([yearly_converted_to_monthly, yearly_converted_to_monthly], agg_col_funct="sum")
    print(combined_monthly_forecast_model)
    print()

     # test combining two different data sets that are the same in every way
    combined_yearly_forecast_model = ForecastDataModel.combine_forecast_models([monthly_converted_to_yearly, monthly_converted_to_yearly], agg_col_funct="sum")
    print(combined_yearly_forecast_model)
    print()
   
   # test double combine (month)
    double_combined_monthly_forecast_model = ForecastDataModel.combine_forecast_models([combined_monthly_forecast_model, combined_monthly_forecast_model], agg_col_funct="sum")
    print(double_combined_monthly_forecast_model)
    print()

   # test double combine (year)
    double_combined_yearly_forecast_model = ForecastDataModel.combine_forecast_models([combined_yearly_forecast_model, combined_yearly_forecast_model])
    print(double_combined_yearly_forecast_model)
    print()




if __name__ == "__main__":
    main()