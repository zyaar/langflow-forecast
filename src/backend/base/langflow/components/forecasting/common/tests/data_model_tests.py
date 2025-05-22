from langflow.components.forecasting.common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.schema.dataframe import DataFrame

from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.date_utils import gen_dates


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

    # Test creating a YEARLY forecast model to easy way
    dummy_patient_series_yearly = [1000] * forecast_period

    print("Create Yearly ForecastDataModel - easiest way")
    forecast_3yr_by_year_easy = ForecastDataModel.init_forecast_data_model_single_series(start_year=start_year,
                                                                                         start_month=start_month,
                                                                                         num_years=forecast_period,
                                                                                         input_type=ForecastModelInputTypes.TIME_BASED,
                                                                                         timescale=ForecastModelTimescale.YEAR,
                                                                                         data=dummy_patient_series_yearly,
                                                                                         series_name="epi2")
    (is_valid, errMsg) = ForecastDataModel.is_valid_forecast_data_model(forecast_3yr_by_year_easy)

    if(not is_valid):
        raise ValueError(errMsg)

    print(forecast_3yr_by_year_easy)
    print()

    print("Test retrieving attributes:")
    dump_attr(forecast_3yr_by_year_easy)


    # Test creating a YEARLY forecast model to a more complicated way (using a dictionary instead of passing a series)
    print("Create Yearly ForecastDataModel - more complicated way")
    time_series_yearly = ForecastDataModel.gen_forecast_dates(start_year = start_year, start_month = start_month, num_years = forecast_period, timescale = ForecastModelTimescale.YEAR)
    forecast_3yr_by_year = ForecastDataModel.init_forecast_data_model(start_year=start_year,
                                                                      start_month=start_month,
                                                                      num_years=forecast_period,
                                                                      input_type=ForecastModelInputTypes.SINGLE_INPUT,
                                                                      timescale=ForecastModelTimescale.YEAR,
                                                                      data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: time_series_yearly, "epi1": dummy_patient_series_yearly})
    (is_valid, errMsg) = ForecastDataModel.is_valid_forecast_data_model(forecast_3yr_by_year)

    if(not is_valid):
        raise ValueError(errMsg)

    print(forecast_3yr_by_year)
    print()

    # test if attrbutes are retrievable
    dump_attr(forecast_3yr_by_year)


    # test generating new pandas and see if attributes survive
    print("Running DataFrame through pandas transformation and back to see if everything survives")
 
    forecast_3yr_by_year = ForecastDataModel.convert_to_working_index(forecast_3yr_by_year)

    forecast_3yr_by_year_1 = forecast_3yr_by_year.loc[forecast_3yr_by_year.index < max(forecast_3yr_by_year.index)]

    forecast_3yr_by_year = ForecastDataModel.convert_to_counter_index(forecast_3yr_by_year)

    print("did original model survive?")
    (is_valid, errMsg) = ForecastDataModel.is_valid_forecast_data_model(forecast_3yr_by_year)

    if(not is_valid):
        raise ValueError(errMsg)


    print("Yes\n\nDid the created model survive?")
    forecast_3yr_by_year_1 = ForecastDataModel.convert_to_counter_index(forecast_3yr_by_year_1)

    (is_valid, errMsg) = ForecastDataModel.is_valid_forecast_data_model(forecast_3yr_by_year_1)

    if(not is_valid):
        raise ValueError(errMsg)
    
    print("Yes\n")

    print(forecast_3yr_by_year_1)
    print()

    # test if attrbutes are retrievable
    dump_attr(forecast_3yr_by_year_1)
    print()

    # update the updateable variables
    print("Test recalculating outdated variables:")
    forecast_3yr_by_year_1 = ForecastDataModel.recalculate_forecast_data_model_attributes(forecast_3yr_by_year_1)

    # test if attrbutes are retrievable
    dump_attr(forecast_3yr_by_year_1)
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

    dump_attr(forecast_3yr_by_month)

    
    # Test converting a YEARLY forecast model to MONTHLY
    print("Test converting a YEARLY forecast model to MONTHLY")
    yearly_converted_to_monthly = ForecastDataModel.upsample_yearly_to_monthly(forecast_3yr_by_year)
    print(yearly_converted_to_monthly)
    print()

    dump_attr(forecast_3yr_by_month)


    # Test converting a MONTHLY forecast model to a YEARLY
    print("Test converting a MONTHLY forecast model to a YEARLY")
    monthly_converted_to_yearly = ForecastDataModel.downsample_monthly_to_yearly(yearly_converted_to_monthly)
    print(monthly_converted_to_yearly)
    print()

    dump_attr(monthly_converted_to_yearly)


    # TEST COMBINING DATA SETS
    # ------------------------

    # SINGLE COLUMN

    # Test combining two different data sets MONTHLY, no agg function
    print("Test combining two different data sets MONTHLY, no agg function")
    combined_monthly_forecast_model = ForecastDataModel.combine_forecast_models([yearly_converted_to_monthly, yearly_converted_to_monthly])
    print(combined_monthly_forecast_model)
    print()

    dump_attr(combined_monthly_forecast_model)


    # Test combining two different data sets MONTHLY, SUM agg function
    print("Test combining two different data sets MONTHLY, 'sum' agg function")
    combined_monthly_forecast_model = ForecastDataModel.combine_forecast_models([yearly_converted_to_monthly, yearly_converted_to_monthly], agg_col_funct="sum")
    print(combined_monthly_forecast_model)
    print()

    dump_attr(combined_monthly_forecast_model)


    # Test of combining two different data sets YEARLY, no agg function
    print("Test of combining two different data sets YEARLY, no agg function")
    combined_yearly_forecast_model = ForecastDataModel.combine_forecast_models([monthly_converted_to_yearly, monthly_converted_to_yearly])
    print(combined_yearly_forecast_model)
    print()

    dump_attr(combined_yearly_forecast_model)

   
    # Test of combining two different data sets YEARLY, SUM agg function
    print("Test of combining two different data sets YEARLY, 'sum' agg function")
    combined_yearly_forecast_model = ForecastDataModel.combine_forecast_models([monthly_converted_to_yearly, monthly_converted_to_yearly], agg_col_funct="sum")
    print(combined_yearly_forecast_model)
    print()

    dump_attr(combined_yearly_forecast_model)
    
    
    # DOUBLE COLUMN

    # Test combine MONTHLY, no agg function
    print("Test of combining two different data sets, multi-column, MONTHLY, no agg function")
    double_combined_monthly_forecast_model = ForecastDataModel.combine_forecast_models([combined_monthly_forecast_model, combined_monthly_forecast_model])
    print(double_combined_monthly_forecast_model)
    print()

    dump_attr(forecast_3yr_by_month)


    # Test combine MONTHLY, SUM agg function
    print("Test of combining two different data sets, multi-column, MONTHLY, 'sum' agg function")
    double_combined_monthly_forecast_model = ForecastDataModel.combine_forecast_models([combined_monthly_forecast_model, combined_monthly_forecast_model], agg_col_funct="sum")
    print(double_combined_monthly_forecast_model)
    print()

    dump_attr(forecast_3yr_by_month)

    
    # Test of combining two different data sets YEARLY, no agg function
    print("Test of combining two different data sets YEARLY multi-col")

    double_combined_yearly_forecast_model = ForecastDataModel.combine_forecast_models([combined_yearly_forecast_model, combined_yearly_forecast_model])
    print(double_combined_yearly_forecast_model)
    print()

    dump_attr(double_combined_monthly_forecast_model)

    
    # Test of combining two different data sets YEARLY, SUM agg function
    print("Test of combining two different data sets YEARLY multi-col")

    double_combined_yearly_forecast_model = ForecastDataModel.combine_forecast_models([combined_yearly_forecast_model, combined_yearly_forecast_model], agg_col_funct="sum")
    print(double_combined_yearly_forecast_model)
    print()

    dump_attr(double_combined_monthly_forecast_model)


if __name__ == "__main__":
    main()