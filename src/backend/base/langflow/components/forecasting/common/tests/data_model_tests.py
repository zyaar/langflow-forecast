from langflow.components.forecasting.common.constants import ForecatModelTimescale
from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel

def main():
    # test gen_dates
    start_year = 2026
    forecast_period = 5

    # def __init__(
    #         self,
    #         data: list[dict] | list[Data] | pd.DataFrame | None = None,
    #         text_key: str = "text",
    #         default_value: str = "",
    #         start_year: int = None,
    #         num_years: int = None,
    #         input_type: ForecastModelInputTypes = None,
    #         start_month: int = FORECAST_COMMON_MONTH_NAMES_AND_VALUES["January"], # default to January start date
    #         timescale: ForecatModelTimescale = None,
    #         **kwargs,
    #         ):

    # Test YEARLY
    dummy_patient_series_yearly = [1000] * forecast_period
    print(dummy_patient_series_yearly)

    new_forecast = ForecastDataModel(start_year=start_year, num_years=forecast_period, data={"epi1": dummy_patient_series_yearly})
    print(new_forecast)

    # Test MONTHLY
    dummy_patient_series_monthly = [1000] * (forecast_period*12)
    print(dummy_patient_series_monthly)

    new_forecast = ForecastDataModel(start_year=start_year, num_years=forecast_period, timescale=ForecatModelTimescale.MONTH, data={"epi1": dummy_patient_series_monthly})
    print(new_forecast)

    # Test creating a database using the reserved column name
    new_forecast = ForecastDataModel(start_year=start_year, num_years=forecast_period, timescale=ForecatModelTimescale.MONTH, data={ForecastDataModel.RESERVED_COLUMN_INDEX_NAME: dummy_patient_series_monthly})

    # date_series = gen_dates(start_year, num_years=forecast_period)
    # print(date_series)

    # date_series = gen_dates(start_year, num_years=forecast_period, timescale=ForecatModelTimescale.MONTH)
    # print(date_series)

if __name__ == "__main__":
    main()