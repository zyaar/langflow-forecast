#from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.constants import ForecastModelTimescale
from langflow.components.forecasting.common.date_utils import gen_dates

def main():
    # test gen_dates
    start_year = 2026
    forecast_period = 1

    date_series = gen_dates(start_year, num_years=forecast_period)
    print(date_series)

    date_series = gen_dates(start_year, num_years=forecast_period, time_scale=ForecastModelTimescale.MONTH)
    print(date_series)

if __name__ == "__main__":
    main()