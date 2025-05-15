#from langflow.components.forecasting.common.data_model.forecast_data_model import ForecastDataModel
from langflow.components.forecasting.common.constants import ForecatModelTimescale
from langflow.components.forecasting.common.date_utils import gen_dates

def main():
    # test gen_dates
    start_year = 2026
    forecast_period = 5

    date_series = gen_dates(start_year, num_years=forecast_period)
    print(date_series)

    date_series = gen_dates(start_year, num_years=forecast_period, timescale=ForecatModelTimescale.MONTH)
    print(date_series)

if __name__ == "__main__":
    main()