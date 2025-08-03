from langflow.base.forecasting_common.models.date_utils import gen_dates, gen_pre_dates, conv_dates_monthly_to_yearly, conv_dates_yearly_to_monthly
from langflow.base.forecasting_common.constants import ForecastModelTimescale


print("gen_dates")
print("---------")

# yearly
print("\nGenerate 3 year, yearly, end of calendar year dates, starting in 2026")
test1 = gen_dates(start_year = 2026, num_years = 3)
print(f"test1: {test1}")

print("\nGenerate 3 year, yearly, end of fiscal year dates, starting in 2026, fiscal start in April")
test2 = gen_dates(start_year = 2026, num_years = 3, start_month=4)
print(f"test2: {test2}")

# monthly
print("\nGenerate 3 year, monthly, end of calendar year dates, starting in 2026")
test3 = gen_dates(start_year = 2026, num_years = 3, time_scale = ForecastModelTimescale.MONTH)
print(f"test3: {test3}")

print("\nGenerate 3 year, monthly, end of fiscal year dates, starting in 2026, fiscal start in April")
test4 = gen_dates(start_year = 2026, num_years = 3, start_month=4, time_scale = ForecastModelTimescale.MONTH)
print(f"test4: {test4}")


print("\n\ngen_pre_dates")
print("-------------")

print("\nGenerate 3 pre_dates, YEARLY timescale")
test5 = gen_pre_dates(first_forecast_date = test1[0], num_periods = 3, time_scale = ForecastModelTimescale.YEAR)
print(f"test5 (from test1)= {test5}")


print("\nGenerate 6 pre_dates, MONTHLY timescale")
test6 = gen_pre_dates(first_forecast_date = test4[0], num_periods = 6, time_scale = ForecastModelTimescale.MONTH)
print(f"test6 (from test4)= {test6}")


print("\n\nconv_dates_monthly_to_yearly")
print("----------------------------")

# monthly_to_yearly
print("\nConvert 3 year, monthly, end of calendar year dates, starting in 2026 to YEARLY")
test10 = conv_dates_monthly_to_yearly(data = test3)
print(f"FROM test3: {test3}")
print(f"TO  test10: {test10}")

print("\nConvert 3 year, monthly, end of fiscal year dates, starting in 2026, fiscal start in April to YEARLY")
test11 = conv_dates_monthly_to_yearly(data = test4)
print(f"FROM test4: {test4}")
print(f"TO  test11: {test11}")



print("\n\nconv_dates_yearly_to_monthly")
print("----------------------------")

# yearly_to_monthly
print("\nConvert 3 year, yearly, end of calendar year dates, starting in 2026 to MONTHLY")
test20 = conv_dates_yearly_to_monthly(data = test1)
print(f"FROM test1: {test1}")
print(f"TO  test20: {test20}")

print("\nConvert 3 year, yearly, end of fiscal year dates, starting in 2026, fiscal start in April to MONTHLY")
test21 = conv_dates_yearly_to_monthly(data = test2)
print(f"FROM test2: {test2}")
print(f"TO  test21: {test21}")



