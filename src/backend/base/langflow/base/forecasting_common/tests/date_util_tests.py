from langflow.base.forecasting_common.models.date_utils import gen_dates, conv_dates_monthly_to_yearly, conv_dates_yearly_to_monthly
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
print(f"test1: {test3}")

print("\nGenerate 3 year, monthly, end of fiscal year dates, starting in 2026, fiscal start in April")
test4 = gen_dates(start_year = 2026, num_years = 3, start_month=4, time_scale = ForecastModelTimescale.MONTH)
print(f"test2: {test4}")


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



