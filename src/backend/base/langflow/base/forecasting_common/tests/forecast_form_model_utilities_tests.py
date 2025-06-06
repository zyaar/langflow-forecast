import pandas as pd

from langflow.schema.dataframe import DataFrame

from langflow.base.forecasting_common.constants import FORECAST_MIN_FORECAST_START_YEAR, FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities
from langflow.base.forecasting_common.models.date_utils import gen_dates


df = pd.DataFrame({"col1" : [1,2,3], "col2" : [4,5,6], "col3" : [7,8,9]})
print(df)
# new_columns = df.columns.to_list() + ['col4', 'col5', 'col6']
# #new_columns = df.columns.to_list()[:2] # note that this slice INCLUDES THE MIN, BUT NOT THE MAX
# print(new_columns)

# df2 = df.reindex(columns = new_columns, fill_value=0)
# print(df2)

# df3 = df.reindex(list(range(9)), fill_value=0)
# print(df3)

# df4 = df.copy().reindex(index=list(range(9)), columns = new_columns, fill_value=0)
# print(df4)
                 
# exit()

# # refill_drataframe(self, new_dim_rows: int, new_dim_cols: int, prev_data: DataFrame, default_value: float = pd.NA, col_name_prefix = "col_")
# print(f"\n\nrefill_dataframe:  dataframe is empty, new dataframe has dims 5 rows, 10 cols, no default value, no index")
# results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=5, new_dim_cols=10, prev_data=None)
# print(results)

# print(f"\n\nrefill_dataframe:  dataframe is empty, new dataframe has dims 5 rows, 10 cols, default_value=0, no index")
# results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=5, new_dim_cols=10, prev_data=None, default_value=0)
# print(results)

# print(f"\n\nrefill_dataframe:  dataframe is empty, new dataframe has dims 5 rows, 10 cols, default_value=0, col_name_prefix='segment_', no index")
# results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=5, new_dim_cols=10, prev_data=None, default_value=0.0, col_name_prefix="segment_")
# print(results)

# print(f"\n\nrefill_dataframe:  now put an old dataframe (the 3 rows,3 cols generated in the prev step), into a dataframe which is LARGER in both rows and cols (10 rows, 20 cols)")
# new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=10, new_dim_cols=20, prev_data=df, default_value=0.0, col_name_prefix="segment_")
# print(new_results)

# print(f"\n\nrefill_dataframe:  now put an old dataframe (the 3 rows,3 cols generated in the prev step), into a dataframe which is SAME SIZE in both rows and cols (3 rows, 3 cols)")
# new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=3, new_dim_cols=3, prev_data=df, default_value=0.0, col_name_prefix="segment_")
# print(new_results)


df = pd.DataFrame({"col1" : [1,2,3], "col2" : [4,5,6], "col3" : [7,8,9]})
print(df)

for num_rows in [2, 3, 5, 10]:
    for num_cols in [2, 3, 5, 20]:
        print(f"\n\nresize from (3 rows, 3 cols) to ({num_rows}, {num_cols})")
        new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=num_rows, new_dim_cols=num_cols, prev_data=df, default_value=0.0, col_name_prefix="segment_")
        print(new_results)


print("Test the edge case of adding just one more column, does it work?")
print(df)
old_dim_rows = 3
old_dim_cols = 3
new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=old_dim_rows, new_dim_cols=old_dim_rows+1, prev_data=df, default_value=0.0, col_name_prefix="segment_")
print(new_results)

