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

# # test refill_dataframe
# df = pd.DataFrame({"col1" : [1,2,3], "col2" : [4,5,6], "col3" : [7,8,9]})
# print(df)

# for num_rows in [2, 3, 5, 10]:
#     for num_cols in [2, 3, 5, 20]:
#         print(f"\n\nresize from (3 rows, 3 cols) to ({num_rows}, {num_cols})")
#         new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=num_rows, new_dim_cols=num_cols, prev_data=df, default_value=0.0, col_name_prefix="segment_")
#         print(new_results)


# print("Test the edge case of adding just one more column, does it work?")
# print(df)
# old_dim_rows = 3
# old_dim_cols = 3
# new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=old_dim_rows, new_dim_cols=old_dim_rows+1, prev_data=df, default_value=0.0, col_name_prefix="segment_")
# print(new_results)


# test fill_columns

# 
print("\n\nTest starting with no previous columns, generate 5 columns")
new_col_names1 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=5)
print(f"None -> {new_col_names1}")

# using start_num
print("\n\nTest starting with no previous columns, generate 5 columns, start at 5")
new_col_names2 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=5, start_num=5)
print(f"None -> {new_col_names2}")

# using previous_columns
print("\n\nTest starting with previous columns, generate 5 columns")
new_col_names10 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=10, prev_col_names=new_col_names1)
print(f"{new_col_names1} -> {new_col_names10}")

print("\n\nTest starting with previous columns, generate 5 new columns, start at 20")
new_col_names20 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=10, prev_col_names=new_col_names2, start_num=20)
print(f"{new_col_names2} -> {new_col_names20}")

print("\n\nTest starting with previous columns, reducing size by 5")
new_col_names100 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(new_col_names10)-5, prev_col_names=new_col_names10)
print(f"{new_col_names10} -> {new_col_names100}")

print("\n\nTest starting with previous columns, increasing by 1")
prev_values = new_col_names100
new_col_names100 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(new_col_names100)+1, prev_col_names=new_col_names100)
print(f"{prev_values} -> {new_col_names100}")

print("\n\nTest starting with previous columns, decrease by 4")
prev_values = new_col_names100
new_col_names100 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(new_col_names100)-4, prev_col_names=new_col_names100)
print(f"{prev_values} -> {new_col_names100}")

print("\n\nTest starting with previous columns, increasing size by 10")
prev_values = new_col_names100
new_col_names100 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(new_col_names100)+10, prev_col_names=new_col_names100)
print(f"{prev_values} -> {new_col_names100}")


print("\n\nTEST NUM_STATIC_COLS FEATURE")
print("=============")

print("\n\nCreate a list of columns which start after the two static names are provided")
test_static_1 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=10, prev_col_names=["date", "progression_cuve"], num_static_cols=2)
print(f"None -> {test_static_1}")

print("\n\nAdd ONE auto-generated column")
old_vals = test_static_1
test_static_2 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(old_vals)+1, prev_col_names=old_vals, num_static_cols=2)
print(f"{old_vals} -> {test_static_2}")

print("\n\nSubtract FOUR auto-generated column")
old_vals = test_static_2
test_static_3 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(old_vals)-4, prev_col_names=old_vals, num_static_cols=2)
print(f"{old_vals} -> {test_static_3}")

print("\n\nAdd TWO auto-generated column")
old_vals = test_static_3
test_static_4 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(old_vals)+2, prev_col_names=old_vals, num_static_cols=2)
print(f"{old_vals} -> {test_static_4}")



print("\n\nTEST START_INDEX_AT FEATURE")
print("=============")

print("\n\nCreate a list of columns which start after the two static names are provided, with start_index_at = 0")
test_static_1 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=10, prev_col_names=["date", "progression_cuve"], num_static_cols=2, start_index_at=0)
print(f"None -> {test_static_1}")

print("\n\nAdd ONE auto-generated column, with start_index_at = 0")
old_vals = test_static_1
test_static_2 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(old_vals)+1, prev_col_names=old_vals, num_static_cols=2, start_index_at=0)
print(f"{old_vals} -> {test_static_2}")

print("\n\nSubtract FOUR auto-generated column, with start_index_at = 0")
old_vals = test_static_2
test_static_3 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(old_vals)-4, prev_col_names=old_vals, num_static_cols=2, start_index_at=0)
print(f"{old_vals} -> {test_static_3}")

print("\n\nAdd TWO auto-generated column, with start_index_at = 0")
old_vals = test_static_3
test_static_4 = ForecastFormModelUtilities().fill_col_names(new_dim_cols=len(old_vals)+2, prev_col_names=old_vals, num_static_cols=2, start_index_at=0)
print(f"{old_vals} -> {test_static_4}")




print("\n\nTEST FILL_DATAFRAME")
print("=============")

import pandas as pd
from datetime import datetime

# print("\n\nStart by creating a new dataframe")
# new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 4,
#                                                      new_dim_cols = 5,
#                                                      set_col_names = list = None,  
#                                                      prev_data =  None, 
#                                                      default_col_value = pd.NA, 
#                                                      individual_default_col_values = None, 
#                                                      col_name_prefix = "col_", 
#                                                      num_static_cols = 1, 
#                                                      start_num = -1, 
#                                                      start_index_at = 1)







print("\n\nStart by creating a new dataframe")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 4,
                                                     new_dim_cols = 5,
                                                     default_col_value = pd.NA, 
                                                     num_static_cols = 0,)
print(new_df1)


print("\n\nCreate same dataframe, but now with a 'dates' column, also test num_static_cols = 0")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 4,
                                                     new_dim_cols = 5,
                                                     set_col_names = ["dates"],  
                                                     default_col_value = pd.NA, 
                                                     num_static_cols = 0)
print(new_df1)

print("\n\nCreate same dataframe, repeat the 'dates' column, also test num_static_cols = 0, and now add 0's indexing (i.e. start_index_at=0)")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 4,
                                                     new_dim_cols = 5,
                                                     set_col_names = ["dates"],  
                                                     default_col_value = pd.NA, 
                                                     num_static_cols = 0,
                                                     start_index_at=0)
print(new_df1)


print("\n\nCreate same dataframe, now prepopulate the 'dates' column")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 4,
                                                     new_dim_cols = 5,
                                                     set_col_names = ["dates"],  
                                                     prev_data =  None, 
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = None, 
                                                     col_name_prefix = "col_", 
                                                     num_static_cols = 1, 
                                                     start_num = -1, 
                                                     start_index_at = 1,
                                                     dates = pd.date_range(datetime.today(), periods=4).tolist())

print(new_df1)


print("\n\nCreate 6 rows by 2 cols dataframe, now prepopulate the 'month' as linearly increasing and 'patient_progression' as decreasing")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 6,
                                                     new_dim_cols = 2,
                                                     set_col_names = ["month", "patient_progression"],  
                                                     prev_data =  None, 
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,7)))

print(new_df1)


print("\n\nWith above dataframe, add a product")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 6,
                                                     new_dim_cols = 3,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,7)))

print(new_df1)


print("\n\nWith above dataframe, add a row")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 7,
                                                     new_dim_cols = 3,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,8)))

print(new_df1)


print("\n\nWith above dataframe, add another row but don't provide any values")
new_df2 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 8,
                                                     new_dim_cols = 3,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2)

print(new_df2)



print("\n\nGO BACK to dataframe TWO ROWS above, add another row and another treatment group")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 8,
                                                     new_dim_cols = 4,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,9)))

print(new_df1)


print("\n\nWith dataframe above, remove one col")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 8,
                                                     new_dim_cols = 3,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,9)))

print(new_df1)


print("\n\nWith dataframe above, remove last col")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 8,
                                                     new_dim_cols = 2,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,9)))

print(new_df1)


print("\n\nWith dataframe above, add 4 back in")
new_df1 = ForecastFormModelUtilities.fill_drataframe(new_dim_rows= 8,
                                                     new_dim_cols = 6,
                                                     prev_data =  new_df1,
                                                     default_col_value = pd.NA, 
                                                     individual_default_col_values = {"patient_progression": 1}, 
                                                     col_name_prefix = "treatment_", 
                                                     num_static_cols = 2, 
                                                     month = list(range(1,9)))

print(new_df1)






# df = pd.DataFrame({"col1" : [1,2,3], "col2" : [4,5,6], "col3" : [7,8,9]})
# print(df)

# for num_rows in [2, 3, 5, 10]:
#     for num_cols in [2, 3, 5, 20]:
#         print(f"\n\nresize from (3 rows, 3 cols) to ({num_rows}, {num_cols})")
#         new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=num_rows, new_dim_cols=num_cols, prev_data=df, default_value=0.0, col_name_prefix="segment_")
#         print(new_results)


# print("Test the edge case of adding just one more column, does it work?")
# print(df)
# old_dim_rows = 3
# old_dim_cols = 3
# new_results = ForecastFormModelUtilities().refill_drataframe(new_dim_rows=old_dim_rows, new_dim_cols=old_dim_rows+1, prev_data=df, default_value=0.0, col_name_prefix="segment_")
# print(new_results)



    # def fill_drataframe(new_dim_rows: int, 
    #                     new_dim_cols: int,
    #                     set_col_names: list = None,  
    #                     prev_data: DataFrame | List[dict] | None = None, 
    #                     default_col_value: float = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
    #                     individual_default_col_values: dict = None, 
    #                     col_name_prefix: str = "col_", 
    #                     num_static_cols: int = 1, 
    #                     start_num: int = -1, 
    #                     start_index_at: Literal[0, 1] = 1, 
    #                     **kwargs) -> DataFrame:
