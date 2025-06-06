###################################################
# forecast_form_model_utilities
# 
# A bunch of helper static methods to facilitate working with ForecastDataModel DataFrames
# within the forms structure of langflow.
#
###################################################


# COMMON IMPORTS
# ==============
from enum import Enum
import pandas as pd
from langflow.schema.dataframe import DataFrame
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel



class ForecastFormModelUtilities():

    # refill_drataframe
    # Given that a dataframe field has been flagged to be updated, and there is previous data in that dataframe, figure 
    # out how to "stuff" the previous data back into the updated dataframe, so that users don't have to continously re-type data
    # 
    # INPUTS:
    #   new_dim_rows - number of rows in the new dataframe
    #   new_dim_cols - number of columns in the new dataframe
    #   prev_data - the data from the previous dimension dataframe
    #
    # OUTPUTS:
    #   new_dataframe - a new dataframe which matches the new_dim_rows and new_dim_cols, but includes as much
    #                   of the prev_data as makes since
    @staticmethod
    def refill_drataframe(new_dim_rows: int, new_dim_cols: int, prev_data: DataFrame, default_value: float = ForecastDataModel.EDITABLE_VALUES_TOKEN, col_name_prefix = "col_", **kwargs):

        # first, handle the case the the new dimensions are not legitimate, if so, throw an error
        if(new_dim_rows < 1 or new_dim_cols < 1):
            raise ValueError(f"* refill_dataframe error:  invalid dimensions for new dataframe rows: {new_dim_rows}, cols: {new_dim_cols}")
        
        # get dimensions of the prev_data
        if(prev_data is None):
            prev_dim_rows = 0
            prev_dim_cols = 0
        else:
            prev_dim_rows = len(prev_data.index)
            prev_dim_cols = len(prev_data.keys())

        # second, handle the case that the previous dimensions are not legitimate, if so, simply return a blank dataset
        if(prev_dim_rows < 1 or prev_dim_cols < 1):
            data = {}
            one_col_default_values = [default_value] * new_dim_rows

            for i in range(new_dim_cols):
                data[f"{col_name_prefix}{i}"] = one_col_default_values
            
            new_df = DataFrame(data=data)
        
        # third, handle the case that the the dimensions (old and new) are the same, in which case, return the
        # previous dataset
        elif(new_dim_rows == prev_dim_rows and new_dim_cols == prev_dim_cols):
            new_df = prev_data

        # finally, handle all the cases where there's a mismatch in the dimensions betwee new and old
        else:
            if(new_dim_cols > prev_dim_cols):
                new_col_names = prev_data.columns.to_list() + list(map(lambda item: f"{col_name_prefix}{item}", list(range((prev_dim_cols), new_dim_cols))))
                #new_col_names = prev_data.columns.to_list() + list(map(lambda item: col_name_prefix + str(item+1), list(range((prev_dim_cols), new_dim_cols))))
            elif(new_dim_cols == prev_dim_cols):
                new_col_names = prev_data.columns.to_list()
            else:
                new_col_names = prev_data.columns.to_list()[:new_dim_cols]

            print(new_col_names)

            data = prev_data.reindex(index=list(range(new_dim_rows)), columns = new_col_names, fill_value=default_value)
            new_df = DataFrame(data=data)
            print(new_df)

        # one last step added, use kwargs to offer the ability to ovveride any existing column in the dataframe with
        # specific values as on option (this will generally cover the dates column which needs to be prefilled regardless
        # of resizing)
        df_keys = new_df.columns.to_list()

        for key, value in kwargs.items():
            if key in df_keys:
                new_df[key] = value

        # return the new dataframe
        return(new_df)

        


