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
from typing import List, Literal
import pandas as pd
from langflow.schema.dataframe import DataFrame
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel

# FORECAST SPECIFIC IMPORTS
# =========================


# COMPONENT SPECIFIC IMPORTS
# ==========================
import numpy as np



class ForecastFormModelUtilities():

    # TODO: Get rid of this function... fill_datframe can do everything it does and MORE
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
    def refill_drataframe(new_dim_rows: int, new_dim_cols: int, prev_data: DataFrame, default_value: float = ForecastDataModel.EDITABLE_VALUES_TOKEN, col_name_prefix: str = "col_", **kwargs) -> DataFrame:

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
            elif(new_dim_cols == prev_dim_cols):
                new_col_names = prev_data.columns.to_list()
            else:
                new_col_names = prev_data.columns.to_list()[:new_dim_cols]

            data = prev_data.reindex(index=list(range(new_dim_rows)), columns = new_col_names, fill_value=default_value)
            new_df = DataFrame(data=data)

        # one last step added, use kwargs to offer the ability to ovveride any existing column in the dataframe with
        # specific values as on option (this will generally cover the dates column which needs to be prefilled regardless
        # of resizing)
        df_keys = new_df.columns.to_list()

        for key, value in kwargs.items():
            if key in df_keys:
                new_df[key] = value

        # return the new dataframe
        return(new_df)
    

    
    # fill_col_names
    # Idiot proof function that will return the right names for columns if we are changing the number of columns in a dataframe.
    # Covers all the different scenarios from having no existing columns to align to, expanding an existing list, contracting, etc.
    # NOTE:  Auto column naming starts at "1", not at "0" like most pythong functions (so:  first name will be col_1, NOT col_0), but can be
    #        overriden by setting start_index_at = 0
    # 
    # INPUTS:
    #   new_dim_cols - number of columns in the new dataframe
    #   prev_col_names (optional) - the previous list of column names to change
    #   col_name_prefix (optional) - the prefix to use for a new column number
    #   num_static_cols (optional) - to help with the automatic column name generation, determine the how many columns have non-auto-generated names (we discount those when determining the start number)
    #   start_num (optional) - the starting number for any newly generated colum names.  Iff set to -1 (default), function with automatically set it
    #   start_index_at (optional) - set to 0 or 1 (default is 1), determines if column numbers generated are zero indexed or 1-indexed (i.e. col_0, col_1, col_2 vs. col_1, col_2, col_3)
    #
    # OUTPUTS:
    #   new_col_names - a list of new column names to use
    #
    # TODO: Integrate this function into the previous function refill_dataframe, so that we don't have TWO functions that calculate the number of columns

    @staticmethod
    def fill_col_names(new_dim_cols: int, 
                       prev_col_names: list| pd.Index = None, 
                       col_name_prefix : str = "col_", 
                       num_static_cols : int = 0, 
                       start_num : int = -1, 
                       start_index_at: Literal[0, 1] = 1) -> List[str]:
        # first, handle the case the the new dimensions are not legitimate, if so, throw an error
        if(new_dim_cols < 1):
            raise ValueError(f"* fill_columns error:  invalid dimensions for number of columns: {new_dim_cols}\n")

        # second, handle no previous column names, generate a sequntially rising prefix+number set of columns
        if(prev_col_names is None):
            if(start_num == -1):
                new_col_names = list(map(lambda item: f"{col_name_prefix}{item}", list(range(start_index_at, new_dim_cols))))
            else:
                new_col_names = list(map(lambda item: f"{col_name_prefix}{item}", list(range(start_num, start_num + new_dim_cols))))
        
        # third, handle a situation where the number of columns is CHANGING
        else:
            # courtesy check if it's a pandas Index, covert to a list (common error, so let's handle it in the function)
            if isinstance(prev_col_names, pd.Index):
                prev_col_names = list(prev_col_names)

            # get the length for number of rows
            prev_dim_cols = len(prev_col_names)

            # handle if we need to expand col names
            if(new_dim_cols > prev_dim_cols):
                num_cols_to_add = new_dim_cols - prev_dim_cols

                if(start_num == -1):
                    set_start_num = len(prev_col_names) - num_static_cols + start_index_at
                else:
                    set_start_num = start_num
                    
                new_col_names = prev_col_names + list(map(lambda item: f"{col_name_prefix}{item}", list(range(set_start_num, set_start_num + num_cols_to_add))))

            # handle if no change needed to number of columns
            elif(new_dim_cols == prev_dim_cols):
                new_col_names = prev_col_names

            # handle if we want to contract number of columns
            else:
                new_col_names = prev_col_names[:new_dim_cols]

        return(new_col_names)
    

    
    # fill_drataframe
    # This function is used to generate the correct dataframe (usually for TableInputs in components), either from scratch or given existing date.
    # Since it is meant to greatly reduce the amount of "boilerplate" code used in the individual components, it's a bit "over-featured" and has 
    # a lot of arguments and options (and the supporting lines of business logic), which developers of components can use to generate exactly
    # what they are looking for.  The theory here is that it's better to over-build a single function, and in doing so centralize all the business
    # logic in one place to catch bugs, etc... than to distrbute pieces of that logic all over the various components.
    # 
    # INPUTS:
    #   new_dim_rows - Number of rows in the new dataframe, 
    #   new_dim_cols - Number of columns in the new dataframe,
    #   set_col_names (optional) - FOR NEW ONLY: option to specify some or all of the column names (DO NOT USE WHEN PROVIDING 'prev_data')  
    #   prev_data (optional) - Provide an existing dataframe to resize into the new dimensions (DO NOT USE WHEN PROVIDING 'set_col_names')
    #   default_col_value (optional) - Default value to use for all new cells generated as part of sizing up to higher dims
    #   individual_default_col_values (optional) - Default values to use for specific column names (provided as a dict of (column_name, default_value) pairs 
    #   col_name_prefix (optional) - When generating new column names, the prefix to use
    #   num_static_cols (optional) - When creating auto-generated column names, how many columns in to start counting as "_1" (so if first two columns are not auto-generated, then the third column should start with "_1")
    #   start_num (optional) - What should the starting number be for auto-generated column names 
    #   start_index_at (optional) - ??? 
    #   **kwargs (optional) - Provide a column_name = [all column values], if we want to ovveride the values in any columns (usually because the default value for the cells requires more complexity than a single value)
    #
    # OUTPUTS:
    #   new_dataframe - a new dataframe which matches the new_dim_rows and new_dim_cols, but includes as much
    #                   of the prev_data as makes since
    @staticmethod
    def fill_drataframe(new_dim_rows: int, 
                        new_dim_cols: int,
                        set_col_names: list = None,  
                        prev_data: DataFrame | List[dict] | None = None, 
                        default_col_value: float = ForecastDataModel.EDITABLE_VALUES_TOKEN, 
                        individual_default_col_values: dict = None, 
                        col_name_prefix: str = "col_", 
                        num_static_cols: int = 1, 
                        start_num: int = -1, 
                        start_index_at: Literal[0, 1] = 1, 
                        **kwargs) -> DataFrame:
        
        # can't use set_col_names AND have prev_data, so throw error if both are not None
        if(set_col_names is not None and prev_data is not None):
            raise ValueError(f"* refill_dataframe error:  cannot have values for argument 'set_col_names' and 'prev_data' at the same time.  'set_col_names' should only be used to determine the column names when you are not passing in existing data.")

        
        # first, handle the case the the new dimensions are not legitimate, that would only be a case with no columns
        # if so, throw an error
        if(new_dim_cols < 1):
            raise ValueError(f"* refill_dataframe error:  invalid row dimension for new dataframe rows: {new_dim_rows}, cols: {new_dim_cols}")

        
        # second, handle the case where there is no existing data
        if(prev_data is None):
            # create column names when we DON'T have existing data
            col_names = ForecastFormModelUtilities.fill_col_names(new_dim_cols = new_dim_cols,
                                                                  prev_col_names = set_col_names,
                                                                  col_name_prefix = col_name_prefix,
                                                                  num_static_cols = num_static_cols,
                                                                  start_num = start_num,
                                                                  start_index_at = start_index_at)
            
            # if there are no new rows to create, simply create a DataFrame just with the columns
            if(new_dim_rows < 1):
                new_df = DataFrame(data = pd.DataFrame(columns = col_names))

            # Otherwise, we need to create some blank rows, following the information provided 
            # by the default values and individual column default values
            else:
                if(individual_default_col_values is None):
                    empty_row = [default_col_value] * len(col_names)
                else:        
                    empty_row = [default_col_value if(col_name not in individual_default_col_values.keys()) else individual_default_col_values[col_name] for col_name in col_names]

                data = [empty_row] * new_dim_rows
                new_df = DataFrame(data = pd.DataFrame(data = data, columns = col_names))

        
        # third, handle the case where there IS existing data
        else:
            # if there is existing data, we have to do some set-up first

            # convert prev_data to DataFrame is it isn't one already
            # for easier handling than list of dicts
            if not isinstance(prev_data, DataFrame):
                old_df = DataFrame(data = prev_data)
            else:
                old_df = prev_data

            # determine the dimensions of the existing data, we will use later to see
            # if resizing is required
            old_dim_cols = len(old_df.columns)
            old_dim_rows = len(old_df)

            # if the new and old dimesions are the same, then just return the existing dataframe
            if(old_dim_cols == new_dim_cols and old_dim_rows == new_dim_rows):
                new_df = old_df

            # Otherwise, we need to handle the resizing and generation of new column names
            else:
                # create column names when we DO have existing data
                col_names = ForecastFormModelUtilities.fill_col_names(new_dim_cols = new_dim_cols,
                                                                      prev_col_names = old_df.columns,
                                                                      col_name_prefix = col_name_prefix,
                                                                      num_static_cols = num_static_cols,
                                                                      start_num = start_num,
                                                                      start_index_at = start_index_at)
            
                # Resize the existing old_df into the dimensions of the new_df
                # we do this using pandas 'reindex' feature which will automatically put NaN into any new cells that
                # get generated (or NaT for date columns).
                # use reindex to resize the old_values to the new values
                new_df = old_df.reindex(index=list(range(new_dim_rows)), columns = col_names)

                # iterate over all the individually assigned default colums and fill
                # in their default values
                for col_name in new_df.columns:
                    if((individual_default_col_values is not None) and (col_name in individual_default_col_values.keys())):
                        new_df[col_name] = new_df[col_name].fillna(individual_default_col_values[col_name])
                    else:
                        if(default_col_value is not pd.NA):
                            new_df[col_name] = new_df[col_name].fillna(default_col_value)

        
        # ONE LAST STEP
        # Regardless of resizing or not, the function offers a column value ovverride feature which allows for column name/values
        # to be provided in kwargs and used to override what is in the current column of the same name, this is done as the last step
        # before returning the DataFrame back
        df_keys = new_df.columns.to_list()

        # iterate overall the the key/values provided in kwargs to use as column value overrides
        for key, value in kwargs.items():
            # the key provided in kwargs corresponds to a column name in the dataframe, otherwise go to the next one
            # NOTE:  we don't throw an error if there is no key, should make this simpler to use with the variable DataFrame
            if key in df_keys:
                # check that the number of rows provided, is the same size as the dataframe's
                if(len(value) != new_dim_rows):
                    raise ValueError(f"* fill_datafame:  cannot ovveride column '{key}' with values provided, number of rows mismatch received: {len(value)}, expected: {new_dim_rows}")
                else:
                    new_df[key] = value

        # return the new dataframe
        return(new_df)

