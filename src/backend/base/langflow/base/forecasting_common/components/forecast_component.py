#####################################################################
# forecast_component.py
#
# Abstract class that handles common boilerplate for all forecast components
# 
#
#####################################################################

from langflow.custom import Component
from langflow.io import TableInput, IntInput, StrInput, NestedDictInput
from langflow.schema import DataFrame, Data
from langflow.schema.table import Column
from langflow.template import Output

# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities

from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)

from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket
from langflow.base.forecasting_common.controllers.forecast_controller import ForecastController


# COMPONENT SPECIFIC IMPORTS
# ==========================
from typing import List, Dict, Any
import pandas as pd
import copy


# CONSTANTS
# =========



# CLASSES
# =======

# ForecastComponent
# This abstract class provides common functionality for all forecasting components, including a mechanism
# to ensure that the key ForecastDataModel shared variables are always appending to the input
class ForecastComponent(Component):

    # CONFIG CONSTANTS
    # ================
    
    # COMPONENT INFO
    display_name: str = f"Forecast Component TB"
    description: str = f"Abstract base class for components, the basis for all other Forecast Components."
    icon: str = f""
    name: str = f"ForecastComponentTB"

    # MISC CONFIG
    DEBUG_MODE = True
    COMM = "React_Conf"




    # INSTANCE ATTRIBUTES
    # generated during the __init__
    # -----------------------------
    # inputs = None   # (list) InputTypes for the component
    # outputs = None  # (list) OutputTypes for the component
    # controller 


    # INIT
    # ====
    def __init__(self, **kwargs) -> None:
        # set-up a controller if needed
        if not hasattr(self, "controller"):
            self.controller = ForecastController()

        # generates some instance variables instead of using class variables, this allows us to customize
        # this instance variables in the children of this abstract class without having to rewrite all the

        # set-up inputs and outputs with the child class's configuration variables
        self.inputs = self._gen_inputs()
        self.outputs = self._gen_outputs()

        super().__init__(**kwargs)
    


    # GENERATE INPUTS / OUTPUTS
    # -------------------------
    def _gen_inputs(self) -> list:
        inputs_list = [
            
        # hidden field which holds the current configuration of the outputs
            NestedDictInput(
                name="output_config",
                required = False,
                dynamic = True,
                real_time_refresh = True,
                advanced = True,
                is_list = True,
                value = {},
            ),

        # Number of Years in Forecast
            StrInput(
                name="display_name2",
                display_name="Name",
                info="The name of this component",
                placeholder = self.display_name,
                required=False,
                dynamic = False,
                real_time_refresh = False,
            ),

            
            # Number of Years in Forecast
            StrInput(
                name="num_years",
                display_name="# of Years to Forecast",
                info="The number of years to include in the forecast.",
                required=True,
                dynamic = True,
                real_time_refresh = True,
                advanced=True,
            ),

            # Start Year
            StrInput(
                name="start_year",
                display_name="Start Year",
                info="The first year to forecast.  This can be a year value (i.e. 2026) or any integer (i.e. 1).  The system will simply use it as a reference point and add +1 for each year until it reaches the number of years to forecast.",
                required=True,
                dynamic = True,
                real_time_refresh = True,
                advanced=True,
            ),

            # Time Scale
            StrInput(
                name = "timescale",
                display_name = "Time-Scale",
                info = "The granularity of the time scale for the forecast.",
                required = True,
                dynamic = True,
                real_time_refresh = True,
                advanced=True,
            ),

            # Month Start of Fiscal Year
            StrInput(
                name="start_month",
                display_name="Month Start of Fiscal Year",
                info="For fiscal years which do not start in January, allows you the option of specifying the start month.",
                required = True,
                show = True,
                dynamic = True,
                real_time_refresh = True,
                advanced=True,
            ),

            # Input Type
            StrInput(
                name = "input_type",
                display_name = "Input Type",
                info = "Determines the type of forecast to generate.  'Time Based Input' generates which allows for individual values to be entered at the time-scale chosen.  'Single Input' uses a base value and growth/shrink rate at the time-scale chosen.",
                required = True,
                show = True,
                dynamic = True,
                real_time_refresh = True,
                advanced=True,
            ),

        ]

        return(inputs_list)
    

    def _gen_outputs(self) -> list:
        outputs_list = []

        return(outputs_list)
    

    # INPUT/OUTPUT VALIDATIONS
    # ========================
    def validate_inputs(self):
        pass

    def validate_outputs(self):
        pass


    # INPUT/OUTPUTS CALCULATIONS
    # ==========================

    def _forecast_model_common_input(self):
        self.validate_inputs()


    def _forecast_model_common_output(self, data: DataFrame | pd.DataFrame, meta_data: ForecastMetaDataFrame, check_ids: bool = True) -> Data:
        self.validate_outputs()

        last_id = None

        # validation of data
        if(data is not None):
            # convert pd.DataFrame to DataFrame
            if isinstance(data, pd.DataFrame):
                data = DataFrame(data)

            # must have at least a DATE AND another column
            if(len(data.columns) < 2):
                raise ValueError(f"\n*  _forecast_model_common_output:  data is missing min columns: '{data.columns}'")
            
        # get the last_id to the last_value_id
        last_value_id = meta_data.get_last_value_id()

        if(last_value_id not in data.columns):
            raise ValueError(f"\n:  _forecast_model_common_output:  invalid, last id in ForecastMetaDataFrame '{last_value_id}' not found in DataFrame '{data.columns}'.")


        return(ForecastDataPacket.gen_data_packet(dataframe = data, 
                                                  meta_data = meta_data, 
                                                  last_id = last_value_id))


    # COMPONENT META-DATA
    # -------------------


    # COMPONENT OUTPUTS
    # -----------------


    # COMPONENT FORM UPDATE RULES
    # ---------------------------

    # UPDATE_BUILD_CONFIG
    # Updates real_time_refreshing fields whenever an update happens from a dynamic field
    # -------------------


    # INPUT VALIDATION
    # ----------------



    # COMMON HELPER FUNCTIONS
    # -----------------------

    # INPUTS

    def get_display_name(self) -> str:
        if(self.display_name2 is None or self.display_name2 == "" or len(self.display_name2) == 0):
            return self.display_name
        else:
            return self.display_name2


    # TABLE

    def _updated_table_schema_cols(self, table_schema_cols, num_target_var_cols, num_static_cols, field_value, field_name) -> list:
        # get the current number of columns
        num_cols = len(table_schema_cols)
        target_num_cols = num_static_cols + num_target_var_cols

        # if not changing the number of columns, return the corrent Schema
        # NOTE:  this should never happen, since the calling method should be handling it, 
        # but handling it just in case
        if(target_num_cols == num_cols):
            return(table_schema_cols)
        
        # cut that array to the number of total columns required, but change nothing else
        elif(target_num_cols < num_cols):
            table_schema_cols = table_schema_cols[:target_num_cols]
            return(table_schema_cols)

        # otherwise, we need to add some variable columns
        else:
            #num_cols_to_add = target_num_cols - num_cols
            start_num = num_cols - num_static_cols

            for i in range(start_num, target_num_cols):
                    table_schema_cols.append(self._gen_new_table_col(col_num = i))

            return(table_schema_cols)


    def _gen_new_table_col(self, col_num: int) -> dict:
        raise ValueError(f"\n*  ForecastComponent._gen_new_table_col:  error, this is an abstract method which should never be called.")



    # _get_input_table_display_name
    # Convenience function to get the display name of a TableInput
    #
    # INPUTS
    #   table_name = name of the TableInput
    #
    # OUTPUTS
    #   table display name

    def _get_input_table_display_name(self, table_name: str) -> str:

        if table_name in list(self._inputs.keys()):
                  input_table = self._inputs[table_name]

                  if hasattr(input_table, "display_name"):
                    return input_table.display_name
                  else:
                      return table_name                     
        else:
            raise ValueError(f"\n*  _get_input_table_display_name:  invalid table name '{table_name}' provided.")


    # _get_input_table_col_display_name
    # Convenience function to get the name (id) of a column in an input_table
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_num = column number as defined in the TableSchema
    #
    # OUTPUTS
    #   column name (id)

    def _get_input_table_col_display_name(self, table_name: str, col: int | str) -> str:
        input_col = self._get_input_table_col(table_name = table_name, col = col)
        
        if hasattr(input_col, "display_name"):
            return input_col.display_name
        else:
            return self._get_input_table_col_name(table_name, col)                    


    # _get_input_table_col_name
    # Convenience function to get the name (id) of a column in an input_table
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col = can be the index of a column or the name of a column in the InputTable's TableSchema
    #
    # OUTPUTS
    #   column name (id)

    def _get_input_table_col_name(self, table_name: str, col: int | str) -> str:
        input_col = self._get_input_table_col(table_name = table_name, col = col)
        
        if hasattr(input_col, "name"):
            return input_col.name
        else:
            return str(col)


    # _get_input_table_col
    # Convenience function to get a Column object from an TableInput's TableSchema by column index or name
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_num = column number as defined in the TableSchema
    #
    # OUTPUTS
    #   column name (id)

    def _get_input_table_col(self, table_name: str, col: int | str) -> Column:
        
        if table_name in list(self._inputs.keys()):
            input_table = self._inputs[table_name].table_schema.columns

            col_num = col if isinstance(col, int) else self._get_input_table_col_num_from_name(table_name = table_name, col_name = col)

            return(input_table[col_num])
        else:
            raise ValueError(f"\n*  _get_input_table_col:  invalid table name '{table_name}' provided.")


    # _get_input_table_col_num_from_name
    # Convenience function to get the index of a column in an InputTable based on it's name.
    # NOTE:  Assumes column names are unique and only returns the first match
    #
    # INPUTS
    #   table_name = name of the TableInput
    #   col_name = column name in the table
    #
    # OUTPUTS
    #   index of column name in table

    def _get_input_table_col_num_from_name(self, table_name: str, col_name: str) -> int:
        col_names = []

        if table_name in list(self._inputs.keys()):
            input_table = self._inputs[table_name].table_schema.columns

            for i in range(len(input_table)):
                if col_name == input_table[i].name:
                    return(i)
                else:
                    col_names.append(input_table[i].name)
                  
            raise ValueError(f"\n*  _get_input_table_col_num_from_name:  column name '{col_name}' not found in '{table_name}', list of columns {col_names}.")

        else:
            raise ValueError(f"\n*  _get_input_table_col_name:  invalid table name '{table_name}' provided.")



    # DATAPACKET

    # __unpack_data_packets
    # unpack a LIST of data packets into two LISTS:  one of Dataframes, one of ForecastMetaDataFrames
    # 
    # INPUTS:
    #   data_packet - (list) of data packets
    #
    # OUTPUTS:
    # dataframes - list of dataframes from the packets
    # meta_datas - list of ForecastMetaDataFrames from the packets
    # total_ids - list of the ids of the last ForecastMetaDataSeries in those frames
    # display_names - list of all the display names for the last ForecastMetaDataSeries in those frames

    def _unpack_data_packets(self, data_packet: list[Data], convert_to_pandas: bool = False) -> tuple[list[DataFrame], list[ForecastMetaDataFrame], list[str], list[str]]:
        (dataframes, meta_datas, last_ids, last_display_names) = ForecastDataPacket.unpack_data_packets(data_packet, convert_to_pandas = convert_to_pandas)
        return(dataframes, meta_datas, last_ids, last_display_names)
    


    # _unpack_data_packet
    # updacks an individual data packet into a single Dataframe and a single ForecastMetaDataFrame
    # 
    # INPUTS:
    #   data_packet - (list) of data packets
    #
    # OUTPUTS:
    # dataframe - the dataframes from the packet
    # meta_data - theForecastMetaDataFrame from the packets
    # total_id - the ids of the last ForecastMetaDataSeries in the frame
    # display_name - the display names for the last ForecastMetaDataSeries in the frame

    def _unpack_data_packet(self, data_packet: Data) -> tuple[DataFrame, ForecastMetaDataFrame, str, str]:
        (dataframe, meta_data, last_id, last_display_name) = ForecastDataPacket.unpack_data_packet(data_packet)
        return(dataframe, meta_data, last_id, last_display_name)

    # given a dataframe and meta_data, returns a DataPacket with both in it
    def _gen_data_packet(self, dataframe: DataFrame | pd.DataFrame, meta_data: ForecastMetaDataFrame, last_id: str, check_ids: bool = True) -> Data:
         data_packet = ForecastDataPacket.gen_data_packet(dataframe = dataframe, meta_data = meta_data, last_id = last_id, default_value = last_id)
         return(data_packet)
    
    # generate a pickle and save it to a file location
    def _pickle_and_save_data_packet(self, data_packet: ForecastDataPacket, path: str):
        ForecastDataPacket.pickle_data_packet(data_packet = data_packet, path = path)



    # META_DATA

    # add_col_data_meta
    # Handles the addition of a action to both the DataFrame and the ForecastMetaDataFrame
    # NOTE:  Due to desire for strict typing, this takes explicity arguments that go into
    #        generating a new ForecastMetaDataSeries.  This means that if ForecastMetaDataSeriesSchema
    #        is updated, it will need to be reflected here.
    # 
    # INPUTS:
    #   dataframe = existing dataframe to update
    #   meta_data = existing ForecastMetaDataFrame to update
    #   id = unique id for the new action column to be added
    #   display_name = user friendly name for the action column
    #   data_values = the data values to add to the dataframe (as a pandas Series)
    #   [next several arguments are all taken from the Schema definition for ForecastMetaDataSeries, see forecast_meta_data.py for more information]
    #   vertify_integrity (optional) = (default: True) set True if you want the function to raise an error if the id being added already exists in the dataframe/meta_data
    #   drop_dups (optional) = (default: False) set True if you want the function to automatically discard any a new column if it's id already exists in the dataframe / meta_data, if this is set, vertify_integrity setting is ignored 
    #   
    # OUTPUTS:
    #   DataFrame = updated DataFrame with the new action column
    #   ForecastMetaDataFrame = updated ForecastMetaDataFrame with the new action column

    @staticmethod
    def _add_col_data_meta(dataframe: DataFrame | pd.DataFrame,
                           meta_data: ForecastMetaDataFrame,
                           id: str,
                           display_name: str,
                           step_type: ForecastDataSeriesMetaDataStepTypes,
                           action: ForecastDataSeriesMetaDataAction,
                           data_type: ForecastDataSeriesMetaDataDataType,
                           display_type: ForecastDataSeriesMetaDataDataType,
                           validation: List[Dict[ForecastDataSeriesMetaDataValidationSchema, Any]],
                           data_values: pd.Series | list = None,
                           pred: List[str | int | float] = None,
                           args: Dict = None,
                           objs: List = None,
                           update_last_id = False,                           
                           verify_integrity: bool = True,
                           drop_dups: bool = False) -> tuple[DataFrame, ForecastMetaDataFrame]:
          
          # make sure data_values is a list (empty or not)
          if(data_values is None):
              data_values = []
          elif(not isinstance(data_values, list)):
              data_values = data_values.to_list()


          # create a data values holder for meta_data
          if(len(data_values) == 0):
              data_values_meta_data = []
          else:
              data_values_meta_data = data_values
          
          # add col to meta_data
          new_meta_col = ForecastMetaDataSeries(id = id,
                                                step_type = step_type,
                                                action = action,
                                                data_type = data_type,
                                                display_type = display_type,
                                                display_name = display_name,
                                                data_values = data_values_meta_data,
                                                validation = validation,
                                                pred = pred,
                                                args = args,
                                                objs = objs)
          updated_meta_data = ForecastMetaDataFrame.concat([meta_data, new_meta_col], verify_integrity = verify_integrity, drop_dups = drop_dups)
          
          # add col to data
          updated_dataframe = ForecastDataModel.add_col_to_model(dataframe, data_values, new_col_name = id)

          # update last id
          if update_last_id:
              updated_meta_data.set_last_id(id = id)


          return(updated_dataframe, updated_meta_data)
    


    # DEBUGGING

    def _dump_in_editor_state(self, field_name = None, field_value = None, frontend_node = None, build_config = None, **kwargs):

        def dump_single(name: str, item: Any):
            if (item is not None):
                print(f"{name} provided:")
                print(f"{name} {type(item)} = {item}")
            else:
                print(f"{name} NOT PROVIDED")

            #print("\n")


        def dump_multi(name: str, item: Any):
            if (item is not None):
                print(f"{name} provided:")
                
                if(isinstance(item, list)):
                    print(f"{name} len = '{len(item)}'")

                    for value in item:
                        print(f"{type(value)}")

                elif(isinstance(item, dict)):
                    for key, value in item.items():
                        print(f"\t{key} = {type(value)}")
            else:
                print(f"{name} NOT PROVIDED")

            #print("\n")


        def dump_unknown(name: str, item: Any):
            if item is not None:
                print(f"'{name}'s type: {type(item)}")

                if isinstance(item, int | float | str):
                    dump_single(name, item)
                elif isinstance(item, list | dict):
                    dump_multi(name, item)
                else:
                    print(f"{name} provided:")
                    print(f"{name} is '{type(item)}'")
            else:
                print(f"{name} NOT PROVIDED")

            #print("\n")
            


        # main function
        print("\n\nDUMP IN EDITOR STATE:\n")

        if(field_name is not None):
            print(f"field_name: {field_name}")
        else:
            print("No field_name provided")

        dump_unknown(field_name, field_value)
        dump_multi("frontend_node", frontend_node)
        dump_multi("build_config", build_config)

        # iterate over kwargs
        if kwargs:
            for key, value in kwargs.items():
                dump_unknown(key, value)

        print("\n\n")




