#####################################################################
# forecast_data_packet.py
#
# Implements a static class with routines to bundle and unbundle
# the Data packet object which holds that DataFrame and the ForecastMetaDataFrame
#
#####################################################################

from langflow.schema.dataframe import DataFrame, Data
import json
from pathlib import Path
import pickle


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
                                                                        ForecastMetaDataFrameSchema,
                                                                        ForecastDataSeriesMetaDataStepTypes, 
                                                                        ForecastDataSeriesMetaDataAction, 
                                                                        ForecastDataSeriesMetaDataDataType, 
                                                                        ForecastDataSeriesMetaDataValidationSchema, 
                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions)


# COMPONENT SPECIFIC IMPORTS
# ==========================
#from datetime import datetime
#from enum import Enum
#import numpy as np
import pandas as pd



# CONSTANTS
# =========



# CLASSES
# =======

class ForecastDataPacket():
    @staticmethod
    def gen_data_packet(dataframe: DataFrame | pd.DataFrame, meta_data: ForecastMetaDataFrame, last_id: str = None, default_value: str = "data missing") -> Data:
        # # use the id from the last column as the text_key, should be the same in both dataframe and meta_data
        # # or something has gone wrong
        # last_id_dataframe = dataframe.columns[-1]
        # last_id_meta_data = list(meta_data.model.keys())[-1]

        # if check_ids and (last_id_dataframe != last_id_meta_data):
        #     raise ValueError(f"* gen_data_packet: error, final cols of dataframe and meta-data do not have the same IDs:  dataframe = '{last_id_dataframe}', meta-data = '{last_id_meta_data}'.")

        if(meta_data is not None):
            new_packet = Data(data={"data": dataframe, "meta_data_json": meta_data.to_Data(), "meta_data": meta_data}, text_key=last_id, default_value = default_value)
        else:
            new_packet = Data(data={"data": dataframe, "meta_data": meta_data}, text_key=last_id, default_value = default_value)

        return(new_packet)


    
    @staticmethod
    def unpack_data_packets(data_packets: list[Data]) -> tuple[list[DataFrame], list[ForecastMetaDataFrame], list[str]]:
        dataframes = []
        meta_datas = []
        last_id_dataframes = []

        # iterate over the entire list of data_packets and break into two lists, one for the dataframes,
        # one for the ForecastMetaDataFrames
        for data_packet in data_packets:
            (dataframe, meta_data, last_id_dataframe) = ForecastDataPacket.unpack_data_packet(data_packet)
            dataframes.append(dataframe)
            meta_datas.append(meta_data)

            last_id_dataframes.append(last_id_dataframe)

        return(dataframes, meta_datas, last_id_dataframes)


    
    @staticmethod
    def unpack_data_packet(data_packet: Data) -> tuple[DataFrame, ForecastMetaDataFrame, str]:
        dataframe = data_packet.data["data"]
        meta_data = data_packet.data["meta_data"]
        text_key = data_packet.text_key

        # make sure the last text_key, the last column id of the dataframe, and the last column id of the meta-data all match
        # otherwise raise an error as something has gone wrong
        last_id_dataframe = dataframe.columns[-1]
        #last_id_meta_data = list(meta_data.model.keys())[-1]
        last_id_meta_data = meta_data.get_last_id()
        last_id_dataframe = data_packet.text_key

        if (last_id_dataframe != last_id_meta_data) or (last_id_dataframe != text_key):
            raise ValueError(f"* unpack_data_packet: error, final cols of dataframe and meta-data do not have the same IDs:  dataframe = '{last_id_dataframe}', meta-data = '{last_id_meta_data}', text_key = '{text_key}'.")

        return(dataframe, meta_data, last_id_dataframe)
    


    @staticmethod
    def pickle_data_packet(data_packet: Data, path: Path):
        with open(path, 'wb') as dest_file:
            pickle.dump(data_packet, dest_file)



    @staticmethod
    def unpickle_data_packet(path: Path) -> Data:
        with open(path, 'rb') as src_file:
            data_packet = pickle.load(src_file)
        
        return data_packet

        
