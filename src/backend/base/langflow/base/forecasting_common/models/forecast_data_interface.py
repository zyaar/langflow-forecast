#####################################################################
# forecast_model_engine.py
#
# Implements the core (builder independent) algorithms of the model creation engine.
# 
#####################################################################


from typing import List, Dict, Tuple, Any
from datetime import datetime
import pandas as pd
import numpy as np
from langflow.schema.dataframe import DataFrame, Data


# FORECAST SPECIFIC IMPORTS
# =========================
from langflow.base.forecasting_common.constants import FORECAST_INT_TO_SHORT_MONTH_NAME, ForecastModelInputTypes, ForecastModelTimescale
#from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataSeries, 
                                                                        ForecastMetaDataFrame, 
                                                                        ForecastMetaDataSeriesSchema, 
#                                                                        ForecastMetaDataFrameSchema,
#                                                                        ForecastDataSeriesMetaDataStepTypes, 
#                                                                        ForecastDataSeriesMetaDataAction, 
#                                                                        ForecastDataSeriesMetaDataDataType, 
#                                                                        ForecastDataSeriesMetaDataValidationSchema, 
#                                                                        ForecastDataSeriesMetaDataValidateInputRestrictions,
#                                                                        ForecastDataSeriesMetaDataComparisonType,
                                                                        ForecastMetaDataSeriesIdGenerator,
                                                                        ForecastMetaDataRange,
                                                                        ForecastMetaDataRangeSchema)


# COMPONENT SPECIFIC IMPORTS
# ==========================
from datetime import datetime
from enum import Enum
import shutil
from openpyxl import Workbook, worksheet, load_workbook
from openpyxl.cell.cell import Cell
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Protection


from abc import ABC, abstractmethod


# CLASSES
# =======


# simple class to hold the pred reference ID for sharing across the iterator and a specific builder address lookup
class ForecastPredRef():
    const = None
    full_id = None
    rel_id = None
    single_value = None
    shift_value = None
    has_full_id = None
    has_single_value = None
    has_shift_value = None

    def __init__(self, 
                 const: int | float = None,
                 full_id : str = None,
                 rel_id : str = None,
                 single_value : int = None,
                 shift_value : int = None,
                 has_full_id : bool = None,
                 has_single_value : bool = None,
                 has_shift_value: bool = None):
        
        self.const = const
        self.full_id = full_id
        self.rel_id = rel_id
        self.single_value = single_value
        self.shift_value = shift_value
        self.has_full_id = has_full_id
        self.has_single_value = has_single_value
        self.has_shift_value = has_shift_value




# IdElementToHandleMap
# Abstract interface to manage a SINGLE mapper which go from ForecastMetaDataFrame and ForecastMetaDataSeries to specific builder (EXCEL, REACT, etc.)
class IdElementToHandleMap(ABC):

    # INSTANCE VARIABLES
    # id_to_ref_map - a dictionary which maps all ForecastDataModel IDs to the tab and the cell reference of their rows in excel
    # default_num_elements - the default number of elements to store for an id

    # __init__ function, does nothing right now
    id_to_ref_map = {}

    # Add a new entry to the map: id is the key, tab_name and object handle
    @abstractmethod
    def add(self, id: str, tab_name: str, cell_ref: Cell, num_elements: int = None) -> dict:
        pass

    # Given an id, return the: tab_name, cell object pointing to the start of row
    @abstractmethod
    def get(self, id: str) -> Tuple[str, Any, int]:
        pass

    # Return a PRED ref, which is more complicated than a simple get
    @abstractmethod
    def ref_to_obj(self, id: ForecastPredRef, element_num: int) -> tuple[str, Any, int]:
        pass




# IdElementToHandleMaps
# Abstract interface to manage a SERIES of mappers which go from ForecastMetaDataFrame and ForecastMetaDataSeries to specific builder (EXCEL, REACT, etc.)
class IdElementToHandleMaps(ABC):

    # INSTANCE VARIABLES
    # player_model = handle to the Workbook object for excel
    # default_ref_map_id = if no ref_map_id provided, use this one

    # CLASS VARIABLES
    ref_maps = {}

    # Add a new entry for one of the objects in the map: id is the key, tab_name and object handle
    @abstractmethod
    def add(self, id: str, tab_name: str, cell_ref: Cell, ref_map_id: str = None, num_elements: int = None):
        pass


    # Given an id, return the: tab_name, cell object pointing to the start of row from the appropriate mapper
    @abstractmethod
    def get(self, id: str, ref_map_id: str = None) -> Tuple[str, Cell, int]:
        pass

        
    # Add a new reference map
    @abstractmethod
    def create_ref_map(self, ref_map_id: str, default_num_elements: int, is_default = False):
        pass


    # convert a reference to a handle
    @abstractmethod
    def ref_to_obj(self, id: ForecastPredRef, element_num: int) -> tuple[str, Any, int]:
        pass


    # # should probably be moved to forecast_meta_data
    # @abstractmethod
    # def _parse_id(self, id: str) -> tuple[str, str, int | None]:
    #     pass
    










# This class is given an acount and an address_mapper and, as an iterator, returns the next address to access
class ForecastPredIterator():
    # INSTANTIATED REFERENCES
    # -----------------------

    # default_card
    # col_meta_data
    # address_mapper

    # total_elements
    # curr_element


    # ranges
    # total_ranges
    # curr_range_expires
    # curr_range_index
    # curr_element_in_range

    # num_preds
    # preds[]
    # pred_calcs{}

    
    # # Enum holding the schema of the meta-data model
    # # The different meta-data attributes stores for each pandas data series (i.e. each column) in the forecast model

    def __init__(self, col: ForecastMetaDataSeries, address_maps: IdElementToHandleMaps, default_card: str, total_elements:int = None):
        self.default_card = default_card
        self.col_meta_data = col
        self.address_mapper = address_maps

        if(total_elements is None):
            self.total_elements = len(col.get_data_values())

            if(self.total_elements < 1):
                raise ValueError(f"\n*  ForecastMetaDataRangeSchema:  total_elements for '{col.get_id()}' is either missing or zero.")
        else:
            self.total_elements = total_elements

        # if no ranges are provided, create a single element list of range(s) by packaging up the ForecastMetaDataSeries schema
        #if (ForecastMetaDataSeriesSchema.RANGES not in col.meta_data.keys()) or (col.meta_data[ForecastMetaDataSeriesSchema.RANGES] is None):
        if (not col.has_ranges()):
            # range = ForecastMetaDataRange(count = None,                                             # setting to None makes it apply to all elements)
            #                               pred = col.meta_data[ForecastMetaDataSeriesSchema.PRED],
            #                               args = col.meta_data[ForecastMetaDataSeriesSchema.ARGS],
            #                               objs = col.meta_data[ForecastMetaDataSeriesSchema.OBJS])
            range = ForecastMetaDataRange(count = None,             # setting to None makes it apply to all elements)
                                          pred = col.get_pred(),
                                          args = col.get_args(),
                                          objs = col.get_objs())
            self.ranges = [range]

        # otherwise copy over the current ranges list
        else:
            self.ranges = col.get_ranges()

        self.total_ranges = len(self.ranges)

        # create a tracker to hold all the preds whose addresses we will be calculating
        self.num_preds = len(self.ranges[0].get_pred())



    # implements the iterator interface
    # initialization
    def __iter__(self):
        # initialize all our counters and tracking structures
        self.curr_element = -1
        self.curr_range_index = -1
        self.curr_range_expires = -1
        self.pred_calcs = {}
        return self



    # implements the iterator interface
    # returns the list of pred ids for the next iteration
    #
    # INPUT:
    #   NA
    #
    # OUTPUT:
    #   openpyxl cells in a dictionary of the form:
    #       {row_id: [tab_name - (str) name of tab in excel, cell_ref - (openpyxl Cell) cell object for the element, num_elements - total number of elements in this row]}
    def __next__(self) -> dict:
        self.curr_element += 1

        # if we have reached or passed the end, throw StopIteration
        if(self.curr_element >= self.total_elements):
            raise StopIteration
        
        # if we have reached the end the current range, get the next range
        if(self.curr_element > self.curr_range_expires):
            self.load_next_range()

        next_addresses = self.get_next_addresses(self.curr_element)

        # increment the curr_element
        self.curr_element_in_range += 1

        # return the addresses
        return next_addresses
    


    # calculate all the addresses to return based on the current_element, and the pred_calcs structures
    def get_next_addresses(self, element_in_range: int):
        results = {}

        # address lookup functionlity implemented here
        for id_key in list(self.pred_calcs.keys()):

            # check if this is a constant, if so, add it back as one and skip the remainder
            if self.pred_calcs[id_key].const is not None:
                results[id_key] = (self.pred_calcs[id_key].const)
            else:
                reference = self.address_mapper.ref_to_obj(self.pred_calcs[id_key], element_in_range)
                results[id_key] = reference

        return results




    # load the next range
    def load_next_range(self):
        self.pred_calcs = {}

        # get the duration for the next range
        curr_range = self.ranges[self.curr_range_index+1]

        if(curr_range.get_count() is None):
            self.curr_range_expires = self.total_elements
        else:
            self.curr_range_expires = self.curr_element + curr_range.get_count() - 1   # since we are zero indexed, and the current element is included


        # parse the preds into the pred_calc structure
        self.parse_pred_ids(preds = curr_range.get_pred(), default_card = self.default_card)

        # restart our location in the current range
        self.curr_element_in_range = 0

        # increment to the next curr_range
        self.curr_range_index += 1


    # parse all the pred_ids
    def parse_pred_ids(self, preds: list[str], default_card: str):
        for pred_id in preds:
            self.parse_pred_id(pred_id, default_card)



    # parse a pred_id into a data_structure for use in iterations
    def parse_pred_id(self, id: str, default_card: str):
        import nanoid

        # check if this is a constant, if so, return as a constant
        if isinstance(id, (int, float)):
            self.pred_calcs[f"Const_{nanoid.generate(size=5)}"] = ForecastPredRef(const = id)

        # process the ref id
        else:
            (full_id, rel_id, single_value, shift_value, has_full_id, has_single_value, has_shift_value) = ForecastMetaDataSeriesIdGenerator.parse_id(id = id, default_full_id = default_card)

            full_ref = f"{full_id}_{rel_id}"
            self.pred_calcs[full_ref] = ForecastPredRef(full_id = full_id,
                                                        rel_id = rel_id,
                                                        single_value = single_value,
                                                        shift_value = shift_value,
                                                        has_full_id = has_full_id,
                                                        has_single_value = has_single_value,
                                                        has_shift_value = has_shift_value)       




    # OUTPUT:
    #   full_id or None
    #   rel_id
    #   single_value or None
    #   shift_value or None
    #   has_full_id - True if there was one, false if not (although default_full_id will be provided even if there isn't one)
    #   has_single_value - True if this is a single value address (i.e. XYZ:1), false if otherwise
    #   has_shift_value - True if this is a shift value address (i.e. XYZ[1]), false if otherwise




# ForecastDataInterfaceJsonSerializer
def ForecastDataInterfaceJsonSerializer(obj):
    from langflow.schema.dataframe import DataFrame

    if isinstance(obj, ForecastMetaDataFrame):
        return ({"meta_data": obj._meta_data, "model": obj._model})
    elif isinstance(obj, ForecastMetaDataSeries):
        return obj.meta_data
    elif isinstance(obj, DataFrame):
        return obj.to_dict()
    elif isinstance(obj, pd.Series):
        return obj.to_list()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, Enum):
        return obj.value
    else:
        raise TypeError(f"Type {type(obj)} not serializable by ForecastDataInterfaceJsonSerializer")



