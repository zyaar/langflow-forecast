# ==============
# COMMON IMPORTS
# ==============

from .forecast_meta_data_common import *
#from .forecast_meta_data_object import ForecastMetaDataObject
#from .forecast_meta_data_container import ForecastMetaDataContainer

import nanoid
import re


# =======
# CLASSES
# =======





# =======
# CLASSES
# =======

# ForecastMetaDataSeriesIdGenerator
# This class encapsulates ID generation and ID parsing for all IDs generated in ForecastMetaData and DataFrame as part of
# the data-model
class ForecastMetaDataSeriesIdGenerator():
    # SAMPLE ID FORMAT:  (EXTERNAL_ID|)(FULL_ID.)REL_ID():SINGLE_VALUE)(NUM_TO_SHIFT)     () = optional
    # EXAMPLE:  EXTERNAL|ABC.XYZ:2[1]        external_id = EXTERNAL, full_id = ABC, rel_id = XYZ, element = 2, shift address by left 1 time

    NANOID_CHAR_SET = "(A-Za-z0-9_-)"
    PREFIX_SEP_CHAR = "_"
    EXTERNAL_ID_SEP_CHAR = "|"
    FULL_ID_SEP_CHAR = "."
    SINGLE_VALUE_SEP_CHAR = ":"

    #external_match_regex = ??? TODO:  finish this to expand to EXTERNAL_ID
    full_match_regex = r"^\s*([\w-]+)\.(?!.*\.)"
    rel_match_regex = r"^([\w-]+)(:|\[|$)"
    shift_match_regex = r"\[(-?\d+)\]\s*$"
    single_match_regex = r"[^:+]:(-?\d+)"


    # instance variables
    # container: ForecastMetaDataFrame - the object holding this instance
    # container_id: str - the id of the object holding this instance

    def __init__(self, container: Any):
        self.container = container


    # FULL ID
    # extract the full_id from the id string
    @staticmethod
    def get_full_id(id: str) -> str:
        match = re.search(ForecastMetaDataSeriesIdGenerator.full_match_regex, id)

        if(match):
            return match[1]
        else:
            return None
        
    # check if there is a full id
    @staticmethod
    def has_full_id(id: str) -> bool:
        if ForecastMetaDataSeriesIdGenerator.get_full_id(id) is None:
            return False
        else:
            return True
        
        
    # RELATIVE ID
    # extract the rel_id from the id string
    @staticmethod
    def get_rel_id(id: str) -> str:
        # check if you can find and remove the full portion of a reference first
        match = re.search(ForecastMetaDataSeriesIdGenerator.full_match_regex, id)

        if(match):
            id = id.removeprefix(match[0])

        match = re.search(ForecastMetaDataSeriesIdGenerator.rel_match_regex, id)

        if(match):
            return match[1]
        else:
            return None
        


    # has_rel_id is not provided, because all ids have to have a relative




    # SINGLE VALUE
    # extract the single_value from the id string
    @staticmethod
    def get_single_value(id: str) -> int:
        match = re.search(ForecastMetaDataSeriesIdGenerator.single_match_regex, id)

        if(match):
            return(int(match[1]))
        else:
            return None
        


    # check if ID has a single_value
    @staticmethod
    def has_single_value(id: str) -> bool:
        if ForecastMetaDataSeriesIdGenerator.get_single_value(id) is None:
            return False
        else:
            return True
        

        

    # SHIFT VALUE
    # extract the shift_value from the id string
    @staticmethod
    def get_shift_value(id: str) -> int:
        match = re.search(ForecastMetaDataSeriesIdGenerator.shift_match_regex, id)

        if match:
            return(int(match[1]))
        else:
            return None

    # check if ID has shift_value
    @staticmethod
    def has_shift_value(id: str) -> bool:
        if ForecastMetaDataSeriesIdGenerator.get_shift_value(id) is None:
            return False
        else:
            return True
        

        

    # parse_id
    # Given an id string, parse out all the different parts and return those and boolean indicators for what is there and what isn't
    #
    # INPUT:
    #   id - the id to parse
    #   default_full_id - (optional) the default full id, if provided, system will return it instead of None if no full-id is found
    #
    # OUTPUT:
    #   full_id or None
    #   rel_id
    #   single_value or None
    #   shift_value or None
    #   has_full_id - True if there was one, false if not (although default_full_id will be provided even if there isn't one)
    #   has_single_value - True if this is a single value address (i.e. XYZ:1), false if otherwise
    #   has_shift_value - True if this is a shift value address (i.e. XYZ[1]), false if otherwise
    
    @staticmethod
    def parse_id(id: str, default_full_id: str =  None) -> Tuple[str, str, int, int, bool, bool, bool]:
        has_full_id = False
        has_single_value = False
        has_shift_value = False

        full_id = None
        rel_id = None
        single_value = None
        shift_value = None

        # REL_ID
        rel_id = ForecastMetaDataSeriesIdGenerator.get_rel_id(id)

        # FULL_ID
        if(ForecastMetaDataSeriesIdGenerator.has_full_id(id)):
            has_full_id = True
            full_id = ForecastMetaDataSeriesIdGenerator.get_full_id(id)
        elif(default_full_id is not None):
            full_id = default_full_id

        # SINGLE_VALUE
        if(ForecastMetaDataSeriesIdGenerator.has_single_value(id)):
            has_single_value = True
            single_value = ForecastMetaDataSeriesIdGenerator.get_single_value(id)

        # SHIFT_VALUE
        if(ForecastMetaDataSeriesIdGenerator.has_shift_value(id)):
            has_shift_value = True
            shift_value = ForecastMetaDataSeriesIdGenerator.get_shift_value(id)

        return(full_id, rel_id, single_value, shift_value, has_full_id, has_single_value, has_shift_value)


    # get the parent container id
    def get_id(self) -> str:
        return self.container.get_id()
        

    # static_gen_rel_id
    @staticmethod
    def static_gen_rel_id(prefix: str = None, length: int = 5) -> str:
        if(prefix is None):
            return nanoid.generate(size=length)
        else:
            return f"{prefix}{ForecastMetaDataSeriesIdGenerator.PREFIX_SEP_CHAR}{nanoid.generate(size=length)}"

    # generate a relative ID
    def gen_rel_id(self, prefix: str = None, length: int = 5) -> str:
        if(prefix is None):
            return nanoid.generate(size=length)
        else:
            return f"{prefix}{ForecastMetaDataSeriesIdGenerator.PREFIX_SEP_CHAR}{nanoid.generate(size=length)}"
        
        
    # generate a full id
    def gen_full_id(self, prefix: str = None, length: int = 5) -> str:
        if(prefix is None):
            return f"{container.id}{nanoid.generate(size=length)}"
        else:
            return f"{self.get_id()}{self.FULL_ID_SEP_CHAR}{prefix}{self.PREFIX_SEP_CHAR}{nanoid.generate(size=length)}"
        

    # convert a relative id to a full id
    def rel_to_full_id(self, rel_id: str) -> str:
        return(f"{self.get_id()}{self.FULL_ID_SEP_CHAR}{rel_id}")

        # if(self.check_rel_id(rel_id)):
        #     return(f"{self.get_id()}{self.FULL_ID_SEP_CHAR}{rel_id}")
        # else:
        #     raise ValueError(f"\n*  rel_to_full_id:  Invalid relative ID provided '{rel_id}', relative id cannon contain a '{self.FULL_ID_SEP_CHAR}'.")

        
    # convert a full_id to a relative id
    @staticmethod
    def full_to_rel_id(full_id: str) -> str:
        if(ForecastMetaDataSeriesIdGenerator.has_full_id(full_id)):
            full_id_prefix = ForecastMetaDataSeriesIdGenerator.get_full_id(full_id)
            return full_id.removeprefix(full_id_prefix)
        else:
            raise ValueError(f"\n*  full_to_rel_id:  Invalid full ID provided '{full_id}'.")






