# ==============
# COMMON IMPORTS
# ==============

from .forecast_meta_data_common import *




# ==============
# COMMON IMPORTS
# ==============

# ForecastMetaDataRange
# Holds a set a specific range for the Series' action with specific parameters (reqs, args, objs).  This allows us to change the parameters (and therefore the calculations)
# over the course of an action processing a column of data (which is important for Treatment)
class ForecastMetaDataRange():

    # CLASS VARIABLES
    # ---------------


    # INSTANCE VARIABLES
    # ------------------
    _meta_data = None


    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, *args, **kwargs):
        self._meta_data = {}

        # init all meta_data attributes
        for attrib in ForecastMetaDataRangeSchema:
            if attrib in kwargs:
                self._meta_data[attrib] = kwargs.get(attrib)
            else:
                self._meta_data[attrib] = None


    # set_forecast_meta_data
    # Takes all the meta_data forecast as a set of arguments and stuffs them in the attributes of the object
    # easier to do than manually updating each attribute in the DataFrame object
    #  
    # INPUTS:
    #   Each meta-data field in the ForecastDataSeriesMetaDataSchema
    # 
    # OUTPUTS:
    #   NA

    def set_forecast_meta_data(self, *args, **kwargs):
        for arg_name in kwargs:
            if arg_name in ForecastMetaDataRangeSchema:
                self._meta_data[arg_name] = kwargs.get(arg_name)
            else:
                raise ValueError(f"*  set_forecast_meta_data:  invalid arg_name '{arg_name}'")
        

    # set_forecast_meta_data_bulk
    # Takes all the meta_data forecast as a set of arguments and stuffs them in the attributes of the object
    # but in a bulk format (dict), might be easier to do when constantly copying from only pandas data series to new ones
    # (after a concat operations, for example, which wipes out all the meta-data)
    #  
    # INPUTS:
    #   Dict with name_value pairs for all the meta-data
    # 
    # OUTPUTS:
    #   NA

    def set_forecast_meta_data_bulk(self, meta_data_attribs: dict):
        for key in meta_data_attribs.keys():
            if key in ForecastMetaDataRangeSchema:
                self._meta_data[key] = meta_data_attribs[key]
            else:
                raise ValueError(f"*  set_forecast_meta_data_bulk:  invalid key '{key}'")
        


    # get_forecast_meta_data_bulk
    # Returns a dump of all the meta-data_attributes from the pandas data series, but in a bulnk format (dict)
    # might be easier to do when constantly copying from only pandas data series to new ones
    # (after a concat operations, for example, which wipes out all the meta-data)
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Dict with name_value pairs for all the meta-data

    def get_forecast_meta_data_bulk(self) -> dict:
        meta_data_attribs = {}

        for attrib in ForecastMetaDataRangeSchema:
            meta_data_attribs[attrib] = self._meta_data[attrib]

        return meta_data_attribs
    

    # COUNT = "count" # the number of elements in the column to apply this, if None, assume all remaining cells
    # PRED = "pred" # predecessors, a set of column ids necessary for the action
    # ARGS = "args" # any additional values necessary for actions, or validations
    # OBJS = "objs" # any additional objects which are required for this step

    def has_count(self):
        if (ForecastMetaDataRangeSchema.COUNT in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataRangeSchema.COUNT] is not None):
            return True
        else:
            return False


    def get_count(self):
        if(self.has_count()):
            return self._meta_data[ForecastMetaDataRangeSchema.COUNT]


    
    def get_pred(self):
        return self._meta_data[ForecastMetaDataRangeSchema.PRED]

    

    
    def has_args(self):
        if (ForecastMetaDataRangeSchema.ARGS in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataRangeSchema.ARGS] is not None) and (len(self._meta_data[ForecastMetaDataRangeSchema.ARGS]) > 0):
            return True
        else:
            return False
    

    def get_args(self):
        if(self.has_args()):
            return self._meta_data[ForecastMetaDataRangeSchema.ARGS]


    def get_arg(self, name: str):
        if(self.has_args()):
            if(name in self._meta_data[ForecastMetaDataRangeSchema.ARGS].keys()):
                return(self._meta_data[ForecastMetaDataRangeSchema.ARGS][name])
            else:
                return None
        else:
            return None

    
    def has_objs(self):
        if (ForecastMetaDataRangeSchema.OBJS in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataRangeSchema.OBJS] is not None) and (len(self._meta_data[ForecastMetaDataRangeSchema.OBJS]) > 0):
            return True
        else:
            return False


    def get_objs(self):
        if(self.has_objs()):
            return self._meta_data[ForecastMetaDataRangeSchema.OBJS]


    def get_obj(self, name: str):
        if(self.has_objs()):
            if(name in self._meta_data[ForecastMetaDataRangeSchema.OBJS].keys()):
                return(self._meta_data[ForecastMetaDataRangeSchema.OBJS][name])
            else:
                return None
        else:
            return None
        


    # __str__
    # Return a printable version of the class instance
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Str with printable version of instance data

    def __str__(self):
        results = super().__str__()

        for attrib in ForecastMetaDataRangeSchema:
            results += f"{attrib} = {self._meta_data[attrib]}\n"

        return results
    

    
    # # to_json
    # # Return a printable version of the class instance
    # #  
    # # INPUTS:
    # #   NA
    # # 
    # # OUTPUTS:
    # #   Str with printable version of instance data

    # def to_json(self, indent: int = 4) -> str:
    #     import json
    #     return json.dumps(self, default=ForecastMetaDataJsonSerializer, indent=indent)



