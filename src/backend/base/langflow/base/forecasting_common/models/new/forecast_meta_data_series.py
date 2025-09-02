# ==============
# COMMON IMPORTS
# ==============

from .forecast_meta_data_common import *

from .forecast_meta_data_object import ForecastMetaDataObject
from .forecast_meta_data_container import ForecastMetaDataContainer
from .forecast_meta_data_range import ForecastMetaDataRange


# ForecastMetaDataSeries
# ----------------------

# Holds all the meta data for a pandas series (i.e. column) we need to render a forecast model
class ForecastMetaDataSeries(ForecastMetaDataObject):

    
    # CLASS VARIABLES
    # ---------------


    # INSTANCE VARIABLE
    # -----------------
    action: ForecastDataSeriesMetaDataAction = None
    ranges: list[ForecastMetaDataRange] = None
    data_type: ForecastDataSeriesMetaDataDataType = None
    display_type: ForecastDataSeriesMetaDataDataType = None
    data_values: list | pd.Series = None
    validation:  list[dict] = None
    pred: list[str] = None
    args: dict = None
    objs: dict = None


    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self,
                 id: str,
                 display_name: str, 
                 action: ForecastDataSeriesMetaDataAction,
                 ranges: list[ForecastMetaDataRange] = None,
                 data_type: ForecastDataSeriesMetaDataDataType = None,
                 display_type: ForecastDataSeriesMetaDataDataType = None,
                 data_values: list | pd.Series = None,
                 validation:  list[dict] = None,
                 pred: list[str] = None,
                 args: dict = None,
                 objs: dict = None,
                 parent: Type['ForecastMetaDataContainer'] = None,
                 **kwargs):
        
        # action
        if(action is not None):
            self.action: ForecastDataSeriesMetaDataAction = action
        else:
            raise ValueError(f"\n* ForecastMetaDataSeries:  error, no action type provided.")
        
        # ranges
        if(ranges is not None):
            self.ranges: list[ForecastMetaDataRange] = ranges
        else:
            ranges: list[ForecastMetaDataRange] = None

        # data_type
        if(data_type is not None):
            self.data_type: ForecastDataSeriesMetaDataDataType = data_type
        else:
            raise ValueError(f"\n* ForecastMetaDataSeries:  error, no data type provided.")

        # display_type
        if(display_type is not None):
            self.display_type: ForecastDataSeriesMetaDataDataType = display_type
        else:
            raise ValueError(f"\n* ForecastMetaDataSeries:  error, no display type provided.")

        # data_values
        if(data_values is not None):
            self.data_values: list | pd.Series = data_values
        else:
            self.data_values: list | pd.Series = None

        # validation
        if(validation is not None):
            self.validation:  list[dict] = validation
        else:
            self.validation: list[dict] = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],

        # pred
        if(pred is not None):
            self.pred: list[str] = pred
        else:
            self.pred: list[str] = None

        # args
        if(args is not None):
            self.args: dict = args
        else:
            self.args: dict = None

        # objs
        if(objs is not None):
            self.objs: dict = objs
        else:
            self.objs: dict = None

        super().__init__(display_name = display_name,
                         id = id,
                         parent = parent, 
                         **kwargs)
        

    # __str__
    # Return a printable version of the class instance
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Str with printable version of instance data

    def __str__(self):
        results = ""

        # add meta-data attributes
        results += f"action = {self.action}\n"
        results += f"ranges = {self.ranges}\n"
        results += f"data_type = {self.data_type}\n"
        results += f"display_type = {self.display_type}\n"
        results += f"data_values = {self.data_values}\n"
        results += f"validation = {self.validation}\n"
        results += f"pred = {self.pred}\n"
        results += f"args = {self.args}\n"
        results += f"objs = {self.objs}\n"

        results += super().__str__()
        return results

        
    # to_dict
    # Serialize this object as a dictionary
    
    def to_dict(self) -> dict:
        out_dict = super().to_dict()

        out_dict["action"] = self.action
        out_dict["ranges"] = self.ranges
        out_dict["data_type"] = self.data_type
        out_dict["display_type"] = self.display_type
        out_dict["data_values"] = self.data_values
        out_dict["validation"] = self.validation
        out_dict["pred "]= self.pred
        out_dict["args"] = self.args
        out_dict["objs"] = self.objs

        return(out_dict)


    # to_json
    # Serialize this object to a JSON string
    #  
    # INPUTS:
    #   ident (optional: 4) - number of spaces to indent the JSON
    # 
    # OUTPUTS:
    #   JSON string

    def to_json(self, indent:int = 4) -> str:
        import json
        return json.dumps(self, default=FormatMetaDataObjectsJsonSerializer, indent=indent)


    # has_data_values
    def has_data_values(self) -> bool:
        if(self.data_values is not None):
            return True
        
    # has ranges
    def has_ranges(self) -> bool:
        if(self.ranges is not None):
            return True

    # has args
    def has_args(self) -> bool:
        if(self.args is not None):
            return True

    # has arg (check INDIVIDUAL arg)
    def has_arg(self, key: str) -> bool:
        if self.has_args() and (key in self.args.keys()):
            return True
        else:
            return False
        
    # has objs
    def has_objs(self) -> bool:
        if(self.objs is not None):
            return True
        
    # has obj (check INDIVIDUAL obj)
    def has_obj(self, key: str) -> bool:
        if self.has_objs() and (key in self.objs.keys()):
            return True
        else:
            return False
        
    # has preds
    def has_preds(self) -> bool:
        if (self.pred is not None):
            return True
        else:
            return False

    # has pred (check INDIVIDUAL pred)
    def has_pred(self, key: str) -> bool:
        if self.has_preds() and (key in self.pred):
            return True
        else:
            return False
