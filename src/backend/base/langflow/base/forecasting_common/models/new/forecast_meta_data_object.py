



# ==============
# COMMON IMPORTS
# ==============

from .forecast_meta_data_common import *
#from .forecast_meta_data_container import ForecastMetaDataContainer


# =======
# CLASSES
# =======



# ForecastMetaDataObject
# Root object for all objects in ForecastMetaData hierarchy
class ForecastMetaDataObject():

    # INSTANCE VARIABLES
    # ------------------
    id: str = None
    parent = None
    #parent: Type['ForecastMetaDataContainer']  = None

    # meta_data attributes
    display_name: str = None


    # __init__
    # Adds initializing all meta-data attributes to None.
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, display_name, id: str, parent, *args, **kwargs):
    #def __init__(self, display_name, id: str, *args, **kwargs):
        
        # initialize all instance variables
        self.id: str = None
        self.display_name: str = None
        #self.parent: Type['ForecastMetaDataContainer'] = None
        self.parent = None


        # load any attributes provided in the init function
        self.id = id
        self.display_name: str = display_name

        #if parent is not None:
        #    self.parent: Type['ForecastMetaDataContainer'] = parent
        if parent is not None:
            self.parent = parent


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
        results += f"id = {self.id}\n"
        results += f"display_name = {self.display_name}\n"

        #if(self.parent is not None):
        #    results += f"parent = {self.parent.id}\n"
        #else:
        #    results += f"no parent\n"

        if(self.parent is not None):
            results += f"parent = {self.parent.id}\n"
        else:
            results += f"no parent\n"

        results += super().__str__()
        return results


    # to_dict
    # Serialize this object as a dictionary
    
    def to_dict(self) -> dict:
        out_dict = {}
        out_dict["id"] = self.id
        out_dict["display_name"] = self.display_name

        #if(self.parent is not None):
        #    out_dict["parent"] = self.parent.id
        if(self.parent is not None):
            out_dict["parent"] = self.parent.id

        return(out_dict)

    
    # # to_json
    # # Serialize this object to a JSON string
    # #  
    # # INPUTS:
    # #   ident (optional: 4) - number of spaces to indent the JSON
    # # 
    # # OUTPUTS:
    # #   JSON string

    # def to_json(self, indent:int = 4) -> str:
    #     import json
    #     return json.dumps(self, default=FormatMetaDataObjectsJsonSerializer, indent=indent)








