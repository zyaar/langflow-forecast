# ==============
# COMMON IMPORTS
# ==============

from .forecast_meta_data_common import *

from .forecast_meta_data_object import ForecastMetaDataObject
from .forecast_meta_data_series import ForecastMetaDataSeries
from .forecast_meta_data_container import ForecastMetaDataGroup, ForecastMetaDataStep



# =======
# CLASSES
# =======


# FormatMetaDataObjectsJsonSerializer
def FormatMetaDataObjectsJsonSerializer(obj):
    if isinstance(obj, ForecastMetaDataSeries):
        return obj.to_dict()
    elif isinstance(obj, ForecastMetaDataGroup):
        return obj.to_dict()
    elif isinstance(obj, ForecastMetaDataStep):
        return obj.to_dict()
    elif isinstance(obj, ForecastMetaDataGroup):
        return obj.to_dict()
    elif isinstance(obj, ForecastMetaDataObject):
        return obj.to_dict()
    elif isinstance(obj, pd.Timestamp):
        return obj.date().strftime("%Y-%m-%d%Z") # adding the timezone (%Z) seems to makes the JSON formatter put in a date instead of a datetime, which is what we want
    else:
        raise TypeError(f"Type {type(obj)} not serializable by FormatMetaDataObjectsJsonSerializer")
