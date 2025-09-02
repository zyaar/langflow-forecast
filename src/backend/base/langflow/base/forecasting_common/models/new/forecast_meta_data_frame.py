# ForecastMetaDataFrame
# Holds all the meta data for a pandas dataframe we need to render a forecast model
class ForecastMetaDataFrame():

    # INSTANCE VARIABLES
    # ------------------
    # meta_data - (dict) stores all the forecast meta-data for this instance
    # model - (dict of ForecastMetaDataSeries) stores all meta data for the specific columsn of the model (ForecastMetaDataSeries)
    # id_mgr - (ForecastMetaDataSeriesIdGenerator) can handle all id tasks (generation, conversion, information methods) 

    _meta_data: dict = None
    _model: dict[ForecastMetaDataSeries] = None


    # __init__
    # Initializing all meta-data attributes to None or to values passed in.  Initialize the model data structure
    #  
    # INPUTS:
    #   Any of the meta-data attributes can be set
    # 
    # OUTPUTS:
    #   NA

    def __init__(self, id_prefix: str = "ForecastMetaDataFrame", *args, **kwargs):
        self.id_prefix: str = id_prefix
        self._meta_data: dict = {}
        self._model: dict[ForecastMetaDataSeries] = {}

        # create an id_mgr and put pointer to this object as it's container
        self.id_mgr:ForecastMetaDataSeriesIdGenerator = ForecastMetaDataSeriesIdGenerator(container = self)

        # Generate a unique ID
        if(id_prefix is not None):
            self._meta_data[ForecastMetaDataFrameSchema.ID] = self.id_mgr.gen_rel_id(prefix = id_prefix)
        else:
            self._meta_data[ForecastMetaDataFrameSchema.ID] = self.id_mgr.gen_rel_id(length = 10)


        # init all meta_data attributes
        for attrib in ForecastMetaDataFrameSchema:
            if attrib in kwargs:
                if(attrib != ForecastMetaDataFrameSchema.MODEL):    # this is done because MODEL is not a meta_data schema but on object attribute
                    self._meta_data[attrib] = kwargs.get(attrib)
                else:
                    self._model: dict[ForecastMetaDataSeries] = kwargs.get(attrib)
            else:
                if(attrib != ForecastMetaDataFrameSchema.MODEL):    # this is done because MODEL is not a meta_data schema but on object attribute
                    # if there is already a value there, leave it alone, if not, create and explicitly set to null
                    if not attrib in self._meta_data.keys():
                        self._meta_data[attrib] = None


        # if no last_id was provided, calculate the last_id
        if not self.has_last_id():
            self.set_last_id(id = self._get_last_id(value_series_only = False))

        # calculate the last_value_id, last_value_id is the last_id which is of type value (not a Step_Init).  Usually that means that same
        # thing, but sometimes it can be different
        if self.has_last_id():
            if not self.has_last_value_id():
                self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = self._get_last_id(value_series_only = True)
        else:
            self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = None





    # iterator interface(s)
    # ---------------------
    def __iter__(self):
        return self._model.__iter__()


    def __next__(self) -> ForecastMetaDataSeries:
        return self._model.__next__()

    def keys(self):
        return(self._model.keys())
    
    def values(self):
        return(self._model.values())
    
    def items(self):
        return(self._model.items())
    



    # concat
    # Combine the meta_data from two or more ForecastMetaDataSeries or ForecastMetaDataFrames, designed to look similar to Pandas Concat
    #  
    # INPUTS:
    #   List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    #   verify_integrity (optional: False) - Ensure that no columns have the same key (otherwise, it will write over the previous col value)
    #   drop_dups (optional:  False) - Drops columns with the same key (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame will all the elements combined

    @staticmethod
    def concat(objs: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']], 
               verify_integrity = False, 
               drop_dups = False, 
               last_id: str = None, 
               **kwargs) -> Type['ForecastMetaDataFrame']:
        results_frame = ForecastMetaDataFrame()
        read_df = False

        if(objs is None or len(objs) < 1):
            raise ValueError("*  concat:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # grab the next object off the list
        for obj in objs:

            # handle the case that the next object is a ForecastMetaDataFrame
            if isinstance(obj, ForecastMetaDataFrame):

                # if this is our first ForecastMetaDataFrame to concat, we can copy all the non_model meta_data over before merging the columns
                if (not read_df):
                    results_frame = copy.deepcopy(obj)
                    read_df = True
                
                # if we have already read a ForecastMetaDataFrame previously in the list, then we need 
                # to confirm that all meta_data in this ForecastMetaDataFrame (other than MODEL) matches
                # since we can't combine forecasts of different types
                else:
                    # make sure that all the forecast meta_data agrees, otherwise we can't merge it
                    if (not verify_integrity) or (ForecastMetaDataFrame._check_meta_data(frame1 = obj, frame2 = results_frame)):
                        #results_frame = ForecastMetaDataFrame._append_cols(src_frame = obj, dest_frame = results_frame)
                        results_frame = ForecastMetaDataFrame._append_cols(src_frame = obj, dest_frame = results_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)
                    else:
                        raise ValueError("*  concat:  error ForecastMetaDataFrames do not have the same meta-data")
                    
            # handle the case that the this object is a ForecastMetaDataSeries
            else:
                results_frame = ForecastMetaDataFrame._append_col(src_series = obj, dest_frame = results_frame, verify_integrity = verify_integrity, drop_dups = drop_dups)

        if(last_id is not None):
            results_frame.set_last_id(last_id)

        # ZIV:  for the moment, don't let last_id be implicitly set, get everyone to explicitly set the value
        # else:
        #     results_frame._meta_data[ForecastMetaDataFrameSchema.LAST_ID] = results_frame._get_last_id()

        return(results_frame)
    
    
    # add_col_data_meta
    # Generate and add a ForecastMetaDataSeries to an ForecastMetaDataFrame

    @staticmethod
    def add_col_meta_data(frame: Type['ForecastMetaDataFrame'],
                          id: str,
                        #   display_name: str,
                        #   data_values: pd.Series,
                        #   step_type: ForecastDataSeriesMetaDataStepTypes,
                        #   action: ForecastDataSeriesMetaDataAction,
                        #   data_type: ForecastDataSeriesMetaDataDataType,
                        #   display_type: ForecastDataSeriesMetaDataDataType,
                        #   validation: List[Dict[ForecastDataSeriesMetaDataValidationSchema, Any]],
                        #   pred: List[str | int | float] = None,
                        #   args: Dict = None,
                        #   objs: List = None,
                          update_last_id = False,
                          verify_integrity: bool = True,
                          drop_dups: bool = False, 
                          **kwargs) -> Type['ForecastMetaDataFrame']:
        
        new_series = ForecastMetaDataSeries(id = id, **kwargs)
        frame._append_col(new_series, frame, verify_integrity = verify_integrity, drop_dups = drop_dups)

        if(update_last_id):
            frame.set_last_id(id = id)

        return(frame)
    

    # BUNCH OF HELPER FUNCTIONS TO QUICKLY GET THE OVERALL META-DATA FROM THE FRAME (the stuff that isn't going to change)

    # META_DATA

    # get_id
    # get the ID for the Frame
    def get_id(self) -> str:
        return(self._meta_data[ForecastMetaDataFrameSchema.ID])        


    # get_timescale
    # get the TIMESCALE of the frame
    def get_timescale(self) -> ForecastModelTimescale:
        return(self._meta_data[ForecastMetaDataFrameSchema.TIMESCALE])
        
    def set_timescale(self, value: ForecastMetaDataFrameSchema):
        self._meta_data[ForecastMetaDataFrameSchema.TIMESCALE] = value



    def get_start_year(self) -> int:
        return(self._meta_data[ForecastMetaDataFrameSchema.START_YEAR])
    
    def set_start_year(self, value: int):
        self._meta_data[ForecastMetaDataFrameSchema.START_YEAR] = value




    def get_start_month(self) -> int:
        return(self._meta_data[ForecastMetaDataFrameSchema.START_MONTH])
    
    def set_start_month(self, value: int):
        self._meta_data[ForecastMetaDataFrameSchema.START_MONTH] = value




    def get_num_periods(self) -> int:
        return(self._meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS])
    
    def set_num_periods(self, value: int):
        self._meta_data[ForecastMetaDataFrameSchema.NUM_PERIODS] = value


    
    def get_input_type(self) -> ForecastModelInputTypes:
        return(self._meta_data[ForecastMetaDataFrameSchema.INPUT_TYPE])
    
    def set_input_type(self, value: ForecastModelInputTypes):
        self._meta_data[ForecastMetaDataFrameSchema.INPUT_TYPE] = value




    # _MODEL

    # has_series
    # Returns true if the _model has 1 or more series in it
    def has_series(self) -> bool:
        if hasattr(self, "_model") and (self._model is not None) and (len(self._model) > 0):
            return True
        else:
            return False
        

    # get_series_ids()
    def get_series_ids(self) -> list[str]:
        if self.has_series():
            return list(self._model.keys())
        else:
            return None
        

    # get_num_series
    def get_num_series(self) -> int:
        series = self.get_series_ids()

        if series is None:
            return 0
        else:
            return len(series)
        

    # get_series
    # given a key or an index into the list of actions, return the corresponding Series object
    def get_series(self, id: int | str) -> ForecastMetaDataSeries:
        # if an int is provided, it's an index, if a string, it's a key
        # convert index to key
        if isinstance(id, int):
            #id = list(self._model.keys())[id]
            id = self.get_series_ids()[id]
        
        return self._model[id]
    

    # has_series_id
    # given a series id, return true if it's among the ForecastMetaDataSeries in model, false otherwise
    def has_series_id(self, id: str) -> bool:
        if(not self.has_series()):
            return False
        else:
            if(id in self.get_series_ids()):
                return True
            else:
                return False






    # _MODEL: LAST SERIES

    # get_last_series
    # Return the series which is pointed to by LAST_ID in _meta_data
    def get_last_series(self, value_series_only = False) -> ForecastMetaDataSeries:
        return self._model[self.get_last_id(value_series_only = value_series_only)]


    # get_last_id
    # Get the id of the last column
    def get_last_id(self, value_series_only: bool = False) -> str:

        # find LAST_ID
        if not value_series_only:
            if not self.has_last_id():
                raise ValueError(f"\n* get_last_id:  No last_id.")
            
            last_id: str = self._meta_data[ForecastMetaDataFrameSchema.LAST_ID]

            # check to make sure this last_id still exists in the model
            if not self.has_series_id(last_id):
                raise ValueError(f"\n* get_last_id: invalid last_id '{last_id}' not found in model keys {self.get_series_ids()}")
            
            # if everything checks out, return it
            return(last_id)
        
        # find LAST_VALUE_ID
        else:
            if not self.has_last_value_id():
                raise ValueError(f"\n* get_last_id:  value_series_only = True, but no last_value_id.")
            
            last_value_id: str = self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID]

            # check to make sure this last_value_id still exists in the model
            if not self.has_series_id(last_value_id):
                raise ValueError(f"\n* get_last_id: value_series_only - True, invalid last_value_id '{last_value_id}' not found in model keys {self.get_series_ids()}")
            
            # if everything checks out, return it
            return(last_value_id)
    

    # convenience wrapper around get_last_id
    def get_last_value_id(self) -> str:
        last_value_id = self.get_last_id(value_series_only = True)
        return(last_value_id)


    def has_last_id(self) -> bool:
        if (ForecastMetaDataFrameSchema.LAST_ID in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataFrameSchema.LAST_ID] is not None):
            return True
        else:
            return False
        
    def has_last_value_id(self) -> bool:
        if (ForecastMetaDataFrameSchema.LAST_VALUE_ID in self._meta_data.keys()) and (self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] is not None):
            return True
        else:
            return False

        
    # _get_last_id
    # NOTE:  This is a special function that doesn't depend on the value of ForecastMetaDataSeriesSchema.LAST_ID or LAST_VALUE_ID,
    # it grabs all the series ids, and takes the last ID in the order (or if value_series_only is true, starts at the end and exists at the).
    # first series which is a value_action.
    #   
    # This function should only be used during the __init__ function of ForecastMetaDataFrame to set its last_id / last_value_id, if it's not explicitly set 
    # and should not be otherwise used (use get_last_id and get_last_value_id respectively)
    def _get_last_id(self, value_series_only = False) -> str:
        last_id = None

        if self.has_series():
           last_id = list(self._model.keys())[-1]
        else:
            return None

        if not value_series_only:
            return(last_id)
        else:
            curr_series: ForecastMetaDataSeries = self.get_series(last_id)
        
            if curr_series.is_value_action():
                return(last_id)

            # algorithm for value_series_only check        
            #start_index = list(self._model.keys()).index(last_id) - 1
            start_index = self.get_series_ids().index(last_id) - 1

            if(start_index > 0): 
                for i in range(start_index-1, 1, -1):
                    curr_id: str = self.get_series_ids()[i]
                    curr_series: ForecastMetaDataSeries = self.get_series(curr_id)

                    if(curr_series.is_value_action()):
                        return(curr_id)
                    
            # raise ValueError(f"\n*  get_last_series:  error, no value series found from '{last_id}' backwards {self.get_series_ids()}.")
            return None




    # set_last_id
    # NOTE:  there is NO set_last_value_id function because value ID should never be set separately from last_id.
    #        last_value_id is simply the last_id that is a value_action (series), so whenever we set a last_id, we simply
    #        check if it's a value_id and set it there, if not, we leave as is
    def set_last_id(self, id: str):

        if (id is None):
            self._meta_data[ForecastMetaDataFrameSchema.LAST_ID] = None
            self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = None
            return

        if (self.has_series_id(id)):
            self._meta_data[ForecastMetaDataFrameSchema.LAST_ID] = id

            if(self.get_series(id).is_value_action()):
                self._meta_data[ForecastMetaDataFrameSchema.LAST_VALUE_ID] = id
        else:
            raise ValueError(f"\n* set_last_id:  error, id value '{id}' does not exist in {self.get_id()}: {self.get_series_ids}")


    # get_last_data_type
    def get_last_display_name(self, value_series_only = False) -> str:
        return(self.get_last_series(value_series_only = value_series_only).get_display_name())


    # get_last_data_type
    def get_last_data_type(self, value_series_only = False) -> ForecastDataSeriesMetaDataDataType:
        return(self.get_last_series(value_series_only = value_series_only).get_data_type())


    # get_last_display_type
    def get_last_display_type(self, value_series_only = False) -> ForecastDataSeriesMetaDataDataType:
        return(self.get_last_series(value_series_only = value_series_only).get_display_type())


    # get_last_step_type(self) 
    def get_last_step_type(self, value_series_only = False) -> ForecastDataSeriesMetaDataStepTypes:
        return(self.get_last_series(value_series_only = value_series_only).get_step_type())


    # get_last_values(self) 
    def get_last_values(self, value_series_only = False) -> list:
        return(self.get_last_series(value_series_only = value_series_only).get_data_values())


    # get first date in the forecast
    def get_first_date(self) -> datetime:
        start_year = self.get_start_year()
        start_month = self.get_start_month()
        timescale = self.get_timescale()
        return(gen_dates(start_year=start_year, start_month = start_month, num_years=1, time_scale = timescale)[0])


    # get last date in the forecast
    def get_last_date(self) -> datetime:
        start_year = self.get_start_year()
        start_month = self.get_start_month()
        num_periods = self.get_num_periods()
        timescale = self.get_timescale()


        if(self.get_timescale() == ForecastModelTimescale.MONTH):
            num_periods = num_periods / 12
        
        return(gen_dates(start_year=start_year, start_month = start_month, num_years=num_periods, time_scale = timescale)[-1])









    # PRIVATE HELPER FUNCTIONS
    # ------------------------



    # _get_list_of_last_ids
    # Go through 
    #  
    # INPUTS:
    #   List of ForecastMetaDataSeries or ForecastMetaDataFrames to combine
    # 
    # OUTPUTS:
    #   List of IDs

    @staticmethod
    def _get_list_of_last_ids(datas: list[ForecastMetaDataSeries | Type['ForecastMetaDataFrame']]) -> tuple[list[str], list[ForecastMetaDataSeries]]:
        list_of_ids = []
        list_of_forecast_series = []

        if(datas is None or len(datas) < 1):
            raise ValueError("*  _get_list_of_last_ids:  number of meta_data elements is zero or list is set to None, need at least 1 element.")

        # iterate over all the data_objects in datas grabbing the last id
        for data_obj in datas:

            # handle the case that the data_obj is a ForecastMetaDataFrame
            if isinstance(data_obj, ForecastMetaDataFrame):
                # get the id of the last key in the model (the last column of ForecastMetaDataSeries to be added)
                last_id = data_obj.get_last_id()
                list_of_forecast_series.append(data_obj.get_last_series())
                list_of_ids.append(last_id)

            # handle the case that the data_obj is ForecastMetaDataSeries
            else:
                list_of_forecast_series.append(data_obj)
                list_of_ids.append(data_obj.get_id())

        return(list_of_ids, list_of_forecast_series)
    

    # _get_display_data_type
    # Go through a list of ForecastMetaDataSeries and ensure they all have the same display_type and data_type, returning the common values
    #  
    # INPUTS:
    #   list_of_forecast_series - List of ForecastMetaDataSeries to check
    # 
    # OUTPUTS:
    #   (display_type, data_type) - the common display_type and data_type found in the list

    @staticmethod
    def _get_display_data_type(list_of_forecast_series: list[ForecastMetaDataSeries]) -> tuple[ForecastDataSeriesMetaDataDataType, ForecastDataSeriesMetaDataDataType]: #(display_type, data_type) = ForecastMetaDataFrame._get_display_data_type(list_of_forecast_series)
        
        last_display_type = None
        last_data_type = None

        for series in list_of_forecast_series:
            if series.get_display_type() is not None:
                if last_display_type is None:
                    last_display_type = series.get_display_type()
                elif last_display_type != series.get_display_type():
                    raise ValueError(f"*  _get_display_data_type:  inconsistent display_types found in list_of_forecast_series, '{last_display_type}' != '{series.get_display_type()}'")
            
            if series.get_data_type() is not None:
                if last_data_type is None:
                    last_data_type = series.get_data_type()
                elif last_data_type != series.get_data_type():
                    raise ValueError(f"*  _get_display_data_type:  inconsistent data_types found in list_of_forecast_series, '{last_data_type}' != '{series.get_data_type()}'")

        # if we never found a display_type or data_type, default to FLOAT
        if last_display_type is None:
            last_display_type = ForecastDataSeriesMetaDataDataType.FLOAT
        if last_data_type is None:
            last_data_type = ForecastDataSeriesMetaDataDataType.FLOAT

        return(last_display_type, last_data_type)
    

    # _check_meta_data
    # Check all the NONE-model meta-data between two frames and ensure they are the same (since we can't combine different types of forecasts)
    #  
    # INPUTS:
    #   frame1 - First frame to check
    #   frame2 - Second frame to check
    # 
    # OUTPUTS:
    #   Bool - True is matches, False if not

    @staticmethod
    def _check_meta_data(frame1: Type['ForecastMetaDataFrame'], frame2: Type['ForecastMetaDataFrame']) -> bool:

        # check the meta-data
        for attrib in ForecastMetaDataFrameSchema:

            # check that all the forecast attributes match, otherwise, can't merge the two forecasts
            # ignore LAST_ID since that is not expected to match, ignore MODEL because it's not an attribute
            # TODO:  add the ability to do this for .model as well, currently cannot be done since MODEL is an attribute not a dict
            if(attrib != ForecastMetaDataFrameSchema.MODEL and 
               attrib != ForecastMetaDataFrameSchema.LAST_ID and
               attrib != ForecastMetaDataFrameSchema.LAST_VALUE_ID):
                if(frame1._meta_data[attrib] != frame2._meta_data[attrib]):
                    return False
                
        return True
    
    

    # _copy_frame_meta_data
    # Copy all the non-model meta-data from one frame to another
    #  
    # INPUTS:
    #   src_frame - source of meta-data
    #   dest_frame - destination for meta-data
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's meta-data added
    #
    # NOTE:  This function is only called ONCE as part of this object concat() function.  It's just is just to copy for _meta_data
    #        portion of the ForecastMetaDataFrame object from the src to the dest.  It does not handle the _model portion.  That
    #        part is handled by a different fuction: _append_cols
    @staticmethod
    def _copy_frame_meta_data(src_frame: Type['ForecastMetaDataFrame'], dest_frame: Type['ForecastMetaDataFrame']) -> Type['ForecastMetaDataFrame']:

        for attrib in ForecastMetaDataFrameSchema:

            # copy all the common attributes over except MODEL (which isn't an attrbute), LAST_ID and LAST_VALUE_ID
            if(attrib != ForecastMetaDataFrameSchema.MODEL and 
               attrib != ForecastMetaDataFrameSchema.LAST_ID and
               attrib != ForecastMetaDataFrameSchema.LAST_VALUE_ID):
                dest_frame._meta_data[attrib] = src_frame._meta_data[attrib]
            
        return dest_frame
    



    # _append_cols (PLURAL)
    # Add all the cols in the src_frame to cols in the dest_frame, will overwrite existing cols in dest_frame if verify_integrity or drop_dups not set (see below)
    #  
    # INPUTS:
    #   src_frame - source of columns
    #   dest_frame - destination for columns
    #   verify_integrity (optional: False) - Raise an error if columns with the same key (in src) are attempted to be added to dest
    #   drop_dups (optional:  False) - Do not add columns from src to dest if a column with the same key already exists (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's columns added

    @staticmethod
    def _append_cols(src_frame: Type['ForecastMetaDataFrame'], 
                     dest_frame: Type['ForecastMetaDataSeries'],
                     update_last_id = False,
                     verify_integrity: bool = False, 
                     drop_dups: bool = False) -> Type['ForecastMetaDataFrame']:
        
        # check if the src_frame has _model
        if(not src_frame.has_series()):
            return dest_frame
        
        # iterate over each column and add it
        for col_key in src_frame.get_series_ids():
            dest_frame = ForecastMetaDataFrame._append_col(src_series = src_frame.get_series(col_key), 
                                                           dest_frame = dest_frame, 
                                                           update_last_id = update_last_id, 
                                                           verify_integrity = verify_integrity, 
                                                           drop_dups = drop_dups)

        return dest_frame



    # _append_col (SINGLE)
    # Add a ForecastMetaDataSeries src_series as a col in the dest_frame, will overwrite existing cols in dest_frame if verify_integrity or drop_dups not set (see below)
    #  
    # INPUTS:
    #   src_series - source of columns
    #   dest_frame - destination for columns
    #   verify_integrity (optional: False) - Raise an error if a key in the src_series already exists as a column in dest_frame
    #   drop_dups (optional:  False) - Do not add src_series if it's key already exists in dest_frame (if this is set, verify_integrity is ignored)
    # 
    # OUTPUTS:
    #   ForecastMetaDataFrame dest_frame with src_frame's columns added

    @staticmethod
    def _append_col(src_series: ForecastMetaDataSeries, dest_frame: Type['ForecastMetaDataFrame'], update_last_id = False, verify_integrity: bool = False, drop_dups: bool = False) -> Type['ForecastMetaDataFrame']:
        key = src_series.get_id()
        
        if(drop_dups or verify_integrity):
            key_exists  = dest_frame.has_series_id(key)

            if(drop_dups and key_exists):
                return dest_frame
            elif(verify_integrity and key_exists):
                raise ValueError(f"*  _append_col:  col '{key}' already exists in ForecastMetaDataFrame\n\n{dest_frame}\n\n{src_series}\n")
            
        dest_frame._model[key] = src_series
        
        if update_last_id:
            dest_frame.set_last_id(key)

        return dest_frame



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

        # print ForecastMetaDataFrame specific meta-data
        for attrib in ForecastMetaDataFrameSchema:
            if(attrib != ForecastMetaDataFrameSchema.MODEL):    # this is done because MODEL is not a meta_data schema but on object attribute
                results += f"\n{attrib} = {self._meta_data[attrib]}"

        # iterate on all columns and print their meta-data
        if self.has_series():
            for col_key in self.get_series_ids():
                results += f"\n\nCol '{col_key}':\n{self.get_series(col_key)}"

        return results

    
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
        return json.dumps(self, default=ForecastMetaDataJsonSerializer, indent=indent)



    # to_Data
    # Converts this object to a Data object (with a single text column) for use in Langflow
    #  
    # INPUTS:
    #   NA
    # 
    # OUTPUTS:
    #   Data object with a single text column containing the JSON serialization of this object

    def to_Data(self) -> Data:
        import unicodedata
        import orjson

        def normalize_text(text):
            return unicodedata.normalize("NFKD", text)

        text = orjson.loads(self.to_json())

        if isinstance(text, dict):
            text = {k: normalize_text(v) if isinstance(v, str) else v for k, v in text.items()}
        elif isinstance(text, list):
            text = [normalize_text(item) if isinstance(item, str) else item for item in text]

        return Data(data=text)
