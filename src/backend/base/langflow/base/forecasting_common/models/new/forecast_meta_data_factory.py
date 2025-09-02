# ==============
# COMMON IMPORTS
# ==============

from .forecast_meta_data_common import *
from .forecast_meta_data_series import ForecastMetaDataSeries
from .forecast_meta_data_container import ForecastMetaDataGroup, ForecastMetaDataStep
from .forecast_meta_data_range import ForecastMetaDataRange




# =======
# CLASSES
# =======

class ForecastMetaDataFactory():

    # FACTORY METHODS TO CREATE DIFFERENT CONTAINER TYPES
    # ---------------------------------------------------

    # create different CONTAINER types
    # --------------------------------

    # GROUP

    # add_group
    # Create a new GROUP object, in the factory
    #  
    # INPUTS:
    #   ident (optional: 4) - number of spaces to indent the JSON
    # 
    # OUTPUTS:
    #   JSON string
    def add_group(self,
                  display_name: str,
                  id: str = None,
                  new_defaults: bool = False,
                  new_id_gen: bool = False) -> Type['ForecastMetaDataGroup']:
        
        # pass the id_generator to the child
        if not new_id_gen:
            id_generator = self.id_generator
        else:
            id_generator = None

        new_group = ForecastMetaDataGroup(display_name = display_name, 
                                          id = id, 
                                          parent = self, # set parent to current container 
                                          id_generator = id_generator)
        self._model[new_group.id] = new_group

        # pass defaults through
        if not new_defaults:
            new_group.default_data_type = self.default_data_type
            new_group.default_display_type = self.default_display_type
            new_group.default_validation = self.default_validation
            new_group.default_pred = self.default_pred
            new_group.default_update_last_id = self.default_update_last_id
            new_group.default_verify_integrity = self.default_verify_integrity
            new_group.default_drop_dups = self.default_drop_dups

        return(new_group)


    # STEP

    # _add_step
    # Hidden method to create a new STEP object in the factory
    #  
    # INPUTS:
    #   display_name
    #   step_type
    #   ident (optional: 4) - number of spaces to indent the JSON
    # 
    # OUTPUTS:
    #   JSON string

    def _add_step(self,
                  display_name: str,
                  step_type: ForecastDataSeriesMetaDataStepTypes,
                  id_prefix: str = None, 
                  id: str = None,
                  new_defaults: bool = False,
                  new_id_gen: bool = False) -> Type['ForecastMetaDataStep']:
        
        # pass the id_generator to the child
        if not new_id_gen:
            id_generator = self.id_generator
        else:
            id_generator = None

        new_step = ForecastMetaDataStep(display_name = display_name, 
                                        step_type = step_type, 
                                        id_prefix = id_prefix, 
                                        id = id, 
                                        parent = self, # set parent to current container 
                                        id_generator = id_generator)

        self._model[new_step.id] = new_step


        # pass defaults through
        if not new_defaults:
            new_step.default_data_type = self.default_data_type
            new_step.default_display_type = self.default_display_type
            new_step.default_validation = self.default_validation
            new_step.default_pred = self.default_pred
            new_step.default_update_last_id = self.default_update_last_id
            new_step.default_verify_integrity = self.default_verify_integrity
            new_step.default_drop_dups = self.default_drop_dups

        return(new_step)
    

    # define the PUBLIC step creation methods that we offer
    def add_epidemiology(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Epidemiology", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY, id = id)

    def add_pricing(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Pricing", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.PRICING, id = id)

    def add_delay(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Delay", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.DELAY, id = id)

    def add_pop_cut(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Pop_cut", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.POPULATION_CUT, id = id)

    def add_segment(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Segment", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT, id = id)

    def add_summation(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Summation", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.SUMMATION, id = id)

    def add_treatment(self, display_name: str, id: str = None, new_defaults = False, new_id_gen = False) -> Type['ForecastMetaDataStep']:
        return self._add_step(id_prefix = "Treatment", display_name = display_name, step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT, id = id)




    # create different SERIES / ACTION types
    # --------------------------------------


    # _add_action
    # Hidden method to create a new action object in the factory
    #  
    # INPUTS:
    #   display_name
    #   step_type
    #   ident (optional: 4) - number of spaces to indent the JSON
    # 
    # OUTPUTS:
    #   JSON string
    
    def _add_action(self,
                    action_pred_type: ForecastDataSeriesMetaDataActionPredTypes,
                    id_prefix: str,
                    display_name: str,
                    action: ForecastDataSeriesMetaDataAction,
                    data_type: ForecastDataSeriesMetaDataDataType,
                    display_type: ForecastDataSeriesMetaDataDataType,
                    data_values: list | pd.Series = None,
                    validation:  list[dict] = None,
                    ranges: list[ForecastMetaDataRange] = None,
                    pred: list[str] = None,
                    args: dict = None,
                    objs: dict = None,
                    update_last_id: bool = None,
                    verify_integrity: bool = None,
                    drop_dups: bool = None,
                    id: str = None,
                    id_postfix: str = None,
                    **kwargs) -> Type['ForecastMetaDataSeries']:
        
        # generate an ID
        if(id is None):
            id = f"{self.id_generator.gen_rel_id(prefix = id_prefix)}_{id_postfix}"



        # set any DEFAULT values that need to be set

        # data_type
        if(data_type is None):
            data_type = self.default_data_type

        # display_type
        if(display_type is None):
            display_type = self.default_display_type

        # validation
        if(validation is None):
            validation = self.default_validation

        # pred
        if(pred is None):
            pred = self.default_pred

        # update_last_id
        if(update_last_id is None):
            update_last_id = self.default_update_last_id

        # verify_integrity
        if(verify_integrity is None):
            verify_integrity = self.default_verify_integrity

        # drop_dups
        if(drop_dups is None):
            drop_dups = self.default_drop_dups



        # determine if we can automatically add any information to the pred based on the
        # ForecastDataSeriesMetaDataActionPredTypes.  This is a major convience as many of our actions form:
        # take some ACTION on the last_id(), so if we can figure out where to auto-fill that last id, the 
        # using the api will be much clear and less error prone

        # class ForecastDataSeriesMetaDataActionPredTypes(str, Enum):
        #     NO_PREDS = "no_preds" # this action does not take any preds (i.e. DATES)
        #     ONE_PRED = "one_pred" # this action take ONE AND ONLY ONE pred (i.e. YEAR_TO_MONTH, MONTH_TO_YEAR)
        #     TWO_OR_MORE_PREDS = "two_or_more_preds" # this action takes AT LEAST TWO preds (i.e SUM, PROD, SUB, etc.)


        #     INPUTS
        #     DATES
        #     VALUES = "values" # display read-only values
        #     SUM = "sum" # sum up a series of col ids (in preds) or constants
        #     TOTAL = "total" # same as sum, but may be treated different visually
        #     PROD = "prod" # multiply a series of col ids (preds) or constants
        #     SUB = "sub"  # subtract a series of col ids (preds) or constants
        #     YEAR_TO_MONTH = "year_to_month" # convert a yearly series to monthly
        #     MONTH_TO_YEAR = "month_to_year" # convert a monthly series to yearly

        if (pred is None):
            num_preds = 0
        else:
            num_preds = len(pred)

        match action_pred_type:
            # INPUTS, DATES, VALUES
            case ForecastDataSeriesMetaDataActionPredTypes.NO_PREDS:
                pass

            # YEAR_TO_MONTH, MONTH_TO_YEAR
            case ForecastDataSeriesMetaDataActionPredTypes.ONE_PRED:
                if num_preds == 0:
                    pred = self.last_id()

            # SUM, TOTAL, PROD, SUB
            case ForecastDataSeriesMetaDataActionPredTypes.TWO_OR_MORE_PREDS:
                if num_preds < 2:
                    pred.insert(0, self.last_id())


        # create the new action / series
        new_action = ForecastMetaDataSeries(id = id,
                                            display_name = display_name, 
                                            action = action,
                                            ranges = ranges,
                                            data_type = data_type,
                                            display_type = display_type,
                                            data_values = data_values,
                                            validation =  validation,
                                            pred = pred,
                                            args = args,
                                            objs = objs,
                                            parent = self, # set the parent to this container
                                            **kwargs)
        
        # TODO: verify_integrity implementation

        # TODO: drop_dups implementation

        self._model[new_action.id] = new_action

        # update_last_id
        # check if we should update the last_id for this (this means its a row of values for later use)
        if(update_last_id):
            self.last_id = new_action.id

            # if this is a legit last_id, then it can also serve as a first_id, IF NO first_id is already defined
            if self.first_id is None:
                self.first_id = new_action.id

        return(new_action)
    


    # define the PUBLIC action creation methods that we offer
    # NOTE ON CODING STYLE HERE:
    #   since these are all very long with lots of the same args being defined / passed, for each action,
    #   have used a single line instead of one line per argument.  HOWEVER:  in the _add_action() call, 
    #   I've moved the arguments to the front which change from action to action, to make them easier to see

    # add_action:
    #                 display_name: str,
    #                 pred: list[str] = None,
    #                 data_values: list | pd.Series = None,

    #                 BLANK MOST OF THE TIME
    #                 ranges: list[ForecastMetaDataRange] = None,
    #                 id_postfix: str = None,

    #                 DEFAULTS TYPICALLY COVER
    #                 data_type: ForecastDataSeriesMetaDataDataType,
    #                 display_type: ForecastDataSeriesMetaDataDataType,
    #                 validation:  list[dict] = None,
    #                 args: dict = None,
    #                 objs: dict = None,
    #                 update_last_id: bool = None,
    #                 verify_integrity: bool = None,
    #                 drop_dups: bool = None,
    #                 id: str = None,
    #                 **kwargs) -> Type['ForecastMetaDataSeries']:
    
    # def _add_action(self,
    #                 action_pred_type: ForecastDataSeriesMetaDataActionPredTypes,
    #                 id_prefix: str,
    #                 display_name: str,
    #                 action: ForecastDataSeriesMetaDataAction,
    #                 data_type: ForecastDataSeriesMetaDataDataType,
    #                 display_type: ForecastDataSeriesMetaDataDataType,
    #                 data_values: list | pd.Series = None,
    #                 validation:  list[dict] = None,
    #                 ranges: list[ForecastMetaDataRange] = None,
    #                 pred: list[str] = None,
    #                 args: dict = None,
    #                 objs: dict = None,
    #                 update_last_id: bool = None,
    #                 verify_integrity: bool = None,
    #                 drop_dups: bool = None,
    #                 id: str = None,
    #                 id_postfix: str = None,
    #                 **kwargs) -> Type['ForecastMetaDataSeries']:


    def _add_action(self,
                    action_pred_type: ForecastDataSeriesMetaDataActionPredTypes, id_prefix: str, display_name: str, action: ForecastDataSeriesMetaDataAction, data_type: ForecastDataSeriesMetaDataDataType, display_type: ForecastDataSeriesMetaDataDataType, data_values: list | pd.Series = None, validation:  list[dict] = None,
                    ranges: list[ForecastMetaDataRange] = None,
                    pred: list[str] = None,
                    args: dict = None,
                    objs: dict = None,
                    update_last_id: bool = None,
                    verify_integrity: bool = None,
                    drop_dups: bool = None,
                    id: str = None,
                    id_postfix: str = None,
                    **kwargs) -> Type['ForecastMetaDataSeries']:
        pass


    # DATES
    def add_dates(self, display_name: str, values: pd.Series | list, id_postfix: str = None, id: str = None):
        self._add_step()

    # INPUTS
    def add_input(self, display_name: str, values: pd.Series | list, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any], id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, id: str = None, update_last_id: bool = True):
        pass

    # VALUES
    def add_values(self, display_name: str, values: pd.Series | list, id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.FLOAT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any] = FORECAST_READ_ONLY_VALIDATION, id: str = None, update_last_id: bool = True):
        pass

    # COPY
    def add_copy(self, display_name: str, pred: str, id_postfix: str = None, update_last_id: bool = True):
        pass

    # SUM
    def add_sum(self, display_name: str, pred: list[str], id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.FLOAT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any] = FORECAST_READ_ONLY_VALIDATION, id: str = None, update_last_id: bool = True):
        pass

    # PROD
    def add_prod(self, display_name: str, pred: list[str], id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.FLOAT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any] = FORECAST_READ_ONLY_VALIDATION, id: str = None, update_last_id: bool = True):
        pass

    # SUB
    def add_sub(self, display_name: str, pred: list[str], id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.FLOAT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any] = FORECAST_READ_ONLY_VALIDATION, id: str = None, update_last_id: bool = True):
        pass

    # YEAR_TO_MONTH
    def add_ytm(self, display_name: str, pred: str, id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.FLOAT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any] = FORECAST_READ_ONLY_VALIDATION, id: str = None, update_last_id: bool = True):
        pass

    # MONTH_TO_YEAR
    def add_mty(self, display_name: str, pred: str, id_postfix: str = None, data_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.FLOAT, display_type: ForecastDataSeriesMetaDataDataType = ForecastDataSeriesMetaDataDataType.INT, validation: dict[ForecastDataSeriesMetaDataValidationSchema, Any] = FORECAST_READ_ONLY_VALIDATION, id: str = None, update_last_id: bool = True):
        pass

        


    # def add_input(self, display_name: str, id_postfix: str, data_type: ForecastDataSeriesMetaDataDataType, display_type: ForecastDataSeriesMetaDataDataType, data_values: list | pd.Series = None, validation:  list[dict] = None, ranges: list[ForecastMetaDataRange] = None, pred: list[str] = None, args: dict = None, objs: dict = None, update_last_id: bool = None, verify_integrity: bool = None, drop_dups: bool = None, id: str = None, **kwargs) -> Type['ForecastMetaDataSeries']:
    #     id_postfix = f"Inputs_{id_postfix}" if (id_postfix is not None) else "Inputs"
    #     return self._add_action(action_pred_type = ForecastDataSeriesMetaDataActionPredTypes.NO_PREDS, action = ForecastDataSeriesMetaDataAction.INPUT, validation = validation, id_postfix = id_postfix, id_prefix = self.id, data_type = data_type, display_type =  display_type, data_values = data_values, ranges = ranges, pred = pred, args = args, objs = objs, update_last_id = update_last_id, verify_integrity = verify_integrity, drop_dups = drop_dups, id = id, **kwargs)

    # # DATES
    # def add_dates(self, display_name: str, data_values: list | pd.Series, id_postfix: str = None, drop_dups: bool = True, id: str = None, **kwargs) -> Type['ForecastMetaDataSeries']:
    #     id_postfix = f"Dates_{id_postfix}" if (id_postfix is not None) else "Dates"
    #     return self._add_action(action_pred_type = ForecastDataSeriesMetaDataActionPredTypes.NO_PREDS, action = ForecastDataSeriesMetaDataAction.DATES, data_type = ForecastDataSeriesMetaDataDataType.DATE, display_type =  ForecastDataSeriesMetaDataDataType.DATE, validation = FORECAST_READ_ONLY_VALIDATION, id_postfix = id_postfix, id_prefix = self.id, display_name = display_name, ranges = None, pred = None, args = None, objs = None, update_last_id = False, drop_dups = drop_dups, verify_integrity = False, data_values = data_values, id = id, **kwargs)

    # # VALUES
    # def add_values(self, display_name: str, id_postfix: str, data_type: ForecastDataSeriesMetaDataDataType, display_type: ForecastDataSeriesMetaDataDataType, data_values: list | pd.Series = None, validation:  list[dict] = None, ranges: list[ForecastMetaDataRange] = None, pred: list[str] = None, args: dict = None, objs: dict = None, update_last_id: bool = None, verify_integrity: bool = None, drop_dups: bool = None, id: str = None, **kwargs) -> Type['ForecastMetaDataSeries']:
    #     id_postfix = f"Values_{id_postfix}" if (id_postfix is not None) else "Values"
    #     return self._add_action(action_pred_type = ForecastDataSeriesMetaDataActionPredTypes.NO_PREDS, id_postfix = id_postfix, action = ForecastDataSeriesMetaDataAction.INPUT, validation = validation, id_prefix = self.id, data_type = data_type, display_type =  display_type, data_values = data_values, ranges = ranges, pred = pred, args = args, objs = objs, update_last_id = update_last_id, verify_integrity = verify_integrity, drop_dups = drop_dups, id = id, **kwargs)
