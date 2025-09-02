# from langflow.base.forecasting_common.models.forecast_meta_data import (ForecastMetaDataObject, 
#                                                                         ForecastMetaDataContainer,
#                                                                         ForecastMetaDataStep, 
#                                                                         ForecastMetaDataGroup,
#                                                                         ForecastDataSeriesMetaDataStepTypes,
#                                                                         ForecastDataSeriesMetaDataAction,
#                                                                         ForecastDataSeriesMetaDataDataType,
#                                                                         ForecastDataSeriesMetaDataAction,
#                                                                         ForecastDataSeriesMetaDataValidationSchema,
#                                                                         ForecastDataSeriesMetaDataValidateInputRestrictions
#                                                                         )

#from langflow.base.forecasting_common.models.new.forecast_meta_data_model import *
#from langflow.base.forecasting_common.models.new.forecast_meta_data_object import ForecastMetaDataObject
#from langflow.base.forecasting_common.models.new.forecast_meta_data_container import ForecastMetaDataContainer
from langflow.base.forecasting_common.models.new.forecast_meta_data_model import *


from langflow.base.forecasting_common.models.date_utils import gen_dates



def main():

    # # PART 1:  BASIC TESTS
    # # ====================
    # test_obj1 = ForecastMetaDataObject(id = "123", display_name = "test_obj1")
    # test_obj2 = ForecastMetaDataObject(id = "345", display_name = "test_obj2")
    # test_obj3 = ForecastMetaDataObject(id = "678", display_name = "test_obj3")
    # test_obj4 = ForecastMetaDataObject(id = "91011", display_name = "test_obj4")

    # test_step1 = ForecastMetaDataStep(id = "121314", display_name = "test_step1", step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY)
    # test_step2 = ForecastMetaDataStep(id = "151617", display_name = "test_step2", step_type = ForecastDataSeriesMetaDataStepTypes.SEGMENT)
    # test_step3 = ForecastMetaDataStep(id = "181920", display_name = "test_step3", step_type = ForecastDataSeriesMetaDataStepTypes.TREATMENT)
    # test_step4 = ForecastMetaDataStep(id = "212223", display_name = "test_step4", step_type = ForecastDataSeriesMetaDataStepTypes.PRICING)

    # test_group1 = ForecastMetaDataGroup(id = "242526", display_name = "test_group1")
    # test_group2 = ForecastMetaDataGroup(id = "272829", display_name = "test_group2")
    # test_group3 = ForecastMetaDataGroup(id = "303132", display_name = "test_group3")
    # test_group4 = ForecastMetaDataGroup(id = "333435", display_name = "test_group4")
    

    # print(test_obj1)
    # print(test_obj2)
    # print(test_obj3)
    # print(test_obj4)
    # print()

    # print(test_step1)
    # print(test_step2)
    # print(test_step3)
    # print(test_step4)
    # print()

    # print(test_group1)
    # print(test_group2)
    # print(test_group3)
    # print(test_group4)
    # print("\n\n\n")

    # create some structures
    # test_group1[test_obj1.id] = test_obj1
    # test_group1[test_obj2.id] = test_obj2
    # test_group2[test_obj3.id] = test_obj3
    # test_group1[test_group2.id] = test_group2


    # test_step1[test_group1.id] = test_group1
    # test_step1[test_obj4.id] = test_obj4

    # print(test_step1)



    # PART 2:  TEST THE HIERARCHY AND THE BASICS OF THE FACTORY
    # =========================================================

    # create new step
    test_step1 = ForecastMetaDataStep(display_name = "Step 1", step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY)

    # add default attributes
    test_step1.default_data_type = ForecastDataSeriesMetaDataDataType.FLOAT
    test_step1.default_display_type = ForecastDataSeriesMetaDataAction.INPUT
    test_step1.default_validation = [{ForecastDataSeriesMetaDataValidationSchema.INPUT_RESTRICTION: ForecastDataSeriesMetaDataValidateInputRestrictions.READ_ONLY}],


    test_step2 = test_step1.add_epidemiology("Cancer patients")
    print(test_step1)
    print("\n\n\n--------------------------")

    with test_step1.add_treatment("Cancer treatment") as s:
        s.add_group("Total patients by treatment month")
        s.add_group("Total patients leaving by treatment month")

        with s.add_group("Total 'Product 1' Rx for each month by treatment month") as g:
            g.add_group("Subset 1")
            with g.add_group("Subset 2") as g2:
                g2.add_dates("Dates (end-of)")

    print(test_step1.to_json())
    print("--------------------------")



    # PART 3:  TEST THE TIME SAVING FEATURES OF THE FACTORY
    # =====================================================

    # # create new step
    # with ForecastMetaDataStep("Step 1", step_type = ForecastDataSeriesMetaDataStepTypes.EPIDEMIOLOGY, id_prefix = "Epidemiology") as s:
    #     s.default_data_type = ForecastDataSeriesMetaDataDataType.FLOAT
    #     s.default_display_type = ForecastDataSeriesMetaDataDataType.INT

    #     forecast_dates = gen_dates(start_year = 2027, num_years = 5)
    #     s.add_dates("Dates (end-of):", forecast_dates)
    #     s.add_input("# of Cancer patients total", [1000, 2000, 3000, 4000, 5000])
    #     print(s.to_json())


    
    #with test_step1.add_input("# of Cancer Patients total", data_values: list | pd.Series = None, validation:  list[dict] = None, ranges: list[ForecastMetaDataRange] = None, pred: list[str] = None, args: dict = None, objs: dict = None, update_last_id: bool = None, verify_integrity: bool = None, drop_dups: bool = None, id: str = None, **kwargs) -> Type['ForecastMetaDataSeries']:

    #print(test_step1.to_json())





        #s.default_data_type = ForecastDataSeriesMetaDataDataType.FLOAT
        #s.default_display_type = ForecastDataSeriesMetaDataDataType.FLOAT

    #def add_dates(self, display_name: str, id_postfix: str = None, data_values: list | pd.Series = None, drop_dups: bool = None, id: str = None, **kwargs) -> Type['ForecastMetaDataSeries']:





if __name__ == "__main__":
    main()



