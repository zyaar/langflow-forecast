import pandas as pd
import numpy as np

from langflow.base.forecasting_common.models.forecast_meta_data import ForecastDataSeriesMetaDataDataType
from langflow.base.forecasting_common.renderers.excel.forecast_excel_validation_builder import ForecastExcelValidationRuleBuilder

def main():
    arg1 = "'Summary'!A1"
    arg1_type = ForecastDataSeriesMetaDataDataType.INT
    arg2 = "'Summary'!B1"
    arg2_type = ForecastDataSeriesMetaDataDataType.FLOAT
    arg3 = "'Summary'!C1"
    arg3_type = ForecastDataSeriesMetaDataDataType.PCT
    arg4 = "'Summary'!D1"
    arg4_type = ForecastDataSeriesMetaDataDataType.CURRENCY
    arg5 = "'Summary'!E1"
    arg5_type = ForecastDataSeriesMetaDataDataType.DATE

    validator = ForecastExcelValidationRuleBuilder()


   # Type checking formula
    formula1 = validator._build_custom_type_validation_formula(validation = arg1_type, arg = arg1)
    print(f"Type checking formula {arg1_type}: {formula1})")
    formula2 = validator._build_custom_type_validation_formula(validation = arg2_type, arg = arg2)
    print(f"Type checking formula {arg2_type}: {formula2})")
    formula3 = validator._build_custom_type_validation_formula(validation = arg3_type, arg = arg3)
    print(f"Type checking formula {arg3_type}: {formula3})")
    formula4 = validator._build_custom_type_validation_formula(validation = arg4_type, arg = arg4)
    print(f"Type checking formula {arg4_type}: {formula4})")
    formula5 = validator._build_custom_type_validation_formula(validation = arg5_type, arg = arg5)
    print(f"Type checking formula {arg5_type}: {formula5})") 


    # def _build_custom_comparison_validation_formula(self,
    #                                                 validation: ForecastExcelValidationRanges, 
    #                                                 arg1: str, arg1_type: ForecastDataSeriesMetaDataDataType,
    #                                                 arg2: str = None, arg2_type: ForecastDataSeriesMetaDataDataType = None,
    #                                                 arg3: str = None, arg3_type: ForecastDataSeriesMetaDataDataType = None) -> str:
        
    formula6 = validator._build_custom_comparison_validation_formula(validation = ForecastExcelValidationRuleBuilder.ForecastExcelValidationRanges.LT, 
                                                                     arg1 = arg1, arg1_type = arg1_type, 
                                                                     arg2 = arg2, arg2_type = arg2_type,
                                                                     arg3 = arg3, arg3_type = arg3_type)
    
    print(f"Comparison validation formula (less than): {formula6})")

    formula7 = validator._build_custom_comparison_validation_formula(validation = ForecastExcelValidationRuleBuilder.ForecastExcelValidationRanges.BETWEEN, 
                                                                     arg1 = arg1, arg1_type = arg1_type, 
                                                                     arg2 = arg2, arg2_type = arg2_type,
                                                                     arg3 = arg3, arg3_type = arg3_type)
    print(f"Comparison validation formula (between): {formula7})")
   













#     # setup variables
#     start_year = 1
#     start_month = 1
#     timescale = ForecastModelTimescale.YEAR
#     num_periods = 5

#     data_model_file = "~/output.json"
#     template_file = "excel_player_template.xlsx"
#     output_file = "test_excel_player.xlsx"



# # class ForecastModelSchema(str, Enum):
# #     ACTION = "action"
# #     ID = "id"
# #     PRED = "pred"
# #     DISPLAY_NAME = "display_name"


#     # SETUP TEST DATASET
#     # ==================
#     forecast_model = pd.read_json(data_model_file, orient='records', convert_dates=["dates"]) # load model

#     # add meta-data to simulate actual dataframe

#     # DATES (0)
#     # =====
#     forecast_model[forecast_model.columns[0]].attrs = {str(ForecastModelSchema.ACTION): str(ForecastModelActions.DATES),
#                                                        str(ForecastModelSchema.ID): str(forecast_model[forecast_model.columns[0]].name),
#                                                        str(ForecastModelSchema.DISPLAY_NAME): str(forecast_model[forecast_model.columns[0]].name),
#                                                        str(ForecastModelSchema.PRED): None}

#     # EPI (1)
#     # ===
#     forecast_model[forecast_model.columns[1]].attrs = {str(ForecastModelSchema.ACTION): str(ForecastModelActions.VALUE_INPUT),
#                                                        str(ForecastModelSchema.ID): str(forecast_model[forecast_model.columns[1]].name),
#                                                        str(ForecastModelSchema.DISPLAY_NAME): str(forecast_model[forecast_model.columns[1]].name),
#                                                        str(ForecastModelSchema.PRED): None}

#     # TREATMENTS PER MONTH
#     # ====================
#     values_to_sum = []

#     # treatments per cohort per month (2-7)
#     for i in range(2,8):
#         forecast_model[forecast_model.columns[i]].attrs = {str(ForecastModelSchema.ACTION): str(ForecastModelActions.VALUE_INPUT),
#                                                            str(ForecastModelSchema.ID): str(forecast_model[forecast_model.columns[i]].name),
#                                                            str(ForecastModelSchema.DISPLAY_NAME): str(forecast_model[forecast_model.columns[i]].name),
#                                                            str(ForecastModelSchema.PRED): str(None)}
#         values_to_sum.append(str(forecast_model[forecast_model.columns[i]].name))

#     # Treatment patients total (8)
#     forecast_model[forecast_model.columns[8]].attrs = {str(ForecastModelSchema.ACTION): str(ForecastModelActions.SUM_PRED),
#                                                        str(ForecastModelSchema.ID): str(forecast_model[forecast_model.columns[8]].name),
#                                                        str(ForecastModelSchema.DISPLAY_NAME): str(forecast_model[forecast_model.columns[8]].name),
#                                                        str(ForecastModelSchema.PRED): str(values_to_sum)}
    
#     # TREATMENTS PRODUCT_1 PER MONTH
#     # ==============================
#     values_to_sum = []

#     # Treatment products_1 per month cohort per month (9-14)
#     for i in range(9,15):
#         forecast_model[forecast_model.columns[i]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.VALUE_INPUT,
#                                                            ForecastModelSchema.ID: forecast_model[forecast_model.columns[i]].name,
#                                                            ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[i]].name,
#                                                            ForecastModelSchema.PRED: None}
#         values_to_sum.append(forecast_model[forecast_model.columns[i]].name)

#     # Treatment products_1 total (15)
#     forecast_model[forecast_model.columns[15]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.SUM_PRED,
#                                                        ForecastModelSchema.ID: forecast_model[forecast_model.columns[15]].name,
#                                                        ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[15]].name,
#                                                        ForecastModelSchema.PRED: values_to_sum}
    
#     values_to_product = [forecast_model[forecast_model.columns[15]].name]


#     # PRICE PRODUCT_1
#     # ===============

#     # product_1 price per month (16)
#     forecast_model[forecast_model.columns[16]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.VALUE_INPUT,
#                                                         ForecastModelSchema.ID: forecast_model[forecast_model.columns[16]].name,
#                                                         ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[16]].name,
#                                                         ForecastModelSchema.PRED: values_to_sum}
#     values_to_product.append(forecast_model[forecast_model.columns[16]].name)

#     # sub-total revenue product_1 (17)
#     forecast_model[forecast_model.columns[17]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.MULTIPLY_PRED,
#                                                         ForecastModelSchema.ID: forecast_model[forecast_model.columns[17]].name,
#                                                         ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[17]].name,
#                                                         ForecastModelSchema.PRED: values_to_product}
#     values_to_final_total = [forecast_model[forecast_model.columns[17]].name]


#     # TREATMENTS PRODUCT_2 PER MONTH
#     # ==============================
#     values_to_sum = []

#     # Treatment products_2 per month cohort per month (18-23)
#     for i in range(18,24):
#         forecast_model[forecast_model.columns[i]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.VALUE_INPUT,
#                                                            ForecastModelSchema.ID: forecast_model[forecast_model.columns[i]].name,
#                                                            ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[i]].name,
#                                                            ForecastModelSchema.PRED: None}
#         values_to_sum.append(forecast_model[forecast_model.columns[i]].name)

#     # Treatment products_24 total (24)
#     forecast_model[forecast_model.columns[15]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.SUM_PRED,
#                                                         ForecastModelSchema.ID: forecast_model[forecast_model.columns[24]].name,
#                                                         ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[24]].name,
#                                                         ForecastModelSchema.PRED: values_to_sum}
#     values_to_product = [forecast_model[forecast_model.columns[24]].name]
    

#     # PRICE PRODUCT_2
#     # ===============

#     # product_1 price per month (25)
#     forecast_model[forecast_model.columns[25]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.VALUE_INPUT,
#                                                         ForecastModelSchema.ID: forecast_model[forecast_model.columns[25]].name,
#                                                         ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[25]].name,
#                                                         ForecastModelSchema.PRED: values_to_sum}
#     values_to_product.append(forecast_model[forecast_model.columns[25]].name)

#     # sub-total revenue product_1 (26)
#     forecast_model[forecast_model.columns[26]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.MULTIPLY_PRED,
#                                                         ForecastModelSchema.ID: forecast_model[forecast_model.columns[26]].name,
#                                                         ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[26]].name,
#                                                         ForecastModelSchema.PRED: values_to_product}
#     values_to_final_total.append(forecast_model[forecast_model.columns[16]].name)


#     # FINAL TOTAL (27)
#     # ===========
#     forecast_model[forecast_model.columns[27]].attrs = {ForecastModelSchema.ACTION: ForecastModelActions.SUM_PRED,
#                                                         ForecastModelSchema.ID: forecast_model[forecast_model.columns[27]].name,
#                                                         ForecastModelSchema.DISPLAY_NAME: forecast_model[forecast_model.columns[27]].name,
#                                                         ForecastModelSchema.PRED: values_to_final_total}


#     print(f"forecast_model:\n{forecast_model}")

#     for i in range(len(forecast_model.columns)):
#         print(f"attr:{forecast_model[forecast_model.columns[i]].attrs}")


#     print(f"\n\n\n\n\n")



#     # RENDER EXCEL VERSION OF MODEL
#     # =============================
#     render_excel = ForecastRendererExcelTB(output_location = output_file, template_location = template_file)
#     render_excel.render_player(start_year, start_month, num_periods, timescale,  forecast_model = forecast_model)



#     exit()





























#     # OLDER TESTING CODE

#     render_excel = ForecastRendererExcelTB(output_location = output_file, template_location = template_file)
#     render_excel.render_player(start_year, start_month, num_periods, timescale,  forecast_model = "none")

#     render_excel._initialize_new_render()
    

#     exit()

#     render_excel = ForecastRendererExcelTB(output_location = output_file, template_location = template_file)
#     render_excel.render_player(start_year, start_month, num_periods, timescale,  forecast_model = "none")

#     render_excel._initialize_new_render()

#     # _build_new_renderer
#     render_excel._generate_core_tabs()

#     # try some of the EXCEL PRIMITIVES METHODS
#     id = 0
#     render_excel._add_values_row(id = "1",
#                                  tab_name = render_excel.EXCEL_REQUIRED_WORKBOOK_TABS[0], 
#                                  values = [1.5, 2.0, 3.25, 4.125], 
#                                  type = ForecastDataModelRenderType.INT, 
#                                  restriction = ForecastRenderInputRestrictions.READ_ONLY)
#     render_excel._add_values_row(id = "2",
#                                  tab_name = render_excel.EXCEL_REQUIRED_WORKBOOK_TABS[0], 
#                                  values = [1.5, 2.0, 3.25, 4.125], 
#                                  type = ForecastDataModelRenderType.INT, 
#                                  restriction = ForecastRenderInputRestrictions.READ_WRITE)
#     render_excel._add_values_row(id = "3",
#                                  tab_name = render_excel.EXCEL_REQUIRED_WORKBOOK_TABS[0], 
#                                  values = [1.5, 0, 3.25, 0], 
#                                  type = ForecastDataModelRenderType.INT, 
#                                  restriction = ForecastRenderInputRestrictions.TOKEN_CHECK)

#     render_excel._finalize_new_render(output_file)

#     exit()
    

#     render_excel = ForecastRendererExcelTB(output_location = output_file, template_location = template_file)

#     # render an excel player
#     # render_excel.render_player(start_year = start_year,
#     #                            start_month = start_month,
#     #                            timescale = timescale,
#     #                            num_periods =  num_periods,
#     #                            forecast_model = forecast_model,)


if __name__ == "__main__":
    main()
