import pandas as pd
import numpy as np

from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities
from langflow.base.forecasting_common.renderers.forecast_renderer_excel_TB import ForecastRendererExcelTB
from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket

def main():
    data_packet_file = "output.pickle"
    template_file = "excel_player_template.xlsx"
    output_file = "test_excel_player.xlsx"


    data_packet = ForecastDataPacket.unpickle_data_packet(data_packet_file)
    #data_packet = ForecastDataPacket.unpack_data_packet(data_packet)
    data_frame = data_packet[0].data["data"]
    meta_data = data_packet[0].data["meta_data"]
    # print(data_frame)
    #print(meta_data)
    #exit()

    # RENDER EXCEL VERSION OF MODEL
    # =============================
    render_excel = ForecastRendererExcelTB(data_frame = data_frame, meta_data = meta_data, output_location = output_file, template_location = template_file)
    render_excel.render_player()

#     exit()














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
