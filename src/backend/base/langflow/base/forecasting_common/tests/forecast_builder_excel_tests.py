import pandas as pd
import numpy as np

from langflow.base.forecasting_common.constants import FORECAST_COMMON_MONTH_NAMES_AND_VALUES, ForecastModelInputTypes, ForecastModelTimescale
from langflow.base.forecasting_common.models.forecast_data_model import ForecastDataModel
from langflow.base.forecasting_common.forms.forecast_form_updater import ForecastFormUpdater
from langflow.base.forecasting_common.forms.forecast_form_trigger_calc import ForecastFormTriggerCalc
from langflow.base.forecasting_common.forms.forecast_form_model_utilities import ForecastFormModelUtilities
from langflow.base.forecasting_common.builders.forecast_builder_excel_TB import ForecastBuilderExcelTB
from langflow.base.forecasting_common.models.forecast_data_packet import ForecastDataPacket

def main():
    working_dir = "./output/"
    data_packet_file = working_dir + "output.pickle"
    json_output_file = working_dir + "meta_data.json"
    template_file = working_dir + "excel_player_template.xlsx"
    output_file = working_dir + "test_excel_player.xlsx"


    data_packet = ForecastDataPacket.unpickle_data_packet(data_packet_file)
    data_frame = data_packet.data["data"]
    meta_data = data_packet.data["meta_data"]

    # with open(json_output_file, "w") as f:
    #     f.write(meta_data.to_json())


    # RENDER EXCEL VERSION OF MODEL
    # =============================
    render_excel = ForecastBuilderExcelTB(data_frame = data_frame, meta_data = meta_data, output_location = output_file, template_location = template_file)
    render_excel.build_player()




if __name__ == "__main__":
    main()
