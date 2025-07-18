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
    data_frame = data_packet[0].data["data"]
    meta_data = data_packet[0].data["meta_data"]

    #meta_data.model = None
    #meta_data.meta_data = None

    #print(meta_data.to_json())
    print(meta_data.to_Data())


if __name__ == "__main__":
    main()
