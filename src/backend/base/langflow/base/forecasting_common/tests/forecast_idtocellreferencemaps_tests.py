from langflow.base.forecasting_common.builders.forecast_builder_excel_TB import IdToCellReferenceMaps, IdToCellReferenceMap

def main():
    map_id_1 = "ForecastMetaDataFrame_Ek8s0"
    map_id_2 = "ForecastMetaDataFrame_xGwbT"
    map_id_3 = "ForecastMetaDataFrame_cPK0Y"

    id_to_cell_maps = IdToCellReferenceMaps(default_num_elements = 5, default_ref_map_id=map_id_1)
    id_to_cell_maps.add(id = "month", tab_name = "Summary", cell_ref = "B2:B13")

    print(id_to_cell_maps.get_all_ids())

    id_to_cell_maps.add(ref_map_id = map_id_2, id = "date", tab_name = "Summary", cell_ref = "A1:A100", num_elements=100)

    print(id_to_cell_maps.get_all_ids())



if __name__ == "__main__":
    main()
