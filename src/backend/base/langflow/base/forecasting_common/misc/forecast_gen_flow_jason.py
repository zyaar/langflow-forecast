import orjson
from uuid import uuid4
from pathlib import Path
import json

from langflow.serialization.serialization import serialize, serialize_or_str
from langflow.graph import Graph
from langflow.graph.edge.schema import EdgeData
from langflow.api.v1.schemas import InputValueRequest
from langflow.custom import Component
from langflow.components.forecasting_TB.forecast_segment_TB import ForecastSegmentTB
from langflow.components.forecasting_TB.forecast_treatment_shares_TB import ForecastTreatmentSharesTB
from langflow.components.forecasting_TB.forecast_treatment_TB import ForecastTreatmentTB
from langflow.base.forecasting_common.components.forecast_sum_input_TB import ForecastSumInputTB




class forecast_flow_generator():
    # constants
    master_attributes: list = ["data", "description", "endpoint_name", "id", "is_component", "last_tested_version", "name", "tags"]
    data_attributes: list = ["nodes", "edges", "viewport"]
    viewport_attributes: list = ["x", "y", "zoom"]

    component_default_height: int = 267
    component_default_width: int = 384
    component_default_type: str = "genericNode"

    #edge_id_prefix = "reactflow__edge-"
    edge_id_prefix = "xy-edge__"
    edge_id_separator = "-"
    edge_handle_separator = "|"
    edge_classname = "animated-edge"
    edge_selected = False
    edge_default_in = ForecastSumInputTB.VAR_IN_NAME # "forecasts_in"

    default_x: float = 163.489748422812
    default_y: float = 843.5296828471614
    default_zoom: float = 0.3031433133020798

    # LLM to Forecast Component conversion table
    type_to_component_map: dict = {
        "segment": ForecastSegmentTB,
        "treatment": ForecastTreatmentTB,
        "treatment_choice": ForecastTreatmentSharesTB,
    }


    # LLM to Forecast Component class name conversion table
    type_to_class_name_map: dict = {
        "segment": "SegmentTB",
        "treatment": "TreatmentTB",
        "treatment_choice": "TreatmentSharesTB",
    }


    # master loop
    def forecast_gen_flow_json(self, json_input: dict, return_str: bool = True) -> dict | str:
        self.node_dict = {}
        self.edge_dict = {}
        self.node_classname_dict = {}

        self.output_graph = Graph()

        for attribute in self.master_attributes:
            match attribute:
                case "data":
                    self._dispatch_data(json_input)

                case "description":
                    self.output_graph.description = self._dispatch_description(json_input)

                case "endpoint_name":
                    pass
                    #self.output_graph ??? = self._dispatch_endpoint_name(json_input)

                case "id":
                    self.output_graph.flow_id = self._dispatch_id(json_input)

                case "is_component":
                   pass
                   #self.output_graph ??? = self._dispatch_is_component(json_input)

                case "last_tested_version":
                    pass
                    #self.output_graph ??? = self._dispatch_last_tested_version(json_input)

                case "name":
                    self.output_graph.flow_name = self._dispatch_name(json_input)

                case "tags":
                    pass
                    # self.output_graph ??? = self._dispatch_tags(json_input)

                case _:
                    raise ValueError(f"\n* forecast_gen_flow_json:  invalid master attribute '{attribute}'.")
                
        # return orjson.dumps(output_json, option=orjson.OPT_INDENT_2).decode('utf-8')
        json_output = self.output_graph.dump()

        if(return_str):
            return orjson.dumps(json_output, option=orjson.OPT_INDENT_2).decode('utf-8')
        else:
            return json_output
                


    # MASTER ATTRIBUTES
    # =================

    # handle generation of 'data' attribute
    def _dispatch_data(self, json_input: dict) -> dict:
        output_dict: dict = {}

        for attribute in self.data_attributes:
            match attribute:
                case "edges":
                    self._dispatch_data_edges(json_input)
                case "nodes":
                    self._dispatch_data_nodes(json_input)
                case "viewport":
                    self._dispatch_data_viewport(json_input)
                case _:
                    raise ValueError(f"\n* forecast_gen_flow_json:  invalid data attribute '{attribute}'.")
                    

        return output_dict


    # handle generation of 'descripition' attribute
    def _dispatch_description(self, json_input: dict) -> str:
        return "TODO:  description"


    # handle generation of 'endpoint_name' attribute
    def _dispatch_endpoint_name(self, json_input: dict) -> str:
        return None


    # handle generation of 'id' attribute
    def _dispatch_id(self, json_input: dict) -> str:
        return uuid4()


    # handle generation of 'is_component' attribute
    def _dispatch_is_component(self, json_input: dict) -> bool:
        return False


    # handle generation of 'last_tested_version' attribute
    def _dispatch_last_tested_version(self, json_input: dict) -> str:
        return None


    # handle generation of 'name' attribute
    def _dispatch_name(self, json_input: dict) -> str:
        return "TODO:  name"


    # handle generation of 'tags' attribute
    def _dispatch_tags(self, json_input: dict) -> list[str]:
        return []



    # DATA ATTRIBUTES
    # ===============

    # handle generation of 'nodes' data attribute
    def _dispatch_data_nodes(self, json_input: dict) -> None:

        for node in json_input["steps"]:
            # create new node
            new_component = self.type_to_component_map[node["type"]](_id = node["id"])
            new_component.display_name = node["name"]
            new_component.description = node["description"]

            # add node to graph
            new_node = self.output_graph.add_component(new_component)

            # get the vertex of the node
            vertex = self.output_graph.get_vertex(new_node)

            # add size and position data
            vertex.full_data["width"] = self.component_default_width
            vertex.full_data["height"] = self.component_default_height
            vertex.full_data["type"] = self.component_default_type


            # add node to lookup dict (for calculating edges later)
            self.node_dict[node["id"]] = new_component
            self.node_classname_dict[node["id"]] = self.type_to_class_name_map[node["type"]]

        return


    # handle generation of 'edges' data attribute
    def _dispatch_data_edges(self, json_input: dict) -> None:
        for node in json_input["steps"]:
            if("next steps" in node and node is not None and len(node) > 0):
                src_node = self.node_dict[node["id"]]
                src_node_classname = self.node_classname_dict[node["id"]]
                src_node_output_name = src_node.outputs[0].name
                
                for edge in node["next steps"]:
                    dst_node = self.node_dict[edge]
                    dst_node_classname = self.node_classname_dict[edge]

                    new_edge = self.add_edge(src_node_classname, src_node, src_node_output_name, dst_node_classname, dst_node, self.edge_default_in)
                    self.output_graph.add_edge(new_edge) # TODO:  ZIV fix input names
                    self.output_graph._build_graph()

        return


    # handle generation of 'viewport' data attribute
    def _dispatch_data_viewport(self, json_input: dict) -> dict:
        return {"x": self.default_x, "y": self.default_y, "zoom": self.default_zoom}



    # HELPERS
    # =======

    #return json.dumps(edge_dict).replace('"', "œ")

    def add_edge(self, source_class: str, source_node: dict, src_output_id: str, target_class: str, target_node: dict, target_input_id: str) -> EdgeData:
        source_handle = self.edge_source_handle_gen(source_node, source_class, src_output_id) # epi_forecast_model
        target_handle = self.edge_target_handle_gen(target_node, target_class, target_input_id) # forecasts_in
        edge_id = self.edge_id_gen(source_node, source_handle, target_node, target_handle)

        source_handle = source_handle.replace('"', "œ")
        target_handle = target_handle.replace('"', "œ")
        edge_id = edge_id.replace('"', "œ")

        new_edge: EdgeData = {
            "animated": False,
            "className": "",
            "data": {
                "sourceHandle": {
                    "dataType": source_class,
                    "id": source_node._id,
                    "name": src_output_id,
                    "output_types": [
                        "Data"
                    ]
                },
                "targetHandle": {
                    "fieldName": target_input_id,
                    "id": target_node._id,
                    "inputTypes": [
                        "Data"
                    ],
                    "type": "other"
                }
            },

            "id": edge_id,
            "selected": False,
            "source": source_node._id,
            "sourceHandle": source_handle,
            "target": target_node._id,
            "targetHandle": target_handle,
        }

        # new_edge = {
        #     "source": source_node["id"],
        #     "sourceHandle": src_output_id,
        #     "target": target_node["id"],
        #     "targetHandle": target_input_id,
        #     "id": f"{self.edge_id_prefix}{source_node['id']}{src_output_id}{self.edge_id_separator}{target_node['id']}{target_input_id}",
        #     "selected": self.edge_selected,
        # }
        return(new_edge)


    def edge_source_handle_gen(self, src_node: dict, src_class: str, src_output_name: str) -> str:
        #format:  {"dataType":"EpidemiologyTB","id":"EpidemiologyTB-DN4sJ","name":"epi_forecast_model","output_types":["Data"]}
        return f"{{\"dataType\":\"{src_class}\",\"id\":\"{src_node._id}\",\"name\":\"{src_output_name}\",\"output_types\":[\"Data\"]}}"
    
    def edge_target_handle_gen(self, dst_node: dict, dst_class: str, dst_input_name: str) -> str:
        #format:  {"fieldName":"forecasts_in","id":"SegmentTB-DMvmX","inputTypes":["Data"],"type":"other"}
        return f"{{\"fieldName\":\"{dst_input_name}\",\"id\":\"{dst_node._id}\",\"inputTypes\":[\"Data\"],\"type\":\"other\"}}"
    
    def edge_id_gen(self, src_node: dict, src_handle: str, dst_node: dict, dst_handle: str) -> str:
        #format:  xy-edge__EpidemiologyTB-DN4sJ{"dataType":"EpidemiologyTB","id":"EpidemiologyTB-DN4sJ","name":"epi_forecast_model","output_types":["Data"]}-SegmentTB-DMvmX{"fieldName":"forecasts_in","id":"SegmentTB-DMvmX","inputTypes":["Data"],"type":"other"}
        return f"{self.edge_id_prefix}{src_node._id}{src_handle}-{dst_node._id}{dst_handle}"






# NOTES:
# FROM:  src/backend/base/langflow/services/database/models/flow/model.py
#
# class Flow(FlowBase, table=True):  # type: ignore[call-arg]
#     id: UUID = Field(default_factory=uuid4, primary_key=True, unique=True)
#     data: dict | None = Field(default=None, sa_column=Column(JSON))
#     user_id: UUID | None = Field(index=True, foreign_key="user.id", nullable=True)
#     user: "User" = Relationship(back_populates="flows")
#     icon: str | None = Field(default=None, nullable=True)
#     tags: list[str] | None = Field(sa_column=Column(JSON), default=[])
#     locked: bool | None = Field(default=False, nullable=True)
#     folder_id: UUID | None = Field(default=None, foreign_key="folder.id", nullable=True, index=True)
#     fs_path: str | None = Field(default=None, nullable=True)
#     folder: Optional["Folder"] = Relationship(back_populates="flows")

#     def to_data(self):
#         serialized = self.model_dump()
#         data = {
#             "id": serialized.pop("id"),
#             "data": serialized.pop("data"),
#             "name": serialized.pop("name"),
#             "description": serialized.pop("description"),
#             "updated_at": serialized.pop("updated_at"),
#         }
#         return Data(data=data)

#     __table_args__ = (
#         UniqueConstraint("user_id", "name", name="unique_flow_name"),
#         UniqueConstraint("user_id", "endpoint_name", name="unique_flow_endpoint_name"),
#     )


        # "id": "SegmentTB-2FP1D",
        # "measured": {
        #   "height": 22,
        #   "width": 22
        # },
        # "position": {
        #   "x": 12,
        #   "y": 12
        # },



# class NodeData(TypedDict):
#     id: str
#     data: dict
#     dragging: NotRequired[bool]
#     height: NotRequired[int]
#     width: NotRequired[int]
#     position: NotRequired[Position]
#     positionAbsolute: NotRequired[Position]
#     selected: NotRequired[bool]
#     parent_node_id: NotRequired[str]
#     type: NotRequired[NodeTypeEnum]



    #   {
    #     "width": 384,
    #     "height": 597,
    #     "id": "dndnode_82",
    #     "type": "genericNode",
    #     "position": {
    #       "x": 520,
    #       "y": 732
    #     },
    #     "data": {
    #       "type": "OpenAI",
    #       "node": {
    #         "template": {
    #           "cache": {
    #             "required": false,
