import { Edge, Node, ReactFlowJsonObject } from "@xyflow/react";
import { BuildStatus } from "../../constants/enums";
import { APIClassType, OutputFieldType } from "../api/index";

export type PaginatedFlowsType = {
  items: FlowType[];
  total: number;
  size: number;
  page: number;
  pages: number;
};

export type FlowType = {
  name: string;
  id: string;
  data: ReactFlowJsonObject<AllNodeType, EdgeType> | null;
  description: string;
  endpoint_name?: string | null;
  style?: FlowStyleType;
  is_component?: boolean;
  last_tested_version?: string;
  updated_at?: string;
  date_created?: string;
  parent?: string;
  folder?: string;
  user_id?: string;
  icon?: string;
  gradient?: string;
  tags?: string[];
  icon_bg_color?: string;
  folder_id?: string;
  webhook?: boolean;
  locked?: boolean | null;
  public?: boolean;
  access_type?: "PUBLIC" | "PRIVATE" | "PROTECTED";
  mcp_enabled?: boolean;
};

// CUSTOM:
//export type GenericNodeType = Node<NodeDataType, "genericNode">;
export type GenericNodeType = Node<GenericNodeDataType, "genericNode">;

export type NoteNodeType = Node<NoteDataType, "noteNode">;

export type AllNodeType = GenericNodeType | NoteNodeType;
export type SetNodeType<T = "genericNode" | "noteNode"> =
  T extends "genericNode" ? GenericNodeType : NoteNodeType;

export type noteClassType = Pick<
  APIClassType,
  "description" | "display_name" | "documentation" | "tool_mode" | "frozen"
> & {
  template: {
    backgroundColor?: string;
    [key: string]: any;
  };
  outputs?: OutputFieldType[];
};

// CUSTOM: START
// add the attribute backgroundColor as a string to the GenericNode definition
export type genericNodeClassType = APIClassType & {template: {backgroundColor?: string;};}
export type GenericNodeDataType = {
  showNode?: boolean;
  type: string;
  node: genericNodeClassType; // CHANGED HERE MADE FROM 'nodeDataType' (defined below)
  id: string;
  output_types?: string[];
  selected_output_type?: string;
  buildStatus?: BuildStatus;
}
// CUSTOM:  END



export type NoteDataType = {
  showNode?: boolean;
  type: string;
  node: noteClassType;
  id: string;
};

// CUSTOM: 
// NOTE:  no changes made, just a note that we really don't need 
// NodeDataType anymore, since only GenericNode was using it, but leaving it
// hear just in case future code updated by Langflow (or us) create NodeTypes
// beyond Note and Generic and assume it's there and available.
// However, this requires that any changes made to this NodeDataType
// for the benefit of GenericNode will need to be reflected back in the same updates
// manually entered into GenericNodeDataType
export type NodeDataType = {
  showNode?: boolean;
  type: string;
  node: APIClassType;
  id: string;
  output_types?: string[];
  selected_output_type?: string;
  buildStatus?: BuildStatus;
};

export type EdgeType = Edge<EdgeDataType, "default">;

export type EdgeDataType = {
  sourceHandle: sourceHandleType;
  targetHandle: targetHandleType;
};

// FlowStyleType is the type of the style object that is used to style the
// Flow card with an emoji and a color.
export type FlowStyleType = {
  emoji: string;
  color: string;
  flow_id: string;
};

export type TweaksType = Array<
  {
    [key: string]: {
      output_key?: string;
    };
  } & FlowStyleType
>;

// right side
export type sourceHandleType = {
  baseClasses?: string[];
  dataType: string;
  id: string;
  output_types: string[];
  conditionalPath?: string | null;
  name: string;
};
//left side
export type targetHandleType = {
  inputTypes?: string[];
  output_types?: string[];
  type: string;
  fieldName: string;
  name?: string;
  id: string;
  proxy?: { field: string; id: string };
};
