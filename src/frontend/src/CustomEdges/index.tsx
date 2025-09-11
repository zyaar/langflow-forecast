import useFlowStore from "@/stores/flowStore";
import { scapeJSONParse } from "@/utils/reactflowUtils";
import { BaseEdge, EdgeProps, getBezierPath, EdgeLabelRenderer, getSmoothStepPath, Position, MarkerType } from "@xyflow/react";

// CUSTOM:  BEGIN
import { COLOR_OPTIONS } from "@/constants/constants";
import {FORECAST_COLOR_MAP, forecast_default_color} from "@/forecast_common/forecast_constants"
import OutputComponent from "@/CustomNodes/GenericNode/components/OutputComponent";
import { stringConvert } from "vanilla-jsoneditor";
import { genericNodeClassType } from "@/types/flow";
// CUSTOM: END

export function DefaultEdge({
  sourceHandleId,
  source,
  sourceX,
  sourceY,
  target,
  targetHandleId,
  targetX,
  targetY,
  ...props
}: EdgeProps) {
  const getNode = useFlowStore((state) => state.getNode);

  const sourceNode = getNode(source);
  const targetNode = getNode(target);

  const targetHandleObject = scapeJSONParse(targetHandleId!);

  const sourceXNew =
    (sourceNode?.position.x ?? 0) + (sourceNode?.measured?.width ?? 0) + 7;
  const targetXNew = (targetNode?.position.x ?? 0) - 7;

  const distance = 200 + 0.1 * ((sourceXNew - targetXNew) / 2);

  const zeroOnNegative =
    (1 +
      (1 - Math.exp(-0.01 * Math.abs(sourceXNew - targetXNew))) *
        (sourceXNew - targetXNew >= 0 ? 1 : -1)) /
    2;

  const distanceY =
    200 -
    200 * (1 - zeroOnNegative) +
    0.3 * Math.abs(targetY - sourceY) * zeroOnNegative;

  const sourceDistanceY =
    200 -
    200 * (1 - zeroOnNegative) +
    0.3 * Math.abs(sourceY - targetY) * zeroOnNegative;

  const targetYNew = targetY + 1;
  const sourceYNew = sourceY + 1;

  const edgePathLoop = `M ${sourceXNew} ${sourceYNew} C ${sourceXNew + distance} ${sourceYNew + sourceDistanceY}, ${targetXNew - distance} ${targetYNew + distanceY}, ${targetXNew} ${targetYNew}`;

  // CUSTOM:  BEGIN


  // HANDLE EDGE-LABLES
  // determine if the source object allows for edge labels, if yes, go down the edge_label display route, if no, use the default edges
  let src_display_name: string = "undefined";
  const edgeLabelsEnabled: boolean = sourceNode?.data.node.template.edge_labels?.value ?? false

  if(edgeLabelsEnabled)
  {
    const sourceHandleObject = scapeJSONParse(sourceHandleId);

    // grab the name of the output
    const label_name = sourceHandleObject?.name ?? "undefined";

    if(label_name != "undefined")
    {
      let output_len: number = sourceNode?.data.node.outputs?.length ?? 0;

      if(output_len > 0)
      {
        // get the display_name
        for(let i = 0; i < output_len; i++)
        {
          let next_output: genericNodeClassType = sourceNode!.data.node.outputs[i] ?? null
          let output_name_to_test: string = next_output.name ?? "undefined";

          if(output_name_to_test === label_name)
          {
            src_display_name = next_output.display_name ?? output_name_to_test;
            break;
          }
        }
      }
    }
  }


  // HANDLE BACKGROUND COLORS
  // grab the background color of the sourceNode
  const nodeBackgroundColor  =  Object.keys(COLOR_OPTIONS).find((key) => key === sourceNode?.data.node.template.backgroundColor,) ?? FORECAST_COLOR_MAP[sourceNode?.data.type] ?? forecast_default_color


  // Switched the line rendering style to Smooth Step instead of Bezier curves to be more aligned with the Epi flow style
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX: sourceXNew,
    sourceY: sourceYNew,
    sourcePosition: Position.Right,
    targetPosition: Position.Left,
    targetX: targetXNew,
    targetY: targetYNew,
  });

  // const [edgePath] = getBezierPath({
  //   sourceX: sourceXNew,
  //   sourceY: sourceYNew,
  //   sourcePosition: Position.Right,
  //   targetPosition: Position.Left,
  //   targetX: targetXNew,
  //   targetY: targetYNew,
  // });

  // CUSTOM:  END

  const {
    animated,
    selectable,
    deletable,
    sourcePosition,
    targetPosition,
    pathOptions,
    selected,
    ...domSafeProps
  } = props;

  // CUSTOM:  START
  if(edgeLabelsEnabled)
  {
    return (
      <>
        <BaseEdge
          path={targetHandleObject.output_types ? edgePathLoop : edgePath}
          markerEnd = {MarkerType.Arrow}
          strokeDasharray={targetHandleObject.output_types ? "5 5" : "0"}
          {...domSafeProps}
          data-animated={animated ? "true" : "false"}
          data-selectable={selectable ? "true" : "false"}
          data-deletable={deletable ? "true" : "false"}
          data-selected={selected ? "true" : "false"} />
        <EdgeLabelRenderer>
          <div
            data-testid="forecast-label-node"
            style={{
              transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
              backgroundColor: `${COLOR_OPTIONS[nodeBackgroundColor]}`,
            }}
            className="react-flow__node nopan selectable draggable  ring-[0.75px] ring-muted-foreground border-muted-foreground hover:shadow-node w-90 generic-node-div group/node relative rounded-xl border shadow-sm hover:shadow-md grid text-wrap p-4 leading-5 relative rounded-xl gap-3"
          >
              <div class="z-50 transition-all duration-300 ease-out">
                <div class="grid text-wrap leading-5 relative rounded-xl gap-3"> {/* removed p-4 */}
                  <div class="generic-node-title-arrangement">
                    <div class="generic-node-tooltip-div truncate">
                      <div class="group flex w-full items-center gap-1">
                        <div class="nodoubleclick w-full truncate font-medium text-primary cursor-default">
                          <div class="flex cursor-grab items-center gap-2">
                            <span class="cursor-grab text-sm max-w-70">{src_display_name}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
        </EdgeLabelRenderer>
      </>
    );
  }
  else
  {
    return (
      <BaseEdge
        path={targetHandleObject.output_types ? edgePathLoop : edgePath}
        markerEnd = {MarkerType.Arrow}
        strokeDasharray={targetHandleObject.output_types ? "5 5" : "0"}
        {...domSafeProps}
        data-animated={animated ? "true" : "false"}
        data-selectable={selectable ? "true" : "false"}
        data-deletable={deletable ? "true" : "false"}
        data-selected={selected ? "true" : "false"}
      />
    );
  }





  // return (
  //   <BaseEdge
  //     path={targetHandleObject.output_types ? edgePathLoop : edgePath}
  //     markerEnd = {MarkerType.Arrow}
  //     strokeDasharray={targetHandleObject.output_types ? "5 5" : "0"}
  //     {...domSafeProps}
  //     data-animated={animated ? "true" : "false"}
  //     data-selectable={selectable ? "true" : "false"}
  //     data-deletable={deletable ? "true" : "false"}
  //     data-selected={selected ? "true" : "false"}
  //   />
  // );

  // CUSTOM: END
}
