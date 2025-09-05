import { Button } from "@/components/ui/button";
import { COLOR_OPTIONS } from "@/constants/constants";
import { NodeDataType } from "@/types/flow";
import { cn } from "@/utils/utils";

import { memo } from "react";

export const ToolbarColorPickerButtons = memo(
  ({
    bgColor,
    data,
    setNode,
  }: {
    bgColor: string;
    data: NodeDataType;
    setNode: (id: string, updater: any) => void;
  }) => (
    <div className="flew-row flex gap-3">
      {Object.entries(COLOR_OPTIONS).map(([color, code]) => (
        <Button
          data-testid={`toolbar_color_picker_button_${color}`}
          unstyled
          key={color}
          // onClick={() => {
          //   setNode(data.id, (old) => ({
          //     ...old,
          //     data: {
          //       ...old.data,
          //       node: {
          //         ...old.data.node,
          //         template: {
          //           ...old.data.node?.template,
          //           backgroundColor: color,
          //         },
          //       },
          //     },
          //   }));
          // }

          onClick={() => {
            if (!("backgroundColor" in data?.node?.template) || (typeof(data.node.template["backgroundColor"]) === "string"))
            {
              data.node.template["backgroundColor"] = color
            }
          }



          }
        >
          <div
            className={cn(
              "h-4 w-4 rounded-full hover:border hover:border-ring",
              bgColor === color ? "border-2 border-blue-500" : "",
              code === null && "border",
            )}
            style={{
              backgroundColor: code ?? "#FFFFFF",
            }}
          />
        </Button>
      ))}
    </div>
  ),
);

ToolbarColorPickerButtons.displayName = "ToolbarColorPickerButtons";
