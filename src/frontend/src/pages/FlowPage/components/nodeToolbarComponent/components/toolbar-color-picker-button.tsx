import { Button } from "@/components/ui/button";
import { COLOR_OPTIONS } from "@/constants/constants";

// CUSTOM:
//import { NodeDataType } from "@/types/flow";
import { GenericNodeDataType } from "@/types/flow";

import { cn } from "@/utils/utils";

import { memo, VoidFunctionComponent } from "react";

export const ToolbarColorPickerButtons = memo(
  ({
    bgColor,
    data,
    setColorPickerOpen,
    handleSelectChange,
  }: {
    bgColor: string;
    data: GenericNodeDataType;
    setColorPickerOpen: (state: boolean) => void;
    handleSelectChange: (event: string) => void;
  }) => (
    <div className="flew-row flex gap-3">
      {Object.entries(COLOR_OPTIONS).map(([color, code]) => (
        <Button
          data-testid={`toolbar_color_picker_button_${color}`}
          unstyled
          key={color}

          onClick={() => { 
            data.node.template.backgroundColor = color;
            setColorPickerOpen(false);
            handleSelectChange("colorChange");
           }}
        >
          <div
            className={cn(
              "h-4 w-4 rounded-full hover:border hover:border-ring",
              bgColor === color ? "border-2 border-blue-500" : "",
              code === null && "border",
            )}
            style={{
              backgroundColor: code ?? "#00000000",
            }}
          />
        </Button>
      ))}
    </div>
  ),
);

ToolbarColorPickerButtons.displayName = "ToolbarColorPickerButtons";
