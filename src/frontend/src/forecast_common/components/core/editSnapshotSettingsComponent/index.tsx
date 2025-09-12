import React, { ChangeEvent, useState } from "react";
import { SnapshotProps, InputProps } from "@/types/components";
import { cn, isEndpointNameValid } from "@/utils/utils";

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export const EditExportDiagramSettings: React.FC<SnapshotProps> = ({
  name,
  width = 2048,
  height = 2048,
  scale = 1,
  backgroundColor = "#FFFFFF",
  setName,
  setWidth,
  setHeight,
  setScale,
  setBackgroundColor,
}: SnapshotProps): JSX.Element => {

  const handleNameChange = (event: ChangeEvent<HTMLInputElement>) => {
    setName!(event.target.value);
  };

  const handleWidthChange = (event: ChangeEvent<HTMLInputElement>) => {
    setWidth!(Number(event.target.value));
  }

  const handleHeightChange = (event: ChangeEvent<HTMLInputElement>) => {
    setHeight!(Number(event.target.value));
  }

  const handleScaleChange = (event: ChangeEvent<HTMLInputElement>) => {
    setScale!(Number(event.target.value));
  }

  const handleBackgroundColorChange = (event: ChangeEvent<HTMLInputElement>) => {
    setBackgroundColor!(event.target.value);
  }

  //this function is necessary to select the text when double clicking, this was not working with the onFocus event
  const handleFocus = (event) => event.target.select();

  return (
    <>
      <Label>
        <div className="edit-flow-arrangement mt-3">
          <span className="font-medium">Name{setName ? "" : ":"}</span>{" "}
        </div>
        {setName ? (
          <Input
            className="nopan nodelete nodrag noflow mt-2 font-normal"
            onChange={handleNameChange}
            type="text"
            name="name"
            value={name ?? ""}
            placeholder="filename.png"
            id="name"
            //maxLength={maxLength}
            onDoubleClickCapture={(event) => {
              handleFocus(event);
            }}
            data-testid="input-snapshot-name"
          />
        ) : (
          <span className="font-normal text-muted-foreground word-break-break-word">
            {name}
          </span>
        )}
      </Label>
      {/* <Label>
        <div className="edit-flow-arrangement mt-3">
          <span className="font-medium">Width (in pixels){setWidth ? "" : ":"}</span>{" "}
        </div>
        {setWidth ? (
          <Input
            className="nopan nodelete nodrag noflow mt-2 font-normal"
            onChange={handleWidthChange}
            type="number"
            name="width"
            value={width ?? ""}
            placeholder="2048"
            id="width"
            onDoubleClickCapture={(event) => {
              handleFocus(event);
            }}
            data-testid="input-snapshot-width"
          />
        ) : (
          <span className="font-normal text-muted-foreground word-break-break-word">
            {width}
          </span>
        )}
      </Label> */}
      {/* <Label>
        <div className="edit-flow-arrangement mt-3">
          <span className="font-medium">Height (in pixels){setHeight ? "" : ":"}</span>{" "}
        </div>
        {setHeight ? (
          <Input
            className="nopan nodelete nodrag noflow mt-2 font-normal"
            onChange={handleHeightChange}
            type="number"
            name="height"
            value={height ?? ""}
            placeholder="2048"
            id="height"
            onDoubleClickCapture={(event) => {
              handleFocus(event);
            }}
            data-testid="input-snapshot-height"
          />
        ) : (
          <span className="font-normal text-muted-foreground word-break-break-word">
            {height}
          </span>
        )}
      </Label> */}
      <Label>
        <div className="edit-flow-arrangement mt-3">
          <span className="font-medium">Scale (if images are too fuzzy, increase this number){setScale ? "" : ":"}</span>{" "}
        </div>
        {setScale ? (
          <Input
            className="nopan nodelete nodrag noflow mt-2 font-normal"
            onChange={handleScaleChange}
            type="number"
            name="scale"
            value={scale ?? ""}
            placeholder="1"
            id="scale"
            onDoubleClickCapture={(event) => {
              handleFocus(event);
            }}
            data-testid="input-snapshot-scale"
          />
        ) : (
          <span className="font-normal text-muted-foreground word-break-break-word">
            {scale}
          </span>
        )}
      </Label>
    </>
  );
};

export default EditExportDiagramSettings;
