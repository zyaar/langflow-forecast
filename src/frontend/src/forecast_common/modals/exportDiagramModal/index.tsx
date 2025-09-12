import { track } from "@/customization/utils/analytics";
import useFlowStore from "@/stores/flowStore";
import { ReactNode, forwardRef, useEffect, useState } from "react";
import IconComponent from "@/components/common/genericIconComponent";
import EditExportDiagramSettings from "@/forecast_common/components/core/editSnapshotSettingsComponent";
import BaseModal from "@/modals/baseModal";
import { useReactFlow, } from '@xyflow/react';
import { exportDiagram } from '@/forecast_common/utils/forecast_reactflow_utils';


const ExportDiagramModal = forwardRef(
  (props: { children: ReactNode }, ref): JSX.Element => {
    const currentFlow = useFlowStore((state) => state.currentFlow);
    const isBuilding = useFlowStore((state) => state.isBuilding);
    useEffect(() => {
      setName(currentFlow?.name ?? "");
    }, [currentFlow?.name]);

    // edit setting specific variables
    const [name, setName] = useState(currentFlow?.name ?? "");
    const [width, setWidth] = useState(2048);
    const [height, setHeight] = useState(2048);
    const [scale, setScale] = useState(1);
    const [backgroundColor, setBackgroundColor] = useState("#FFFFFF");
    const reactFlowInstance = useReactFlow();

    // BaseModal specific variables
    const [open, setOpen] = useState(false);

    return (
      <BaseModal
        size="smaller-h-full"
        open={open}
        setOpen={setOpen}
        onSubmit={() => {
          exportDiagram(
            reactFlowInstance,
            name,
            width,
            height,
            scale,
            backgroundColor,
          );
          setOpen(false);
          track("Diagram Exported", { flowId: currentFlow!.id });
        }}
      >
        <BaseModal.Trigger asChild>{props.children}</BaseModal.Trigger>
        <BaseModal.Header description="Export flow as PNG image.">
          <span className="pr-2">Export Diagram</span>
          <IconComponent
            name="ImageDown"
            className="h-6 w-6 pl-1 text-foreground"
            aria-hidden="true"
          />
        </BaseModal.Header>
        <BaseModal.Content>
          <EditExportDiagramSettings
            name={name}
            width={width}
            height={height}
            scale={scale}
            backgroundColor={backgroundColor}
            setName={setName}
            setWidth={setWidth}
            setHeight={setHeight}
            setScale={setScale}
            setBackgroundColor={setBackgroundColor}
          />
        </BaseModal.Content>
        <BaseModal.Footer submit={{ label: "Export", loading: isBuilding }} />
      </BaseModal>
    );
  },
);
export default ExportDiagramModal;
