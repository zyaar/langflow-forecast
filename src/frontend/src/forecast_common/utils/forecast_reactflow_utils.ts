
import { getViewportForBounds, ReactFlowInstance, useNodes } from '@xyflow/react';
import { domToPng } from 'modern-screenshot'
import { EXPORT_DIAGRAM_TARGET_NODE } from '@/forecast_common/forecast_constants'


/**
 * Generate an image from the flow DOM and download
 * @param reactFlowInstance - Pointer to the react flow to be exported
 * @param name - filename of the react flow
 * @param imageWidth - Number of pixels of width for the PNG
 * @param imageHeight - Number of pixels of height for the PNG
 * @param scale - Number of dots per pixel (dpi = 96 dpi * scale)
 * @param backgroundColor - Optional background color
 */

export async function exportDiagram(
  reactFlowInstance: ReactFlowInstance,
  name: string,
  imageWidth: number,
  imageHeight: number,
  scale: number,
  backgroundColor?: string,
) 
{
    const element_classnames_to_hide = "react-flow__panel"

    const nodesBounds = reactFlowInstance.getNodesBounds(reactFlowInstance.getNodes());
    const old_viewport = reactFlowInstance.getViewport()
    const viewport = getViewportForBounds(nodesBounds, imageWidth, imageHeight, 0, 10, {top: "200px", bottom: "200px", right: "200px", left: "200px"});
    const targetNode = document.querySelector(EXPORT_DIAGRAM_TARGET_NODE);

    const hideNodes = (cloned: Node) => {
        if(cloned.nodeType === Node.ELEMENT_NODE)
        {
            const myElement = cloned as Element;

            if((myElement.className) && (typeof(myElement.className) == "string") && (myElement.className.includes(element_classnames_to_hide)))
            {
                myElement.style.display = "none";
            }
        }
    };

    if(targetNode != null)
    {
        reactFlowInstance.setViewport(viewport)

        await domToPng(targetNode, {
            backgroundColor: backgroundColor,
            scale: scale,
            //width: imageWidth,
            //height: imageHeight,
            onCloneEachNode: hideNodes,
        }).then((dataUrl) => {
            const link = document.createElement('a')

            if(!name.endsWith(".png"))
                name += ".png"

            link.download = name
            link.href = dataUrl
            link.click()
        })

        reactFlowInstance.setViewport(old_viewport)
    }
}
