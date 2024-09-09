import React, { useContext, useMemo } from "react";
import { Box } from "@mui/material";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { SquareCheckIcon } from "../utils/SquareCheckIcon";
import { SquareCancelIcon } from "../utils/SquareCancelIcon";

export const ControllerRollout = () => {
  const { kindToNodeMap } = useContext(RolloutComponentContext);

  const controllerPods = useMemo(() => {
    const pods = kindToNodeMap?.get("Pod") ?? [];
    return pods.filter(
      (node) => node?.name?.indexOf("numaflow-controller") >= 0
    );
  }, [kindToNodeMap?.get("Pod")]);

  const controllerName = useMemo(() => {
    const numaflowController = kindToNodeMap?.get(
      "NumaflowControllerRollout"
    )?.[0];
    if (numaflowController) {
      const numaControllerName = numaflowController.name;

      // Find the deployment object with the numaControllerName
      const deployment = kindToNodeMap.get("Deployment") ?? [];
      const controllerDeployment = deployment.find(
        (node) => node?.parentRefs?.[0]?.name === numaControllerName
      );

      return controllerDeployment?.name ?? "";
    }
    return "";
  }, [
    kindToNodeMap.get("NumaflowControllerRollout"),
    kindToNodeMap.get("Deployment"),
  ]);

  return (
    <Box>
      <Box>Controller Name : {controllerName}</Box>
      <Box>
        Controller Pod Status:{" "}
        <Box
          sx={{
            display: "flex",
            flexDirection: "row",
            width: "100%",
            backgroundColor: "#dee6eb",
            borderRadius: "3px",
            padding: "6px",
            flexWrap: "wrap",
            marginRight: "-3px",
            marginBottom: "-3px",
          }}
        >
          {controllerPods.map((node) => {
            return (
              <Box key={node.name}>
                <Box>
                  {node?.health?.status === "Healthy" ? (
                    <SquareCheckIcon tooltipTitle={node.name} />
                  ) : (
                    <SquareCancelIcon tooltipTitle={node.name} />
                  )}
                </Box>
              </Box>
            );
          })}
        </Box>
      </Box>
    </Box>
  );
};
