import React, { useContext, useEffect, useMemo, useState } from "react";
import { Node } from "../ArgoPropType";
import { Box, Tooltip } from "@mui/material";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import CancelIcon from "@mui/icons-material/Cancel";

export const ControllerRollout = () => {
  const { props } = useContext(RolloutComponentContext);
  const [kindToNodeMap, setKindToNodeMap] = useState<Map<string, Node[]>>(
    new Map()
  );
  useEffect(() => {
    const tempMap = new Map<string, Node[]>();

    const tree = props.tree;
    const nodes = tree.nodes;
    for (const node of nodes) {
      const kind = node.kind;
      if (tempMap.has(kind)) {
        const tempNodes = tempMap.get(kind);
        tempNodes?.push(node);
        tempMap.set(kind, tempNodes || []);
      } else {
        tempMap.set(kind, [node]);
      }
    }
    setKindToNodeMap(tempMap);
  }, [props.tree]);
  const controllerPods = useMemo(() => {
    if (!kindToNodeMap || !kindToNodeMap.get("Pod")) {
      return [];
    }
    const pods = kindToNodeMap.get("Pod");
    if (!pods) {
      return [];
    }
    return pods.filter((node) => node.name.indexOf("numaflow-controller") >= 0);
  }, [kindToNodeMap]);

  const controllerName = useMemo(() => {
    if (!kindToNodeMap || !kindToNodeMap.get("NumaflowControllerRollout")) {
      return "";
    }
    const numaflowController = kindToNodeMap.get("NumaflowControllerRollout");
    if (!numaflowController) {
      return "";
    }
    const numaControllerName = numaflowController[0].name;

    //Find the deployment object with the numaControllerName
    const deployment = kindToNodeMap.get("Deployment");
    if (!deployment) {
      return "";
    }
    const controllerDeployment = deployment.find(
      (node) =>
        node.parentRefs && node.parentRefs[0]?.name === numaControllerName
    );

    return controllerDeployment?.name;
  }, [kindToNodeMap]);
  return (
    <Box>
      <Box>Controller Name : {controllerName} </Box>
      <Box>
        Controller Pod Status:{" "}
        <Box sx={{ display: "flex", flexDirection: "row" }}>
          {controllerPods.map((node) => {
            return (
              <Box key={node.name}>
                <Box>
                  <Tooltip title={node.name}>
                    {node.health?.status === "Healthy" ? (
                      <CheckBoxIcon sx={{ color: "green" }} />
                    ) : (
                      <CancelIcon sx={{ color: "red" }} />
                    )}
                  </Tooltip>
                </Box>
              </Box>
            );
          })}
        </Box>
      </Box>
    </Box>
  );
};
