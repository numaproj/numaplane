import React, { useContext, useEffect, useState } from "react";
import { Node } from "../ArgoPropType";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { Box, Chip, Tooltip } from "@mui/material";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import CancelIcon from "@mui/icons-material/Cancel";

export const PipelineRollout = () => {
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
    console.log("PipelineRollout: ", tempMap);
  }, [props.tree]);
  return (
    <Box>
      <Box>
        {kindToNodeMap.get("Pipeline")?.map((node) => {
          return (
            <Box key={node.name}>
              <Box>Pipeline Name: {node.name}</Box>
              <Box>
                Pipeline Status:{" "}
                <Tooltip title={node.name}>
                  {props.resource.status.conditions[0].type ===
                  "ChildResourcesHealthy" ? (
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
  );
};
