import React, { useContext, useEffect, useState } from "react";
import { Node } from "../ArgoPropType";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { Box } from "@mui/material";
import { SquareCheckIcon } from "../utils/SquareCheckIcon";
import { SquareCancelIcon } from "../utils/SquareCancelIcon";

export const PipelineRollout = () => {
  const { props } = useContext(RolloutComponentContext);
  const [kindToNodeMap, setKindToNodeMap] = useState<Map<string, Node[]>>(
    new Map()
  );
  useEffect(() => {
    const tempMap = new Map<string, Node[]>();

    const tree = props?.tree;
    const nodes = tree?.nodes ?? [];
    for (const node of nodes) {
      const kind = node?.kind;
      if (kind) {
        const tempNodes = tempMap.get(kind) ?? [];
        tempNodes.push(node);
        tempMap.set(kind, tempNodes);
      }
    }
    setKindToNodeMap(tempMap);
  }, [props?.tree]);

  return (
    <Box>
      <Box>
        {kindToNodeMap.get("Pipeline")?.map((node) => {
          return (
            <Box key={node?.name}>
              <Box>Pipeline Name: {node?.name}</Box>
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
                Pipeline Status:{" "}
                {props?.resource?.status?.conditions?.[0].type ===
                "ChildResourcesHealthy" ? (
                  <SquareCheckIcon tooltipTitle={node?.name} />
                ) : (
                  <SquareCancelIcon tooltipTitle={node?.name} />
                )}
              </Box>
            </Box>
          );
        })}
      </Box>
    </Box>
  );
};
