import React, { useContext, useEffect, useMemo, useState } from "react";
import { Node } from "../ArgoPropType";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { Box } from "@mui/material";
import { SquareCheckIcon } from "../utils/SquareCheckIcon";
import { SquareCancelIcon } from "../utils/SquareCancelIcon";
import { ISB, ISB_KUBERNETES, POD } from "../utils/Constants";

export const ISBRollout = () => {
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

  const isbPods = useMemo(() => {
    if (!kindToNodeMap || !kindToNodeMap.get(POD)) {
      return [];
    }
    const pods = kindToNodeMap.get(POD);
    if (!pods) {
      return [];
    }
    return pods.filter((node) => node.name.indexOf(ISB_KUBERNETES) >= 0);
  }, [kindToNodeMap]);

  const isbName = useMemo(() => {
    if (!kindToNodeMap || !kindToNodeMap.get(POD)) {
      return "";
    }
    const isbService = kindToNodeMap.get(ISB);
    if (!isbService || !isbService[0]) {
      return "";
    }
    return isbService[0].name;
  }, [kindToNodeMap]);

  return (
    <Box>
      <Box>ISB Name : {isbName} </Box>
      <Box>
        ISB Pod Status:{" "}
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
          {isbPods.map((node) => {
            return (
              <Box key={node?.name}>
                <Box>
                  {node?.health?.status === "Healthy" ? (
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
    </Box>
  );
};
