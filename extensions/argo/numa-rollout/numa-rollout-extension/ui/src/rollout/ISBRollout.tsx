import React, { useContext, useMemo } from "react";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { Box } from "@mui/material";
import { SquareCheckIcon } from "../utils/SquareCheckIcon";
import { SquareCancelIcon } from "../utils/SquareCancelIcon";
import { ISB, ISB_KUBERNETES, POD } from "../utils/Constants";

export const ISBRollout = () => {
  const { kindToNodeMap } = useContext(RolloutComponentContext);

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
