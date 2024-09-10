import React, { useContext } from "react";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { Box } from "@mui/material";
import { SquareCheckIcon } from "../utils/SquareCheckIcon";
import { SquareCancelIcon } from "../utils/SquareCancelIcon";

export const MonovertexRollout = () => {
  const { props, kindToNodeMap } = useContext(RolloutComponentContext);

  return (
    <Box>
      <Box>
        {kindToNodeMap.get("MonoVertexRollout")?.map((node) => {
          return (
            <Box key={node?.name}>
              <Box>Monovertex Rollout Name: {node?.name}</Box>
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
                Monovertex Status:{" "}
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
