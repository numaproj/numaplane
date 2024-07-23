import { Box, Tooltip } from "@mui/material";
import React from "react";
import { CheckIconProps } from "./SquareCheckIcon";

export const SquareCancelIcon = ({ tooltipTitle }: CheckIconProps) => {
  return (
    <Tooltip title={tooltipTitle}>
      <Box
        style={{
          width: "25px",
          height: "25px",
          backgroundColor: "#f44336",
          borderRadius: "3px",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          color: "white",
          cursor: "pointer",
          margin: "3px",
        }}
        aria-label={`Status: Failed for ${tooltipTitle}`}
        data-testid={`tooltip-${tooltipTitle}`}
      >
        <i className="fa fa-times" />
      </Box>
    </Tooltip>
  );
};
