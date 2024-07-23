import { Box, Tooltip } from "@mui/material";
import React from "react";

export interface CheckIconProps {
  tooltipTitle?: string;
}
export const SquareCheckIcon = ({ tooltipTitle }: CheckIconProps) => {
  return (
    <Tooltip title={tooltipTitle}>
      <Box
        sx={{
          width: "25px",
          height: "25px",
          backgroundColor: "#18be94",
          borderRadius: "3px",
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          color: "white",
          cursor: "pointer",
          margin: "3px",
        }}
        aria-label={`Status: Success for ${tooltipTitle}`}
        data-testid={`tooltip-${tooltipTitle}`}
      >
        <i className="fa fa-check" />
      </Box>
    </Tooltip>
  );
};
