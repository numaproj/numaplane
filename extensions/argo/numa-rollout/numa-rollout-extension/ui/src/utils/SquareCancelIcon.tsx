import { Tooltip } from "@mui/material";
import React from "react";
import { CheckIconProps } from "./SquareCheckIcon";

export const SquareCancelIcon = ({ tooltipTitle }: CheckIconProps) => {
  return (
    <Tooltip title={tooltipTitle}>
      <div
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
      >
        <i className="fa fa-times" />
      </div>
    </Tooltip>
  );
};
