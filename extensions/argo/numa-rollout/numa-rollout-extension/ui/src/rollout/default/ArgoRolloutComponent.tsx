import React from "react";

import "./ArgoRolloutComponent.css";
import { ArgoRolloutSummary } from "./ArgoRolloutSummary";
import { CANARY } from "../../utils/Constants";
import { ArgoRolloutContainers } from "./ArgoRolloutContainers";
import { Box } from "@mui/material";

const rolloutParams = {
  strategy: CANARY,
  setWeight: "0",
  actualWeight: "100",
  step: "1",
};
export const ArgoRolloutComponent = () => {
  return (
    <Box sx={{ display: "flex" }}>
      <ArgoRolloutSummary rolloutParams={rolloutParams} />
      <ArgoRolloutContainers />
    </Box>
  );
};
