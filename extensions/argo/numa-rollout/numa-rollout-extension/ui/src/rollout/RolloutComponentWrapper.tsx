import React, {
  createContext,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { Box, Paper } from "@mui/material";
import { ArgoPropType, History, Node } from "../ArgoPropType";
import { ISBRollout } from "./ISBRollout";
import { ControllerRollout } from "./ControllerRollout";
import { PipelineRollout } from "./PipelineRollout";
import { ArgoRolloutComponent } from "./default/ArgoRolloutComponent";

export interface RolloutContextData {
  props: ArgoPropType;
}
export const RolloutComponentContext = createContext<RolloutContextData>({
  props: {} as any,
});
export const RolloutComponentWrapper = (props: ArgoPropType) => {
  const currentNodeKind = props?.resource?.kind;
  const currentStatusResource = props?.application?.status?.resources?.filter(
    (resource) => {
      return resource.kind === currentNodeKind;
    }
  );

  const getRevisionURL = useCallback((revision: History) => {
    return `${props?.application?.spec?.source?.repoURL}/commit/${revision?.revision}`.replace(
      ".git",
      ""
    );
  }, []);
  return (
    <RolloutComponentContext.Provider value={{ props }}>
      <Box>
        <ArgoRolloutComponent
          application={props.application}
          tree={props.tree}
          resource={props.resource as any}
        />
      </Box>
      <Box>
        <Paper sx={{ marginTop: "1rem", padding: "1rem" }}>
          <h5>Revision Data</h5>
          <Box>
            {props.application.status.history.map((revision, index) => {
              return (
                <Box key={revision.id}>
                  Revision :{" "}
                  <a
                    href={getRevisionURL(revision)}
                    target="_blank"
                  >{`Revision ${index + 1}`}</a>
                  <Box>Deployed At : {`${revision.deployedAt}`}</Box>
                  <Box>
                    {index === 0 && currentNodeKind === "ISBServiceRollout" && (
                      <Box sx={{ marginTop: "2rem" }}>
                        <ISBRollout />
                      </Box>
                    )}
                    {index === 0 &&
                      currentNodeKind === "NumaflowControllerRollout" && (
                        <Box sx={{ marginTop: "2rem" }}>
                          <ControllerRollout />
                        </Box>
                      )}
                    {index === 0 && currentNodeKind === "PipelineRollout" && (
                      <Box sx={{ marginTop: "2rem" }}>
                        <PipelineRollout />
                      </Box>
                    )}
                  </Box>
                </Box>
              );
            })}
          </Box>
        </Paper>
      </Box>
    </RolloutComponentContext.Provider>
  );
};
