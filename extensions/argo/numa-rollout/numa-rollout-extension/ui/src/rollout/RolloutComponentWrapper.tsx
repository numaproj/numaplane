import React, { createContext, useCallback } from "react";
import { Box } from "@mui/material";
import { ArgoPropType, History } from "../ArgoPropType";
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
  // Check if the current rollout is hosted inside Intuit
  const isIntuitRollout = window.location.href.indexOf("intuit.com") >= 0;
  const getRevisionURL = useCallback((revision: History) => {
    return `${props?.application?.spec?.source?.repoURL}/commit/${revision?.revision}`.replace(
      ".git",
      ""
    );
  }, []);
  return (
    <RolloutComponentContext.Provider value={{ props }}>
      {isIntuitRollout && (
        <Box
          sx={{
            height: "25px",
            width: "821px",
            paddingLeft: "1rem",
            marginLeft: "1rem",
            backgroundColor: "cornsilk",
            paddingTop: "4px",
            fontSize: "12px",
            borderRadius: "5px",
          }}
        >
          Please read the documentation for{" "}
          <a href="https://numaflow.numaproj.io/" target="_blank">
            help{" "}
          </a>
          regarding this extension. For more help please reach out on{" "}
          <a
            href="https://slack.com/app_redirect?team=T2G8RTHAM&amp;channel=devx-numaproj-support"
            target="_blank"
          >
            #devx-numaproj-support
          </a>
        </Box>
      )}
      <Box sx={{ marginTop: "1rem" }}>
        <ArgoRolloutComponent
          application={props.application}
          tree={props.tree}
          resource={props.resource as any}
        />
      </Box>
      <Box
        sx={{
          width: "818px",
          marginLeft: "18px",
          padding: "10px",
          boxSizing: "border-box",
          borderRadius: "5px",
          backgroundColor: "#fff",
          marginTop: "-1em",
        }}
      >
        <Box sx={{ padding: "1rem" }}>
          <div
            style={{
              fontSize: "18px",
              fontWeight: "600",
              color: "#363c4a",
              fontFamily: "'Heebo', sans-serif",
              marginBottom: "0.5em",
            }}
          >
            Revision
          </div>
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
                      <Box sx={{ marginTop: "1rem" }}>
                        <ISBRollout />
                      </Box>
                    )}
                    {index === 0 &&
                      currentNodeKind === "NumaflowControllerRollout" && (
                        <Box sx={{ marginTop: "1rem" }}>
                          <ControllerRollout />
                        </Box>
                      )}
                    {index === 0 && currentNodeKind === "PipelineRollout" && (
                      <Box sx={{ marginTop: "1rem" }}>
                        <PipelineRollout />
                      </Box>
                    )}
                  </Box>
                </Box>
              );
            })}
          </Box>
        </Box>
      </Box>
    </RolloutComponentContext.Provider>
  );
};
