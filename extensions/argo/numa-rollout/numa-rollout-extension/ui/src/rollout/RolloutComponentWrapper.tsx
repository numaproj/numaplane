import React, { createContext, useCallback, useEffect, useState } from "react";
import { Box } from "@mui/material";
import { ArgoPropType, History, Node } from "../ArgoPropType";
import { ISBRollout } from "./ISBRollout";
import { ControllerRollout } from "./ControllerRollout";
import { PipelineRollout } from "./PipelineRollout";
import { ArgoRolloutComponent } from "./default/ArgoRolloutComponent";
import {
  INTUIT_DOMAIN,
  ISB_SERVICE_ROLLOUT,
  MONOVERTEX_ROLLOUT,
  NUMAFLOW_CONTROLLER_ROLLOUT,
  PIPELINE_ROLLOUT,
} from "../utils/Constants";
import { MonovertexRollout } from "./MonovertexRollout";

export interface RolloutContextData {
  props: ArgoPropType;
  kindToNodeMap: Map<string, Node[]>;
}
export const RolloutComponentContext = createContext<RolloutContextData>({
  props: {} as any,
  kindToNodeMap: new Map(),
});

export const RolloutComponentWrapper = (props: ArgoPropType) => {
  const [kindToNodeMap, setKindToNodeMap] = useState<Map<string, Node[]>>(
    new Map()
  );
  const currentNodeKind = props?.resource?.kind;

  // Check if the current rollout is hosted inside Intuit
  const isIntuitRollout = window.location.href.indexOf(INTUIT_DOMAIN) >= 0;
  props.application.status.history = props.application.status.history.sort(
    (a, b) => {
      const dateA = new Date(a.deployedAt);
      const dateB = new Date(b.deployedAt);
      return dateB.getTime() - dateA.getTime();
    }
  );
  const getRevisionURL = useCallback((revision: History) => {
    return `${props?.application?.spec?.source?.repoURL}/commit/${revision?.revision}`.replace(
      ".git",
      ""
    );
  }, []);
  useEffect(() => {
    const tempMap = new Map<string, Node[]>();

    const tree = props?.tree;
    const nodes = tree?.nodes;
    for (const node of nodes ?? []) {
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
    <RolloutComponentContext.Provider value={{ props, kindToNodeMap }}>
      {isIntuitRollout && (
        <Box
          sx={{
            height: "25px",
            width: "830px",
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
        <ArgoRolloutComponent />
      </Box>
      <Box
        sx={{
          width: "830px",
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
                    {index === 0 && currentNodeKind === ISB_SERVICE_ROLLOUT && (
                      <Box sx={{ marginTop: "1rem" }}>
                        <ISBRollout />
                      </Box>
                    )}
                    {index === 0 &&
                      currentNodeKind === NUMAFLOW_CONTROLLER_ROLLOUT && (
                        <Box sx={{ marginTop: "1rem" }}>
                          <ControllerRollout />
                        </Box>
                      )}
                    {index === 0 && currentNodeKind === PIPELINE_ROLLOUT && (
                      <Box sx={{ marginTop: "1rem" }}>
                        <PipelineRollout />
                      </Box>
                    )}
                    {index === 0 && currentNodeKind === MONOVERTEX_ROLLOUT && (
                      <Box sx={{ marginTop: "1rem" }}>
                        <MonovertexRollout />
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
