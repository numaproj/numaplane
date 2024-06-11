import React, { useCallback, useMemo } from "react";
import { ArgoPropType, History, Node } from "../ArgoPropType";
import { Box, Chip, Paper } from "@mui/material";
import CheckBoxIcon from "@mui/icons-material/CheckBox";
import CancelIcon from "@mui/icons-material/Cancel";

export const RolloutComponentWrapper = (props: ArgoPropType) => {
  const currentNodeKind = props.resource.kind;
  const currentStatusResource = props.application.status.resources.filter(
    (resource) => {
      return resource.kind === currentNodeKind;
    }
  );
  const nodeKindToNodeArrayMap = useMemo(() => {
    const nodeKindToNodeArrayMap = new Map<string, Node[]>();
    const tree = props.tree;
    const nodes = tree.nodes;
    for (const node of nodes) {
      const kind = node.kind;
      if (nodeKindToNodeArrayMap.has(kind)) {
        nodeKindToNodeArrayMap.get(kind)?.push(node);
      } else {
        nodeKindToNodeArrayMap.set(kind, [node]);
      }
    }
    return nodeKindToNodeArrayMap;
  }, [props.tree]);

  const getRevisionURL = useCallback((revision: History) => {
    return `${props.application.spec.source.repoURL}/commit/${revision.revision}`.replace(
      ".git",
      ""
    );
  }, []);
  return (
    <Box>
      <h4>This is Numarollout Component for {currentNodeKind}</h4>
      <Paper sx={{ marginTop: "2rem", padding: "1rem" }}>
        <h5>Pipeline Data</h5>
        {nodeKindToNodeArrayMap.get("Pipeline")?.map((node) => {
          return (
            <Box key={node.uid}>
              <Box>Pipeline Name : {node.name}</Box>
              <Box>
                Pipeline Status :{" "}
                <Chip
                  label={props.resource.status.phase}
                  color={
                    props.resource.status.phase === "Running"
                      ? "success"
                      : "error"
                  }
                />
              </Box>
            </Box>
          );
        })}
      </Paper>
      <Paper sx={{ marginTop: "2rem", padding: "1rem" }}>
        <h5>Rollout Status</h5>
        <Box sx={{ display: "flex" }}>
          {props.application.status.resources.map((resource) => {
            return (
              <Box
                sx={{
                  display: "flex",
                  flexDirection: "column",
                  marginLeft: "1rem",
                }}
                key={resource.name}
              >
                <Box sx={{ display: "flex" }}>
                  Resource Name : {resource.name}
                </Box>
                <Box sx={{ display: "flex" }}>
                  Resource Kind : {resource.kind}
                </Box>
                <Box sx={{ display: "flex" }}>
                  Resource Status :{" "}
                  {resource.status === "Synced" ? (
                    <CheckBoxIcon sx={{ color: "green" }} />
                  ) : (
                    <CancelIcon sx={{ color: "red" }} />
                  )}
                </Box>
              </Box>
            );
          })}
        </Box>
      </Paper>
      <Paper sx={{ marginTop: "2rem", padding: "1rem" }}>
        <h5>Status Data</h5>
        <Box>
          {" "}
          Status :{" "}
          <Chip
            label={currentStatusResource[0].status}
            color={
              currentStatusResource[0].status === "Synced" ? "success" : "error"
            }
          />
        </Box>
        <Box>
          {" "}
          Repo URL :{" "}
          <a href={props.application.spec.source.repoURL} target="_blank">
            {props.application.spec.source.repoURL}
          </a>
        </Box>
      </Paper>

      <Paper sx={{ marginTop: "2rem", padding: "1rem" }}>
        <h5>Revision Data</h5>
        <Box>
          {props.application.status.history.map((revision, index) => {
            return (
              <Box key={revision.id}>
                <Box>
                  Revision :{" "}
                  <a
                    href={getRevisionURL(revision)}
                    target="_blank"
                  >{`Revision ${index + 1}`}</a>
                </Box>
                <Box>Deployed At : {`${revision.deployedAt}`}</Box>
              </Box>
            );
          })}
        </Box>
      </Paper>
      <Paper sx={{ marginTop: "2rem", padding: "1rem" }}>
        <h5>Meta Data</h5>
        {nodeKindToNodeArrayMap.get(currentNodeKind)?.map((node) => {
          return (
            <Box key={node.uid}>
              <Box>Kind : {node.kind}</Box>
              <Box>Name : {node.name}</Box>
              <Box>Namespace : {node.namespace}</Box>
              <Box>Resource Version : {node.resourceVersion}</Box>
              <Box>Created at : {new Date(node.createdAt).toTimeString()}</Box>
            </Box>
          );
        })}
      </Paper>
    </Box>
  );
};
