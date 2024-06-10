import React, { useMemo } from "react";
import { ArgoPropType, Node } from "../ArgoPropType";
import { Box, Paper } from "@mui/material";

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
  return (
    <Box>
      <h4>This is Numarollout Component for {currentNodeKind}</h4>
      <Paper sx={{ padding: "1rem" }}>
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

      <Paper sx={{ marginTop: "2rem", padding: "1rem" }}>
        <h5>Status Data</h5>
        <Box> Status : {currentStatusResource[0].status}</Box>
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
          {props.application.status.history.map((revision) => {
            return (
              <Box key={revision.id}>
                <Box>Revision Hash : {revision.revision}</Box>
                <Box>Deployed At : {revision.deployedAt}</Box>
              </Box>
            );
          })}
        </Box>
      </Paper>
    </Box>
  );
};
