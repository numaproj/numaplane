import React, { useMemo } from "react";
import { ArgoPropType, Node } from "../ArgoPropType";

export const RolloutComponentWrapper = (props: ArgoPropType) => {
  const currentNodeKind = props.resource.kind;
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
    <div>
      <div>This is Numarollout Component for {currentNodeKind}</div>
      {nodeKindToNodeArrayMap.get(currentNodeKind)?.map((node) => {
        return (
          <div key={node.uid}>
            <div>Kind : {node.kind}</div>
            <div>Name : {node.name}</div>
            <div>Namespace : {node.namespace}</div>
            <div>Resource Version : {node.resourceVersion}</div>
            <div>Created at : {new Date(node.createdAt).toTimeString()}</div>
          </div>
        );
      })}
    </div>
  );
};
