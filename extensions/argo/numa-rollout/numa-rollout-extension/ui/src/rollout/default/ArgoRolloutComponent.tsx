import React from "react";
import { RolloutWidget } from "argo-rollouts/ui/src/app/components/rollout/rollout";
import {
  RolloutAnalysisRunInfo,
  RolloutReplicaSetInfo,
  RolloutRolloutInfo,
} from "argo-rollouts/ui/src/models/rollout/generated";
import { ObjectMeta, TypeMeta } from "argo-ui/src/models/kubernetes";
import { default as axios } from "axios";
import {
  ANALYSIS_RUN,
  CANARY,
  DEGRADED,
  ERROR,
  FAILURE,
  HEALTHY,
  PROGRESSING,
  REPLICA_SET,
  REVISION,
  ROLLOUT,
  RUNNING,
  STATUS_REASON,
  SUCCESSFUL,
} from "../../utils/Constants";

export type State = TypeMeta & { metadata: ObjectMeta } & {
  status: any;
  spec: any;
};

const parseInfoFromResourceNode = (app: any, tree: any, resource: State) => {
  const ro: RolloutRolloutInfo = {};
  const { spec, status, metadata } = app;
  ro.objectMeta = metadata as any;

  ro.analysisRuns = parseAnalysisRuns(app, tree, resource);
  ro.replicaSets = parseReplicaSets(tree, resource);

  // Additional checks with ? operator have been added whenever accessing nested properties
  if (spec?.strategy?.canary) {
    ro.strategy = "BlueGreen";
    const steps = spec?.strategy?.canary?.steps || [];
    ro.steps = steps;

    if (steps && status?.currentStepIndex !== null && steps.length > 0) {
      ro.step = `${status.currentStepIndex}/${steps.length}`;
    }

    const { currentStep, currentStepIndex } = parseCurrentCanaryStep(resource);
    ro.setWeight = parseCurrentSetWeight(resource, currentStepIndex);

    ro.actualWeight = "0";

    if (!currentStep) {
      ro.actualWeight = "100";
    } else if (status?.availableReplicas > 0) {
      if (!spec?.strategy?.canary?.trafficRouting) {
        for (const rs of ro.replicaSets) {
          if (rs?.canary && rs?.available) {
            ro.actualWeight = `${rs.available / status.availableReplicas}`;
          }
        }
      } else {
        ro.actualWeight = ro.setWeight;
      }
    }
  } else {
    ro.strategy = CANARY;
    ro.setWeight = "0";
    ro.actualWeight = "100";
  }

  ro.containers = [];
  if (status?.summary?.images) {
    for (const c of status.summary.images) {
      ro.containers.push({ image: c });
    }
  }

  ro.current = status?.replicas;
  ro.available = status?.availableReplicas;
  return ro;
};

const parseCurrentCanaryStep = (
  resource: State
): { currentStep: any; currentStepIndex: number } => {
  const { status, spec } = resource;
  const canary = spec.strategy?.canary;
  if (!canary || !canary.steps || canary.steps.length === 0) {
    return { currentStep: null, currentStepIndex: -1 };
  }
  let currentStepIndex = 0;
  if (status.currentStepIndex) {
    currentStepIndex = status.currentStepIndex;
  }
  if (canary?.steps?.length <= currentStepIndex) {
    return { currentStep: null, currentStepIndex };
  }
  const currentStep = canary?.steps[currentStepIndex];
  return { currentStep, currentStepIndex };
};

const parseCurrentSetWeight = (
  resource: State,
  currentStepIndex: number
): string => {
  const { status, spec } = resource;
  if (status.abort) {
    return "0";
  }

  for (let i = currentStepIndex; i >= 0; i--) {
    const step = spec.strategy?.canary?.steps[i];
    if (step?.setWeight) {
      return step.setWeight;
    }
  }
  return "0";
};

const parseRevision = (node: any) => {
  for (const item of node.info || []) {
    if (item.name === REVISION) {
      const parts = item.value.split(":") || [];
      return parts.length === 2 ? parts[1] : "0";
    }
  }
};

const parsePodStatus = (pod: any) => {
  for (const item of pod.info || []) {
    if (item.name === STATUS_REASON) {
      return item.value;
    }
  }
};

const parseAnalysisRuns = (
  app: any,
  tree: any,
  rollout: any
): RolloutAnalysisRunInfo[] => {
  const [analysisRunResults, setAnalysisRunResults] = React.useState<
    RolloutAnalysisRunInfo[]
  >([]);
  const [analysisRunNodeIds, setAnalysisRunNodeIds] = React.useState<string[]>(
    []
  );
  const [isRefresh, setIsRefresh] = React.useState<boolean>(false);

  // Get the list of AnalysisRun node IDs whenever the tree or rollout props change
  React.useMemo(() => {
    const filteredNodes = tree.nodes.filter(
      (node: any) =>
        node.kind === ANALYSIS_RUN &&
        node.parentRefs.some((ref: any) => ref.name === rollout.metadata.name)
    );
    const nodeIds = filteredNodes.map((node: any) => node.uid);

    // Check if there are any new AnalysisRun node IDs or if the count has changed from previous node IDs
    if (
      nodeIds.length !== analysisRunNodeIds.length ||
      !analysisRunNodeIds.every((value, index) => value === nodeIds[index])
    ) {
      setIsRefresh(true);
    }
    setAnalysisRunNodeIds(nodeIds);
  }, [tree.nodes]);

  const rolloutAnalysisRunInfo = async () => {
    const promises: Promise<RolloutAnalysisRunInfo>[] = analysisRunNodeIds.map(
      async (nodeId) => {
        const node: any = tree.nodes.find((node: any) => node.uid === nodeId);

        const state = await getResource(
          app.metadata.name,
          app.metadata.namespace,
          node as any
        );
        return {
          objectMeta: {
            creationTimestamp: {
              seconds: node.createdAt,
            },
            name: node.name,
            namespace: node.namespace,
            resourceVersion: node.version,
            uid: node.uid,
          },
          specAndStatus: {
            spec: state.spec,
            status: state.status || null,
          },
          revision: parseRevision(node),
          status: parseAnalysisRunStatus(node.health.status),
        };
      }
    );

    const newAnalysisRunResults = await Promise.all(promises);
    setIsRefresh(false);
    setAnalysisRunResults(newAnalysisRunResults);
  };
  // Call the API call function only when isRefresh is true and AnalysisRun node IDs exist
  React.useEffect(() => {
    if (isRefresh && analysisRunNodeIds.length > 0) {
      rolloutAnalysisRunInfo();
    }
  }, [isRefresh, analysisRunNodeIds]);

  return analysisRunResults;
};

const parseAnalysisRunStatus = (status: string): string => {
  switch (status) {
    case HEALTHY:
      return SUCCESSFUL;
    case PROGRESSING:
      return RUNNING;
    case DEGRADED:
      return ERROR;
    default:
      return FAILURE;
  }
};

const parseReplicaSets = (tree: any, rollout: any): RolloutReplicaSetInfo[] => {
  const allReplicaSets = [];
  const allPods = [];
  for (const node of tree.nodes) {
    if (node.kind === REPLICA_SET) {
      allReplicaSets.push(node);
    } else if (node.kind === "Pod") {
      allPods.push(node);
    }
  }

  const ownedReplicaSets: { [key: string]: any } = {};

  for (const rs of allReplicaSets) {
    for (const parentRef of rs.parentRefs) {
      if (
        parentRef?.kind === ROLLOUT &&
        parentRef?.name === rollout?.metadata?.name
      ) {
        const pods = [];
        for (const pod of allPods) {
          const [parentRef] = pod.parentRefs;
          if (
            parentRef &&
            parentRef.kind === REPLICA_SET &&
            parentRef.name === rs.name
          ) {
            const ownedPod = {
              objectMeta: {
                name: pod.name,
                uid: pod.uid,
                namespace: pod.namespace,
                creationTimestamp: pod.creationTimestamp,
              },
              images: pod.images,
              status: parsePodStatus(pod),
              revision: parseRevision(rs),
              canary: true,
            };
            pods.push(ownedPod);
          }
        }
        ownedReplicaSets[rs?.name] = {
          objectMeta: {
            name: rs.name,
            uid: rs.uid,
            namespace: rs.namespace,
          },
          status: rs?.health.status,
          revision: parseRevision(rs),
          canary: true,
        };
        if (pods.length > 0) {
          ownedReplicaSets[rs?.name].pods = pods;
        }
      }
    }
  }

  return (Object.values(ownedReplicaSets) || []).map((rs) => {
    return rs;
  });
};

const getResource = (
  name: string | undefined,
  appNamespace: string | undefined,
  resource: any
): Promise<any> => {
  const params = {
    name,
    appNamespace,
    namespace: resource.namespace,
    resourceName: resource.name,
    version: resource.version,
    kind: resource.kind,
    group: resource.group || "",
  };

  return axios
    .get(`/api/v1/applications/${name}/resource`, { params })
    .then((response) => {
      const { manifest } = response.data;
      return JSON.parse(manifest);
    });
};

// tslint:disable-next-line:no-empty-interface
interface ApplicationResourceTree {}

export const ArgoRolloutComponent = (props: {
  application: any;
  tree: ApplicationResourceTree;
  resource: State;
}) => {
  const { application, tree } = props;
  const { resource } = application;
  const rolloutInfo = parseInfoFromResourceNode(application, tree, resource);

  return <RolloutWidget rollout={rolloutInfo} />;
};
