import React, { useContext, useEffect, useState } from "react";

import "./ArgoRolloutComponent.css";
import { RolloutComponentContext } from "../RolloutComponentWrapper";
import { Node } from "../../ArgoPropType";
import {
  CONTROLLER_MANAGER,
  ISB_KUBERNETES,
  ISB_SERVICE_ROLLOUT,
  KUBERNETES_APP,
  NUMAFLOW_CONTROLLER_ROLLOUT,
  PIPELINE_ROLLOUT,
  POD,
  VERTEX,
} from "../../utils/Constants";

export const ArgoRolloutContainers = () => {
  const { props } = useContext(RolloutComponentContext);
  const [images, setImages] = useState<Set<string>>(new Set());
  useEffect(() => {
    const currentNodeKind = props?.resource?.kind;
    // Get all pods
    const allPods = props?.tree?.nodes?.filter(
      (node: Node) => node.kind === POD
    );
    let filteredPods: any[] = [];

    switch (currentNodeKind) {
      case ISB_SERVICE_ROLLOUT:
        filteredPods = allPods?.filter(
          (pod) =>
            pod?.networkingInfo?.labels &&
            pod.networkingInfo.labels[KUBERNETES_APP] === ISB_KUBERNETES
        );
        break;
      case NUMAFLOW_CONTROLLER_ROLLOUT:
        filteredPods = allPods?.filter(
          (pod) =>
            pod?.networkingInfo?.labels &&
            pod.networkingInfo.labels[KUBERNETES_APP] === CONTROLLER_MANAGER
        );
        break;
      case PIPELINE_ROLLOUT:
        filteredPods = allPods?.filter(
          (pod) =>
            pod?.networkingInfo?.labels &&
            pod.networkingInfo.labels[KUBERNETES_APP] === VERTEX
        );
        break;
      default:
        break;
    }
    let imgs: Set<string> = new Set();
    filteredPods.forEach((pod) => {
      pod.images?.forEach((image: string) => {
        imgs.add(image);
      });
    });
    setImages(imgs);
  }, [props?.tree]);

  return (
    <div className="info rollout__info">
      <div className="info__title">Containers</div>
      {images &&
        Array.from(images).map((dockerImages) => {
          return (
            <div
              style={{
                width: "100%",
                height: "2em",
                minWidth: 0,
                marginTop: "1rem",
                marginBottom: "1rem",
              }}
            >
              <input
                disabled
                className="ant-input ant-input-disabled"
                type="text"
                value={dockerImages}
                style={{ width: "100%", cursor: "default", color: "black" }}
              />
            </div>
          );
        })}
    </div>
  );
};
