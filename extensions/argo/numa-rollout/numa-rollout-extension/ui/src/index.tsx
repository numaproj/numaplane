import * as React from "react";
import { ArgoPropType } from "./ArgoPropType";
import { RolloutComponentWrapper } from "./rollout/RolloutComponentWrapper";

export const roundNumber = (num: number, dig: number): number => {
  return Math.round(num * 10 ** dig) / 10 ** dig;
};

export const Extension = (props: ArgoPropType) => {
  return (
    <React.Fragment>
      <RolloutComponentWrapper
        tree={props.tree}
        resource={props.resource}
        application={props.application}
      />
    </React.Fragment>
  );
};

export const component = Extension;

// Register the component extension in ArgoCD

// registerResourceExtension(component: ExtensionComponent, group: string, kind: string, tabTitle: string)
((window: any) => {
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "numaplane.numaproj.io",
    "ISBServiceRollout",
    "Numarollout",
    { icon: "fa fa-window-restore" }
  );
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "numaplane.numaproj.io",
    "NumaflowControllerRollout",
    "Numarollout",
    { icon: "fa fa-window-restore" }
  );
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "numaplane.numaproj.io",
    "PipelineRollout",
    "Numarollout",
    { icon: "fa fa-window-restore" }
  );
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "numaplane.numaproj.io",
    "MonoVertexRollout",
    "Numarollout",
    { icon: "fa fa-window-restore" }
  );
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "*",
    "Deployment",
    "Numarollout",
    { icon: "fa fa-window-restore" }
  );
})(window);
