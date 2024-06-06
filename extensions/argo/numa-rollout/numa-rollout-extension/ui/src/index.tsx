import * as React from "react";
import { ArgoPropType } from "./ArgoPropType";

export const roundNumber = (num: number, dig: number): number => {
  return Math.round(num * 10 ** dig) / 10 ** dig;
};

export const Extension = (props: ArgoPropType) => {
  return (
    <React.Fragment>
      This is a Numarollout Extension. The props are : {JSON.stringify(props)}
    </React.Fragment>
  );
};

export const component = Extension;

// Register the component extension in ArgoCD
((window: any) => {
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "*",
    "Rollout",
    "Numarollout",
    { icon: "fa fa-chart-area" }
  );
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "",
    "Pod",
    "Numarollout",
    { icon: "fa fa-chart-area" }
  );
  window?.extensionsAPI?.registerResourceExtension(
    component,
    "*",
    "Deployment",
    "Numarollout",
    { icon: "fa fa-chart-area" }
  );
})(window);
