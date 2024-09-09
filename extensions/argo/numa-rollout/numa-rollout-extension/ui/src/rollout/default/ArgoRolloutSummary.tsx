import React from "react";

import "./ArgoSummary.css";

interface ArgoRolloutSummaryProps {
  rolloutParams: {
    strategy: string;
    setWeight: string;
    actualWeight: string;
  };
}
export const ArgoRolloutSummary = ({
  rolloutParams,
}: ArgoRolloutSummaryProps) => {
  return (
    <div className="info rollout__info">
      <div className="info__title">Summary</div>
      <div className="info-item--row">
        <div>
          <label>Strategy</label>
        </div>
        <div className="info-item--row__container">
          <div className="info-item info-item--canary">
            <span style={{ marginRight: "5px" }}>
              <i className="fa fa-dove"></i>
            </span>
            <div>{rolloutParams.strategy}</div>
          </div>
        </div>
      </div>
      <div className="rollout__info__section">
        <div className="info-item--row">
          <div>
            <label>Step</label>
          </div>
          <div className="info-item--row__container">
            <div className="info-item">
              <span style={{ marginRight: "5px" }}>
                <i className="fa fa-shoe-prints"></i>
              </span>
              <div>1/1</div>
            </div>
          </div>
        </div>
        <div className="info-item--row">
          <div>
            <label>Set Weight</label>
          </div>
          <div className="info-item--row__container">
            <div className="info-item">
              <span style={{ marginRight: "5px" }}>
                <i className="fa fa-balance-scale-right"></i>
              </span>
              <div>{rolloutParams.setWeight}</div>
            </div>
          </div>
        </div>
        <div className="info-item--row">
          <div>
            <label>Actual Weight</label>
          </div>
          <div className="info-item--row__container">
            <div className="info-item">
              <span style={{ marginRight: "5px" }}>
                <i className="fa fa-balance-scale"></i>
              </span>
              <div>{rolloutParams.actualWeight}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
