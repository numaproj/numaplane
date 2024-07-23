import { render, screen } from "@testing-library/react";
import { CANARY } from "../../utils/Constants";
import { ArgoRolloutComponent } from "./ArgoRolloutComponent";
const rolloutParams = {
  strategy: CANARY,
  setWeight: "0",
  actualWeight: "100",
  step: "1",
};

//Mock the ArgoRolloutSummary component
jest.mock("./ArgoRolloutSummary", () => {
  return {
    ArgoRolloutSummary: () => {
      return <div>ArgoRolloutSummary</div>;
    },
  };
});

//Mock the ArgoRolloutContainers component
jest.mock("./ArgoRolloutContainers", () => {
  return {
    ArgoRolloutContainers: () => {
      return <div>ArgoRolloutContainers</div>;
    },
  };
});

describe("ArgoRolloutComponent", () => {
  it("should render ArgoRolloutComponent", () => {
    render(<ArgoRolloutComponent />);
    expect(screen.getByText("ArgoRolloutSummary")).toBeInTheDocument();
    expect(screen.getByText("ArgoRolloutContainers")).toBeInTheDocument();
  });
});
