import { render, screen } from "@testing-library/react";
import { ArgoRolloutContainers } from "./ArgoRolloutContainers";
import { RolloutComponentContext } from "../RolloutComponentWrapper";
import {
  mockControllerRolloutProps,
  mockISBRolloutProps,
  mockPipelineRolloutProps,
} from "../../../mocks/mockProps";

describe("ArgoRolloutContainers", () => {
  it("should render for isb rollout", () => {
    render(
      <RolloutComponentContext.Provider value={mockISBRolloutProps as any}>
        <ArgoRolloutContainers />
      </RolloutComponentContext.Provider>
    );
    expect(
      screen.getByDisplayValue("docker.intuit.com/docker-rmt/nats:2.10.11")
    ).toBeInTheDocument();
  });

  it("should render for controller rollout", () => {
    render(
      <RolloutComponentContext.Provider
        value={mockControllerRolloutProps as any}
      >
        <ArgoRolloutContainers />
      </RolloutComponentContext.Provider>
    );
    expect(
      screen.getByDisplayValue(
        "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
      )
    ).toBeInTheDocument();
  });

  it("should render for pipeline rollout", () => {
    render(
      <RolloutComponentContext.Provider value={mockPipelineRolloutProps as any}>
        <ArgoRolloutContainers />
      </RolloutComponentContext.Provider>
    );
    expect(
      screen.getByDisplayValue(
        "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
      )
    ).toBeInTheDocument();
  });
});
