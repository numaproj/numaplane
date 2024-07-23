import { render, screen } from "@testing-library/react";
import { ArgoRolloutSummary } from "./ArgoRolloutSummary";

const rolloutParams = {
  strategy: "blueGreen",
  setWeight: 10,
  actualWeight: 100,
};
describe("ArgoRolloutSummary", () => {
  it("should render", () => {
    render(<ArgoRolloutSummary rolloutParams={rolloutParams as any} />);
    expect(screen.getByText("blueGreen")).toBeInTheDocument();
  });
});
