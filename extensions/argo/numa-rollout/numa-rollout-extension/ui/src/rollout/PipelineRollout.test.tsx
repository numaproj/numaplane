import { render } from "@testing-library/react";

import { PipelineRollout } from "./PipelineRollout";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { mockISBRolloutProps } from "../../mocks/mockProps";
describe("PipelineRollout", () => {
  it("should render", () => {
    const { getByText } = render(
      <RolloutComponentContext.Provider value={mockISBRolloutProps as any}>
        <PipelineRollout />
      </RolloutComponentContext.Provider>
    );
    expect(getByText("Pipeline Name: my-pipeline")).toBeInTheDocument();
  });
});
