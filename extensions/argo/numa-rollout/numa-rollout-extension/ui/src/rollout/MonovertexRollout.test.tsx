import React from "react";
import { render } from "@testing-library/react";
import { MonovertexRollout } from "./MonovertexRollout";
import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { mockMonovertexRolloutProps } from "../../mocks/mockProps";
describe("MonovertexRollout", () => {
  it("should render", () => {
    const { getByText } = render(
      <RolloutComponentContext.Provider
        value={mockMonovertexRolloutProps as any}
      >
        <MonovertexRollout />
      </RolloutComponentContext.Provider>
    );
    expect(getByText("Monovertex Rollout Name: my-mono")).toBeInTheDocument();
  });
});
