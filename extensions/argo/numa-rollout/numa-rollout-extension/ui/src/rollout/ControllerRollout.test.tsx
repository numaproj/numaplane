import { render } from "@testing-library/react";

import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { mockControllerRolloutProps } from "../../mocks/mockProps";
import { ControllerRollout } from "./ControllerRollout";
describe("Controller Rollout", () => {
  it("should render", () => {
    const { getByText } = render(
      <RolloutComponentContext.Provider
        value={mockControllerRolloutProps as any}
      >
        <ControllerRollout />
      </RolloutComponentContext.Provider>
    );
    expect(
      getByText("Controller Name : numaflow-controller")
    ).toBeInTheDocument();
  });
});
