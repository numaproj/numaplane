import { render } from "@testing-library/react";

import { RolloutComponentContext } from "./RolloutComponentWrapper";
import { mockISBRolloutProps } from "../../mocks/mockProps";
import { ISBRollout } from "./ISBRollout";
describe("ISB Rollout", () => {
  it("should render", () => {
    const { getByText } = render(
      <RolloutComponentContext.Provider value={mockISBRolloutProps as any}>
        <ISBRollout />
      </RolloutComponentContext.Provider>
    );
    expect(getByText("ISB Name : my-isbsvc")).toBeInTheDocument();
  });
});
