import { render, screen } from "@testing-library/react";
import { mockISBRolloutProps } from "../../mocks/mockProps";
import {
  RolloutComponentContext,
  RolloutComponentWrapper,
} from "./RolloutComponentWrapper";

describe("RolloutComponentWrapper", () => {
  it("should render", () => {
    render(
      <RolloutComponentWrapper
        tree={mockISBRolloutProps.props.tree}
        resource={mockISBRolloutProps.props.resource as any}
        application={mockISBRolloutProps.props.application}
        {...mockISBRolloutProps}
      />
    );

    const doc = screen.getByText("Actual Weight");
    expect(doc).toBeInTheDocument();
  });
});
