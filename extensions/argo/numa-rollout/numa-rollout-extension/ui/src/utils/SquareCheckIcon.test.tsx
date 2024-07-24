// Test SquareCancelIcon component

import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { SquareCheckIcon } from "./SquareCheckIcon";

describe("SquareCancelIcon", () => {
  it("should render the SquareCancelIcon component without errors", () => {
    const { container } = render(<SquareCheckIcon tooltipTitle="Cancel" />);
    expect(container).toBeInTheDocument();
  });
  it("should render correctly with an empty tooltipTitle", async () => {
    const tooltipTitle = "test1";
    render(<SquareCheckIcon tooltipTitle={tooltipTitle} />);
    const icon = screen.getByTestId(`tooltip-${tooltipTitle}`);
    expect(icon).toBeInTheDocument();
    fireEvent.mouseOver(icon);
    await waitFor(() => {
      const tooltip = screen.getByText(`${tooltipTitle}`);
      expect(tooltip).toBeInTheDocument();
    });
  });
});
