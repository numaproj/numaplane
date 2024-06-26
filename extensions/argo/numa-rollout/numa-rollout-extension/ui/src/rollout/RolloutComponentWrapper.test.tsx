import React from "react";
import { render } from "@testing-library/react";
import "@testing-library/jest-dom";

import { RolloutComponentWrapper } from "./RolloutComponentWrapper";
import { ArgoPropType } from "../ArgoPropType";
import { mockProps } from "./rolloutObjectMockProps";

describe("RolloutComponentWrapper", () => {
  it("should render the component", () => {
    const { getByText } = render(
      <RolloutComponentWrapper {...(mockProps as any)} />
    );
    expect(
      getByText("This is Numarollout Component for ISBServiceRollout")
    ).toBeInTheDocument();
  });
});
