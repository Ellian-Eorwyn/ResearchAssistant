import { describe, expect, it } from "vitest";

import {
  buildSpreadsheetManifestQuery,
  buildSpreadsheetStorageKey,
  clampSpreadsheetColumnWidth,
  nextSpreadsheetSort,
} from "./spreadsheetUtils";

describe("spreadsheetUtils", () => {
  it("builds spreadsheet manifest query params", () => {
    const params = buildSpreadsheetManifestQuery({
      q: "climate",
      sortBy: "source_001",
      sortDir: "desc",
      limit: 50,
      offset: 100,
    });
    expect(params.get("q")).toBe("climate");
    expect(params.get("sort_by")).toBe("source_001");
    expect(params.get("sort_dir")).toBe("desc");
    expect(params.get("limit")).toBe("50");
    expect(params.get("offset")).toBe("100");
  });

  it("builds a per-session storage key", () => {
    expect(buildSpreadsheetStorageKey("session-a", "target-1")).toBe(
      "spreadsheets:v1:session-a:target-1",
    );
  });

  it("clamps persisted column widths", () => {
    expect(clampSpreadsheetColumnWidth(80)).toBe(120);
    expect(clampSpreadsheetColumnWidth(900)).toBe(640);
    expect(clampSpreadsheetColumnWidth(222.2)).toBe(222);
  });

  it("cycles spreadsheet sort state", () => {
    expect(nextSpreadsheetSort("", "", "source_001")).toEqual({
      sortBy: "source_001",
      sortDir: "asc",
    });
    expect(nextSpreadsheetSort("source_001", "asc", "source_001")).toEqual({
      sortBy: "source_001",
      sortDir: "desc",
    });
    expect(nextSpreadsheetSort("source_001", "desc", "source_001")).toEqual({
      sortBy: "",
      sortDir: "",
    });
  });
});
