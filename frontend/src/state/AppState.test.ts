import { describe, expect, it } from "vitest";

import { STAGE_NAMES, useExportSummaryText } from "./AppState";

describe("AppState helpers", () => {
  it("exposes stable stage labels", () => {
    expect(STAGE_NAMES.ingesting).toBe("Ingesting document");
    expect(STAGE_NAMES.matching_citations).toBe("Matching citations");
  });

  it("builds export summary text from result payload", () => {
    const summary = useExportSummaryText(
      {
        rows: [{ id: 1 }],
        matched_count: 1,
        unmatched_count: 0,
        total_bib_entries: 2,
      },
      {
        entries: [
          {
            ref_number: 1,
            authors: [],
            title: "A",
            year: "2024",
            url: "https://example.com/a?utm_source=x",
            doi: "",
            raw_text: "",
            parse_confidence: 1,
          },
          {
            ref_number: 2,
            authors: [],
            title: "B",
            year: "2024",
            url: "https://example.com/a",
            doi: "",
            raw_text: "",
            parse_confidence: 1,
          },
        ],
      },
    );

    expect(summary).toContain("1 rows exported");
    expect(summary).toContain("1 matched");
    expect(summary).toContain("2 bibliography entries");
    expect(summary).toContain("1 URLs ready for source tasks");
  });
});
