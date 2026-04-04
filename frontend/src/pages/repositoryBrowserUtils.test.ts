import { describe, expect, it } from "vitest";

import {
  buildRepositoryBrowserQuery,
  formatRepositoryBrowserExportFilename,
  REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS,
  toggleRepositoryBrowserSelection,
} from "./repositoryBrowserUtils";

describe("repositoryBrowserUtils", () => {
  it("exposes stable default visible columns", () => {
    expect(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS).toEqual([
      "id",
      "source_kind",
      "source_document_name",
      "title",
      "author_names",
      "publication_date",
      "document_type",
      "organization_name",
      "organization_type",
      "tags_text",
      "markdown_char_count",
      "summary_text",
      "rating_overall_relevance",
      "rating_depth_score",
      "rating_relevant_detail_score",
      "rating_rationale",
      "file_pdf",
      "file_html",
      "file_rendered",
      "file_md",
    ]);
  });

  it("builds repository-browser query parameters including threshold filters", () => {
    const params = buildRepositoryBrowserQuery({
      q: "alpha",
      fetchStatus: "success",
      detectedType: "pdf",
      sourceKind: "url",
      documentType: "report",
      organizationType: "agency",
      organizationName: "Alpha Agency",
      authorNames: "Jane Doe",
      publicationDate: "2024",
      tagsText: "housing",
      hasSummary: "true",
      hasRating: "false",
      ratingOverallRelevanceMin: "0.6",
      ratingOverallRelevanceMax: "0.9",
      ratingDepthScoreMin: "",
      ratingDepthScoreMax: "",
      ratingRelevantDetailScoreMin: "0.5",
      ratingRelevantDetailScoreMax: "",
      sortBy: "rating_depth_score",
      sortDir: "desc",
      limit: 100,
      offset: 200,
    });

    expect(params.get("q")).toBe("alpha");
    expect(params.get("fetch_status")).toBe("success");
    expect(params.get("detected_type")).toBe("pdf");
    expect(params.get("source_kind")).toBe("url");
    expect(params.get("document_type")).toBe("report");
    expect(params.get("organization_type")).toBe("agency");
    expect(params.get("organization_name")).toBe("Alpha Agency");
    expect(params.get("author_names")).toBe("Jane Doe");
    expect(params.get("publication_date")).toBe("2024");
    expect(params.get("tags_text")).toBe("housing");
    expect(params.get("has_summary")).toBe("true");
    expect(params.get("has_rating")).toBe("false");
    expect(params.get("rating_overall_min")).toBeNull();
    expect(params.get("rating_overall_relevance_min")).toBe("0.6");
    expect(params.get("rating_overall_relevance_max")).toBe("0.9");
    expect(params.get("rating_relevant_detail_score_min")).toBe("0.5");
    expect(params.get("sort_by")).toBe("rating_depth_score");
    expect(params.get("sort_dir")).toBe("desc");
    expect(params.get("limit")).toBe("100");
    expect(params.get("offset")).toBe("200");
  });

  it("supports shift-range selection and clear behavior", () => {
    const first = toggleRepositoryBrowserSelection({
      orderedIds: ["000001", "000002", "000003", "000004"],
      currentSelectedIds: new Set<string>(),
      targetId: "000002",
      checked: true,
      lastAnchorId: null,
      shiftKey: false,
    });

    expect(Array.from(first.selectedIds)).toEqual(["000002"]);

    const second = toggleRepositoryBrowserSelection({
      orderedIds: ["000001", "000002", "000003", "000004"],
      currentSelectedIds: first.selectedIds,
      targetId: "000004",
      checked: true,
      lastAnchorId: first.anchorId,
      shiftKey: true,
    });

    expect(Array.from(second.selectedIds).sort()).toEqual(["000002", "000003", "000004"]);

    const third = toggleRepositoryBrowserSelection({
      orderedIds: ["000001", "000002", "000003", "000004"],
      currentSelectedIds: second.selectedIds,
      targetId: "000003",
      checked: false,
      lastAnchorId: second.anchorId,
      shiftKey: false,
    });

    expect(Array.from(third.selectedIds).sort()).toEqual(["000002", "000004"]);
    expect(new Set<string>()).toEqual(new Set<string>());
  });

  it("formats flat export filenames with sanitization and collision suffixes", () => {
    const usedNames = new Set<string>();

    const first = formatRepositoryBrowserExportFilename(
      "000123",
      'A / Title: With <Bad> Characters?',
      ".md",
      usedNames,
    );
    const second = formatRepositoryBrowserExportFilename(
      "000123",
      'A / Title: With <Bad> Characters?',
      ".md",
      usedNames,
    );

    expect(first).toBe("000123 - A Title With Bad Characters.md");
    expect(second).toBe("000123 - A Title With Bad Characters (2).md");
  });
});
