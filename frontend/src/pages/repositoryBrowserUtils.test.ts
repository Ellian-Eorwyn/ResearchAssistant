import { describe, expect, it } from "vitest";

import {
  buildRepositoryBrowserSourceTaskQueue,
  REPOSITORY_BROWSER_BIBLIOGRAPHY_COLUMNS,
  buildRepositoryBrowserQuery,
  buildRepositoryBrowserStorageKey,
  clampRepositoryBrowserColumnWidth,
  defaultRepositoryBrowserColumnWidth,
  migrateRepositoryBrowserVisibleColumns,
  moveRepositoryBrowserColumnToEnd,
  REPOSITORY_BROWSER_COLUMN_CATEGORIES,
  formatRepositoryBrowserExportFilename,
  nextRepositoryBrowserSort,
  REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS,
  REPOSITORY_BROWSER_PAGE_SIZE,
  reorderRepositoryBrowserColumns,
  resolveRepositoryBrowserColumnWidth,
  toggleRepositoryBrowserSelection,
} from "./repositoryBrowserUtils";

describe("repositoryBrowserUtils", () => {
  it("exposes stable default visible columns", () => {
    expect(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS).toEqual([
      "title",
      "author_names",
      "publication_year",
      "organization_type",
      "organization_name",
      "rating_overall_relevance",
    ]);
  });

  it("groups columns into categorized visibility sections with bibliography first", () => {
    expect(REPOSITORY_BROWSER_COLUMN_CATEGORIES[0]).toEqual({
      id: "bibliography",
      label: "Bibliography",
      columnKeys: REPOSITORY_BROWSER_BIBLIOGRAPHY_COLUMNS,
    });
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
      citationType: "report",
      citationDoi: "10.1234/example",
      citationReportNumber: "CEC-500-2025-029",
      citationStandardNumber: "",
      citationMissingFields: "authors",
      citationReady: "true",
      citationConfidenceMin: "0.8",
      citationConfidenceMax: "",
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
    expect(params.get("citation_type")).toBe("report");
    expect(params.get("citation_doi")).toBe("10.1234/example");
    expect(params.get("citation_report_number")).toBe("CEC-500-2025-029");
    expect(params.get("citation_missing_fields")).toBe("authors");
    expect(params.get("citation_ready")).toBe("true");
    expect(params.get("citation_confidence_min")).toBe("0.8");
    expect(params.get("sort_by")).toBe("rating_depth_score");
    expect(params.get("sort_dir")).toBe("desc");
    expect(params.get("limit")).toBe("100");
    expect(params.get("offset")).toBe("200");
  });

  it("scopes browser storage keys per repository path", () => {
    expect(buildRepositoryBrowserStorageKey("/tmp/repo-a")).not.toBe(
      buildRepositoryBrowserStorageKey("/tmp/repo-b"),
    );
    expect(buildRepositoryBrowserStorageKey("/tmp/repo-a")).toContain("repository-browser:v6:");
  });

  it("uses a fixed repository browser page size and clamps column widths", () => {
    expect(REPOSITORY_BROWSER_PAGE_SIZE).toBe(250);
    expect(clampRepositoryBrowserColumnWidth(40)).toBe(120);
    expect(clampRepositoryBrowserColumnWidth(810)).toBe(720);
    expect(clampRepositoryBrowserColumnWidth(242.2)).toBe(242);
    expect(defaultRepositoryBrowserColumnWidth("title")).toBe(320);
    expect(defaultRepositoryBrowserColumnWidth("file_pdf")).toBe(110);
    expect(resolveRepositoryBrowserColumnWidth({ title: 60 }, "title")).toBe(120);
    expect(resolveRepositoryBrowserColumnWidth({}, "title")).toBe(320);
  });

  it("migrates unchanged legacy visible columns to the new browser defaults", () => {
    expect(
      migrateRepositoryBrowserVisibleColumns(REPOSITORY_BROWSER_BIBLIOGRAPHY_COLUMNS),
    ).toEqual(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
    expect(
      migrateRepositoryBrowserVisibleColumns(["title", "summary_text", "organization_name"]),
    ).toEqual(["title", "summary_text", "organization_name"]);
  });

  it("cycles sort state from asc to desc to unsorted", () => {
    expect(nextRepositoryBrowserSort("", "", "title")).toEqual({
      sortBy: "title",
      sortDir: "asc",
    });
    expect(nextRepositoryBrowserSort("title", "asc", "title")).toEqual({
      sortBy: "title",
      sortDir: "desc",
    });
    expect(nextRepositoryBrowserSort("title", "desc", "title")).toEqual({
      sortBy: "",
      sortDir: "",
    });
    expect(nextRepositoryBrowserSort("author_names", "desc", "title")).toEqual({
      sortBy: "title",
      sortDir: "asc",
    });
  });

  it("reorders visible columns deterministically for drag and drop", () => {
    expect(
      reorderRepositoryBrowserColumns(
        ["title", "author_names", "publication_year", "organization_name"],
        "publication_year",
        "author_names",
      ),
    ).toEqual(["title", "publication_year", "author_names", "organization_name"]);

    expect(
      moveRepositoryBrowserColumnToEnd(
        ["title", "author_names", "publication_year", "organization_name"],
        "author_names",
      ),
    ).toEqual(["title", "publication_year", "organization_name", "author_names"]);
  });

  it("builds isolated enrichment queues in the expected order", () => {
    const queue = buildRepositoryBrowserSourceTaskQueue({
      draft: {
        rerun_failed_only: false,
        run_download: false,
        run_convert: false,
        run_catalog: true,
        run_citation_verify: true,
        run_llm_cleanup: true,
        run_llm_title: true,
        run_llm_summary: true,
        run_llm_rating: true,
        force_redownload: false,
        force_convert: false,
        force_catalog: false,
        force_citation_verify: false,
        force_llm_cleanup: false,
        force_title: false,
        force_summary: false,
        force_rating: false,
        project_profile_name: "",
        include_raw_file: true,
        include_rendered_html: true,
        include_rendered_pdf: true,
        include_markdown: true,
        scope: "empty_only",
        import_id: "",
      },
      scope: "selected",
      selectedSourceIds: ["000001", "000002"],
      defaultProjectProfileName: "default.yaml",
    });

    expect(queue.map((item) => item.id)).toEqual([
      "convert",
      "cleanup",
      "title",
      "catalog",
      "citation_verify",
      "summary",
      "rating",
    ]);
    expect(queue[0].payload.scope).toBe("empty_only");
    expect(queue[0].payload.source_ids).toEqual(["000001", "000002"]);
    expect(queue[0].payload.selected_phases).toEqual(["convert"]);
    expect(queue[2].payload.selected_phases).toEqual(["title"]);
    expect(queue[4].payload.run_citation_verify).toBe(true);
    expect(queue[5].payload.scope).toBe("all");
    expect(queue[5].payload.project_profile_name).toBe("default.yaml");
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
