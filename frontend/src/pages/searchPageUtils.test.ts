import { describe, expect, it } from "vitest";

import type { SearchResultItem } from "../api/types";
import {
  buildSearchResultMetaTokens,
  buildSearchStartPayload,
  createFallbackSearchOptions,
  toggleSearchCategory,
} from "./searchPageUtils";

describe("searchPageUtils", () => {
  it("creates a fallback search options payload", () => {
    expect(createFallbackSearchOptions()).toEqual({
      categories: ["general", "news", "it", "science", "map"],
      languages: [],
      time_ranges: [],
      supports_oa_doi_helper: false,
      defaults: {
        categories: ["general"],
        language: "",
        time_range: "",
      },
    });
  });

  it("toggles categories while keeping at least one selected", () => {
    expect(toggleSearchCategory(["general"], "general")).toEqual(["general"]);
    expect(toggleSearchCategory(["general"], "science")).toEqual(["general", "science"]);
    expect(toggleSearchCategory(["general", "science"], "science")).toEqual(["general"]);
  });

  it("builds the search start payload with defaults", () => {
    const payload = buildSearchStartPayload({
      prompt: "  climate policy  ",
      targetCount: 150,
      categories: [],
      language: "",
      timeRange: "",
      defaults: {
        categories: ["news"],
        language: "auto",
        time_range: "",
      },
    });

    expect(payload).toEqual({
      prompt: "climate policy",
      target_count: 150,
      categories: ["news"],
      language: "auto",
      time_range: "",
    });
  });

  it("builds compact result metadata tokens", () => {
    const result: SearchResultItem = {
      url: "https://example.com/article",
      title: "Example",
      snippet: "",
      engine: "semantic scholar",
      engines: ["semantic scholar"],
      authors: ["Ada Lovelace", "Grace Hopper"],
      doi: "10.1234/example",
      html_url: "https://example.com/article",
      pdf_url: "https://example.com/article.pdf",
      searxng_score: 1,
      category: "science",
      published_date: "2026-04-10T00:00:00",
      relevance_score: 0.9,
      relevance_scored: true,
    };

    expect(buildSearchResultMetaTokens(result)).toEqual([
      "Ada Lovelace, Grace Hopper",
      "10.1234/example",
      "2026-04-10",
    ]);
  });
});
