import type { SearchOptionsResponse, SearchResultItem } from "../api/types";

type SearchTimeRange = SearchOptionsResponse["defaults"]["time_range"];

export interface SearchStartPayload {
  prompt: string;
  target_count: number;
  categories: string[];
  language: string;
  time_range: SearchTimeRange;
}

export const CURATED_SEARCH_CATEGORIES = ["general", "news", "it", "science", "map"] as const;

export function createFallbackSearchOptions(): SearchOptionsResponse {
  return {
    categories: [...CURATED_SEARCH_CATEGORIES],
    languages: [],
    time_ranges: [],
    supports_oa_doi_helper: false,
    defaults: {
      categories: ["general"],
      language: "",
      time_range: "",
    },
  };
}

export function formatSearchCategoryLabel(category: string): string {
  return category
    .split(/\s+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");
}

export function toggleSearchCategory(selected: string[], category: string): string[] {
  const normalized = category.trim();
  if (!normalized) return selected;
  if (selected.includes(normalized)) {
    if (selected.length === 1) return selected;
    return selected.filter((value) => value !== normalized);
  }
  return [...selected, normalized];
}

export function buildSearchStartPayload(input: {
  prompt: string;
  targetCount: number;
  categories: string[];
  language: string;
  timeRange: SearchTimeRange;
  defaults?: SearchOptionsResponse["defaults"];
}): SearchStartPayload {
  const normalizedCategories = input.categories.length
    ? input.categories
    : (input.defaults?.categories || []);
  return {
    prompt: input.prompt.trim(),
    target_count: input.targetCount,
    categories: normalizedCategories,
    language: input.language.trim() || input.defaults?.language || "",
    time_range: (input.timeRange || input.defaults?.time_range || "") as SearchTimeRange,
  };
}

export function buildSearchResultMetaTokens(result: SearchResultItem): string[] {
  const authors = result.authors.filter(Boolean).join(", ");
  const date = result.published_date ? result.published_date.slice(0, 10) : "";
  return [authors, result.doi, date].filter(Boolean);
}
