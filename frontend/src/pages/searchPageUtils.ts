import { ApiError } from "../api/client";
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

function isNetworkLikeError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error || "");
  return /failed to fetch|networkerror|load failed|connection refused/i.test(message);
}

export function shouldRetrySearchOptionsLoad(error: unknown): boolean {
  if (error instanceof ApiError) {
    return error.status === 404 || error.status >= 500;
  }
  return isNetworkLikeError(error);
}

export function describeSearchOptionsError(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 404) {
      return "Advanced search options are unavailable from the running backend. Basic search controls are being used. Restart the backend if you just updated.";
    }
    if (error.status === 400) {
      return "The backend reports that SearXNG is not configured. Basic search controls are being used.";
    }
    if (error.status === 502) {
      return "The backend could not reach SearXNG to load live search options. Basic search controls are being used.";
    }
    if (error.status >= 500) {
      return "The backend could not load live search options. Basic search controls are being used.";
    }
  }
  if (isNetworkLikeError(error)) {
    return "Could not reach the backend to load live search options. Basic search controls are being used.";
  }
  return "Search options could not be loaded. Basic search controls are being used.";
}
