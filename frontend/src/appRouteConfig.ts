export const PRIMARY_APP_ROUTES = ["/browser", "/ai-guidance", "/settings"] as const;

export const COVERED_LEGACY_REDIRECTS = [
  { from: "project/ingest", to: "/browser" },
  { from: "project/documents", to: "/browser" },
  { from: "project/source-lists", to: "/browser" },
  { from: "project/merge", to: "/settings" },
  { from: "processing/source-capture", to: "/browser" },
  { from: "processing/summaries-ratings", to: "/browser" },
  { from: "data/manifest", to: "/browser" },
  { from: "data/repository-browser", to: "/browser" },
  { from: "research/purpose", to: "/ai-guidance" },
  { from: "research/project-profile", to: "/ai-guidance" },
  { from: "settings/llm-backend", to: "/settings" },
  { from: "settings/repository", to: "/settings" },
] as const;
