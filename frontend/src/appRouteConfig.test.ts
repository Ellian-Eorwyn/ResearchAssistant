import { describe, expect, it } from "vitest";

import { COVERED_LEGACY_REDIRECTS } from "./appRouteConfig";
import { LEGACY_NAV_GROUPS, PRIMARY_NAV } from "./layout/AppShell";

describe("app navigation config", () => {
  it("keeps only the three primary nav destinations at top level", () => {
    expect(PRIMARY_NAV).toEqual([
      { label: "Browser", to: "/browser" },
      { label: "AI Guidance", to: "/ai-guidance" },
      { label: "Settings", to: "/settings" },
    ]);
  });

  it("keeps legacy destinations grouped under the legacy section", () => {
    const legacyLabels = LEGACY_NAV_GROUPS.flatMap((group) => group.items.map((item) => item.label));
    expect(legacyLabels).toContain("Overview");
    expect(legacyLabels).toContain("Citation Extraction");
    expect(legacyLabels).toContain("Job History");
    expect(legacyLabels).toContain("Advanced");
  });

  it("redirects covered legacy routes into the new primary pages", () => {
    expect(COVERED_LEGACY_REDIRECTS).toEqual(
      expect.arrayContaining([
        { from: "data/manifest", to: "/browser" },
        { from: "data/repository-browser", to: "/browser" },
        { from: "processing/source-capture", to: "/browser" },
        { from: "project/merge", to: "/settings" },
        { from: "research/purpose", to: "/ai-guidance" },
        { from: "settings/llm-backend", to: "/settings" },
      ]),
    );
  });
});
