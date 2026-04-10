import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "./client";

describe("api client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("includes mode and base_url when exporting a repository bundle", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(new Blob(["zip"]), {
        status: 200,
        headers: {
          "Content-Disposition": 'attachment; filename="repository-cloud-export.zip"',
          "X-ResearchAssistant-Requested-Count": "1",
          "X-ResearchAssistant-Exported-Count": "4",
          "X-ResearchAssistant-Skipped-Count": "0",
        },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.exportRepositoryBundle({
      mode: "cloud",
      scope: "selected",
      source_ids: ["000001"],
      file_kinds: ["pdf", "md"],
      base_url: "https://cdn.example.com/client-a/",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [, requestInit] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(requestInit.method).toBe("POST");
    expect(JSON.parse(String(requestInit.body))).toEqual({
      mode: "cloud",
      scope: "selected",
      source_ids: ["000001"],
      file_kinds: ["pdf", "md"],
      base_url: "https://cdn.example.com/client-a/",
    });
  });

  it("posts the expanded search payload", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          job_id: "job123",
          state: "pending",
          prompt: "climate policy",
          categories: ["news"],
          language: "en-US",
          time_range: "month",
          generated_queries: [],
          queries_completed: 0,
          total_queries: 0,
          results_found: 0,
          results_scored: 0,
          results_total: 0,
          results: [],
          error_message: "",
        }),
        { status: 200 },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.startSearch({
      prompt: "climate policy",
      target_count: 100,
      categories: ["news"],
      language: "en-US",
      time_range: "month",
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [path, requestInit] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(path).toBe("/api/search/start");
    expect(requestInit.method).toBe("POST");
    expect(JSON.parse(String(requestInit.body))).toEqual({
      prompt: "climate policy",
      target_count: 100,
      categories: ["news"],
      language: "en-US",
      time_range: "month",
    });
  });
});
