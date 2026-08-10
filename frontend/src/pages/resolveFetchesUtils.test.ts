import { describe, expect, it } from "vitest";

import type {
  CaptureInputEvent,
  RepositorySourceTaskRequest,
  ResolveSourceRow,
} from "../api/types";
import {
  buildResolveProcessingQueue,
  cdpMouseButton,
  coalesceInputEvents,
  domKeyEventToCdp,
  filterResolveRows,
  formatFileAge,
  formatFileSize,
  hostFromUrl,
  mapCanvasPointToViewport,
  modifierMask,
  nextUnresolvedId,
} from "./resolveFetchesUtils";

function row(overrides: Partial<ResolveSourceRow>): ResolveSourceRow {
  return {
    id: "000001",
    title: "",
    original_url: "https://example.com/a",
    final_url: "",
    fetch_status: "blocked",
    fetch_verification: "blocked_challenge",
    http_status: 200,
    detected_type: "html",
    error_message: "",
    markdown_char_count: 0,
    fetched_at: "",
    current_content_preview: "",
    ...overrides,
  };
}

describe("filterResolveRows", () => {
  const rows = [
    row({ id: "000045", fetch_status: "blocked", title: "Reddit" }),
    row({ id: "000060", fetch_status: "failed", title: "Broken" }),
    row({ id: "000082", fetch_status: "partial", title: "A Video" }),
  ];

  it("narrows to one status", () => {
    expect(filterResolveRows(rows, "blocked", "").map((entry) => entry.id)).toEqual(["000045"]);
    expect(filterResolveRows(rows, "failed", "").map((entry) => entry.id)).toEqual(["000060"]);
  });

  it("keeps everything under the all filter", () => {
    expect(filterResolveRows(rows, "all", "")).toHaveLength(3);
  });

  it("matches id, title and url", () => {
    expect(filterResolveRows(rows, "all", "000082").map((entry) => entry.id)).toEqual(["000082"]);
    expect(filterResolveRows(rows, "all", "reddit").map((entry) => entry.id)).toEqual(["000045"]);
    expect(filterResolveRows(rows, "all", "example.com")).toHaveLength(3);
    expect(filterResolveRows(rows, "all", "nothing-here")).toHaveLength(0);
  });
});

describe("hostFromUrl", () => {
  it("extracts the host", () => {
    expect(hostFromUrl("https://www.reddit.com/r/eli5/comments/1")).toBe("www.reddit.com");
  });

  it("degrades gracefully on junk", () => {
    expect(hostFromUrl("not a url")).toBe("not a url");
  });
});

describe("mapCanvasPointToViewport", () => {
  it("scales a click from the displayed canvas to the remote viewport", () => {
    // The canvas is 1280x800 but displayed at half size.
    const point = mapCanvasPointToViewport(
      340,
      220,
      { left: 20, top: 20, width: 640, height: 400 },
      1280,
      800,
    );
    expect(point).toEqual({ x: 640, y: 400 });
  });

  it("returns the origin when the element has no size yet", () => {
    expect(mapCanvasPointToViewport(10, 10, { left: 0, top: 0, width: 0, height: 0 }, 1280, 800))
      .toEqual({ x: 0, y: 0 });
  });
});

describe("coalesceInputEvents", () => {
  it("keeps only the last of a run of pointer moves", () => {
    const events: CaptureInputEvent[] = [
      { type: "mouseMoved", x: 1, y: 1 },
      { type: "mouseMoved", x: 2, y: 2 },
      { type: "mouseMoved", x: 3, y: 3 },
    ];
    expect(coalesceInputEvents(events)).toEqual([{ type: "mouseMoved", x: 3, y: 3 }]);
  });

  it("keeps a move that positions the pointer before a click", () => {
    const events: CaptureInputEvent[] = [
      { type: "mouseMoved", x: 1, y: 1 },
      { type: "mousePressed", x: 1, y: 1 },
      { type: "mouseMoved", x: 9, y: 9 },
    ];
    expect(coalesceInputEvents(events)).toEqual(events);
  });

  it("leaves non-pointer events alone", () => {
    const events: CaptureInputEvent[] = [
      { type: "keyDown", key: "a" },
      { type: "keyUp", key: "a" },
    ];
    expect(coalesceInputEvents(events)).toEqual(events);
  });
});

describe("modifierMask", () => {
  it("packs modifiers into the CDP bitmask", () => {
    const none = { altKey: false, ctrlKey: false, metaKey: false, shiftKey: false };
    expect(modifierMask(none)).toBe(0);
    expect(modifierMask({ ...none, altKey: true })).toBe(1);
    expect(modifierMask({ ...none, ctrlKey: true })).toBe(2);
    expect(modifierMask({ ...none, metaKey: true })).toBe(4);
    expect(modifierMask({ ...none, shiftKey: true })).toBe(8);
    expect(modifierMask({ altKey: true, ctrlKey: true, metaKey: true, shiftKey: true })).toBe(15);
  });
});

describe("domKeyEventToCdp", () => {
  const base = { altKey: false, ctrlKey: false, metaKey: false, shiftKey: false };

  it("sends text for a printable key so the page receives the character", () => {
    const event = domKeyEventToCdp({ ...base, key: "a", code: "KeyA" }, "keyDown");
    expect(event.type).toBe("keyDown");
    expect(event.text).toBe("a");
    expect(event.windowsVirtualKeyCode).toBe(65);
  });

  it("omits text on key up", () => {
    expect(domKeyEventToCdp({ ...base, key: "a", code: "KeyA" }, "keyUp").text).toBeUndefined();
  });

  it("maps named keys to their virtual key codes", () => {
    expect(domKeyEventToCdp({ ...base, key: "Enter", code: "Enter" }, "keyDown")
      .windowsVirtualKeyCode).toBe(13);
    expect(domKeyEventToCdp({ ...base, key: "Backspace", code: "Backspace" }, "keyDown")
      .windowsVirtualKeyCode).toBe(8);
    expect(domKeyEventToCdp({ ...base, key: "ArrowDown", code: "ArrowDown" }, "keyDown")
      .windowsVirtualKeyCode).toBe(40);
    expect(domKeyEventToCdp({ ...base, key: "Tab", code: "Tab" }, "keyDown")
      .windowsVirtualKeyCode).toBe(9);
  });

  it("does not send text for a keyboard shortcut", () => {
    const event = domKeyEventToCdp(
      { ...base, ctrlKey: true, key: "c", code: "KeyC" },
      "keyDown",
    );
    expect(event.text).toBeUndefined();
    expect(event.modifiers).toBe(2);
  });
});

describe("cdpMouseButton", () => {
  it("names the DOM button indices", () => {
    expect(cdpMouseButton(0)).toBe("left");
    expect(cdpMouseButton(1)).toBe("middle");
    expect(cdpMouseButton(2)).toBe("right");
  });
});

describe("nextUnresolvedId", () => {
  const rows = [row({ id: "000001" }), row({ id: "000002" }), row({ id: "000003" })];

  it("moves to the next source below the one just resolved", () => {
    expect(nextUnresolvedId(rows, "000001", ["000001"])).toBe("000002");
  });

  it("skips sources already resolved in this pass", () => {
    expect(nextUnresolvedId(rows, "000001", ["000001", "000002"])).toBe("000003");
  });

  it("wraps so a pass started mid-list still finishes", () => {
    expect(nextUnresolvedId(rows, "000003", ["000003"])).toBe("000001");
  });

  it("returns nothing when every source is resolved", () => {
    expect(nextUnresolvedId(rows, "000003", ["000001", "000002", "000003"])).toBe("");
  });

  it("falls back to the first unresolved row when the current id is gone", () => {
    expect(nextUnresolvedId(rows, "999999", ["000001"])).toBe("000002");
  });

  it("handles an empty list", () => {
    expect(nextUnresolvedId([], "000001", [])).toBe("");
  });
});

describe("formatFileAge", () => {
  const now = 1_800_000_000_000;

  it("counts seconds, minutes, hours and days", () => {
    expect(formatFileAge(now - 12_000, now)).toBe("12s ago");
    expect(formatFileAge(now - 4 * 60_000, now)).toBe("4m ago");
    expect(formatFileAge(now - 3 * 3_600_000, now)).toBe("3h ago");
    expect(formatFileAge(now - 2 * 86_400_000, now)).toBe("2d ago");
  });

  it("never reports a negative age for a clock skew", () => {
    expect(formatFileAge(now + 5_000, now)).toBe("0s ago");
  });
});

describe("formatFileSize", () => {
  it("scales the unit to the size", () => {
    expect(formatFileSize(512)).toBe("512 B");
    expect(formatFileSize(2048)).toBe("2 KB");
    expect(formatFileSize(5 * 1024 * 1024)).toBe("5.0 MB");
  });
});

describe("buildResolveProcessingQueue", () => {
  const draft = { project_profile_name: "" } as unknown as RepositorySourceTaskRequest;

  const build = (overrides: Partial<Parameters<typeof buildResolveProcessingQueue>[0]> = {}) =>
    buildResolveProcessingQueue({
      sourceIds: ["000001", "000002"],
      draft,
      defaultProjectProfileName: "profile.yaml",
      ...overrides,
    });

  it("never re-downloads, because fetching these rows reproduces the block", () => {
    for (const task of build()) {
      expect(task.payload.run_download).toBe(false);
      expect(task.payload.force_redownload).toBe(false);
    }
  });

  it("never re-converts, because the capture already wrote the markdown", () => {
    for (const task of build()) {
      expect(task.payload.run_convert).toBe(false);
      expect(task.payload.force_convert).toBe(false);
    }
  });

  it("selects rows by id under the 'all' scope the backend understands", () => {
    for (const task of build()) {
      expect(task.payload.scope).toBe("all");
      expect(task.payload.source_ids).toEqual(["000001", "000002"]);
      expect(task.payload.import_id).toBe("");
    }
  });

  it("forces cleanup and title, which skip on presence rather than digest", () => {
    const tasks = build();
    const cleanup = tasks.find((task) => task.id === "cleanup");
    const title = tasks.find((task) => task.id === "title");
    expect(cleanup?.payload.force_llm_cleanup).toBe(true);
    expect(title?.payload.force_title).toBe(true);
  });

  it("leaves the digest-gated phases unforced unless overwriting", () => {
    const tasks = build();
    expect(tasks.find((task) => task.id === "catalog")?.payload.force_catalog).toBe(false);
    expect(tasks.find((task) => task.id === "summary")?.payload.force_summary).toBe(false);
    expect(tasks.find((task) => task.id === "rating")?.payload.force_rating).toBe(false);

    const forced = build({ overwriteExisting: true });
    expect(forced.find((task) => task.id === "catalog")?.payload.force_catalog).toBe(true);
    expect(forced.find((task) => task.id === "summary")?.payload.force_summary).toBe(true);
  });

  it("emits one job per phase, in dependency order", () => {
    expect(build().map((task) => task.id)).toEqual([
      "cleanup",
      "title",
      "catalog",
      "citation_verify",
      "summary",
      "rating",
    ]);
    for (const task of build()) {
      expect(task.payload.selected_phases).toEqual([task.id]);
    }
  });

  it("honours a narrowed phase selection but keeps the canonical order", () => {
    expect(build({ phases: ["summary", "cleanup"] }).map((task) => task.id)).toEqual([
      "cleanup",
      "summary",
    ]);
  });

  it("falls back to the repository's default project profile", () => {
    expect(build()[0].payload.project_profile_name).toBe("profile.yaml");
  });

  it("returns nothing when there is nothing to process", () => {
    expect(build({ sourceIds: [] })).toEqual([]);
    expect(build({ sourceIds: ["  "] })).toEqual([]);
    expect(build({ phases: [] })).toEqual([]);
  });
});
