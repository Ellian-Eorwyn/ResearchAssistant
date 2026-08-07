import { describe, expect, it } from "vitest";

import type { CaptureInputEvent, ResolveSourceRow } from "../api/types";
import {
  cdpMouseButton,
  coalesceInputEvents,
  domKeyEventToCdp,
  filterResolveRows,
  hostFromUrl,
  mapCanvasPointToViewport,
  modifierMask,
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
