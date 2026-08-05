import type { CaptureInputEvent, ResolveSourceRow } from "../api/types";

export type ResolveFilter = "blocked" | "failed" | "partial" | "all";

export const RESOLVE_FILTER_INCLUDE: Record<ResolveFilter, string> = {
  blocked: "blocked",
  failed: "failed",
  partial: "partial",
  all: "blocked,failed,partial",
};

export function filterResolveRows(
  rows: ResolveSourceRow[],
  filter: ResolveFilter,
  query: string,
): ResolveSourceRow[] {
  const needle = query.trim().toLowerCase();
  return rows.filter((row) => {
    if (filter !== "all" && row.fetch_status !== filter) return false;
    if (!needle) return true;
    return (
      row.id.toLowerCase().includes(needle) ||
      row.title.toLowerCase().includes(needle) ||
      row.original_url.toLowerCase().includes(needle)
    );
  });
}

export function hostFromUrl(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url.replace(/^https?:\/\//, "").split("/")[0] || url;
  }
}

/**
 * Canvas pixels -> remote viewport pixels.
 *
 * The canvas is CSS-scaled to fit its pane, so a click at the same visual spot
 * lands at a different coordinate in the real page.
 */
export function mapCanvasPointToViewport(
  clientX: number,
  clientY: number,
  rect: { left: number; top: number; width: number; height: number },
  canvasWidth: number,
  canvasHeight: number,
): { x: number; y: number } {
  if (!rect.width || !rect.height) return { x: 0, y: 0 };
  return {
    x: ((clientX - rect.left) * canvasWidth) / rect.width,
    y: ((clientY - rect.top) * canvasHeight) / rect.height,
  };
}

/**
 * Drop superseded pointer moves before sending a batch.
 *
 * Only the newest position matters, and a 30ms flush window can easily collect
 * a dozen of them; forwarding all of them just adds latency.
 */
export function coalesceInputEvents(events: CaptureInputEvent[]): CaptureInputEvent[] {
  const result: CaptureInputEvent[] = [];
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index];
    if (event.type === "mouseMoved") {
      const laterMove = events
        .slice(index + 1)
        .some((candidate) => candidate.type === "mouseMoved");
      // Keep a move only if nothing else intervenes before the next one, since a
      // click needs the pointer to be in the right place first.
      const interveningNonMove = events
        .slice(index + 1)
        .findIndex((candidate) => candidate.type !== "mouseMoved");
      if (laterMove && interveningNonMove === -1) continue;
    }
    result.push(event);
  }
  return result;
}

/** CDP modifier bitmask: Alt=1, Ctrl=2, Meta=4, Shift=8. */
export function modifierMask(event: {
  altKey: boolean;
  ctrlKey: boolean;
  metaKey: boolean;
  shiftKey: boolean;
}): number {
  return (
    (event.altKey ? 1 : 0) |
    (event.ctrlKey ? 2 : 0) |
    (event.metaKey ? 4 : 0) |
    (event.shiftKey ? 8 : 0)
  );
}

const NAMED_KEY_CODES: Record<string, number> = {
  Backspace: 8,
  Tab: 9,
  Enter: 13,
  Escape: 27,
  " ": 32,
  PageUp: 33,
  PageDown: 34,
  End: 35,
  Home: 36,
  ArrowLeft: 37,
  ArrowUp: 38,
  ArrowRight: 39,
  ArrowDown: 40,
  Delete: 46,
};

/**
 * DOM keyboard event -> CDP key event.
 *
 * Printable keys carry `text` so the page receives the character; named keys
 * carry a virtual key code so Enter, Tab and the arrows behave.
 */
export function domKeyEventToCdp(
  event: {
    key: string;
    code: string;
    altKey: boolean;
    ctrlKey: boolean;
    metaKey: boolean;
    shiftKey: boolean;
  },
  type: "keyDown" | "keyUp",
): CaptureInputEvent {
  const modifiers = modifierMask(event);
  const isPrintable = event.key.length === 1 && !event.ctrlKey && !event.metaKey;
  const virtualKeyCode =
    NAMED_KEY_CODES[event.key] ??
    (event.key.length === 1 ? event.key.toUpperCase().charCodeAt(0) : undefined);

  const payload: CaptureInputEvent = {
    type,
    key: event.key,
    code: event.code,
    modifiers,
  };
  if (virtualKeyCode !== undefined) {
    payload.windowsVirtualKeyCode = virtualKeyCode;
  }
  if (type === "keyDown" && isPrintable) {
    payload.text = event.key;
    payload.unmodifiedText = event.key;
  }
  return payload;
}

/** DOM mouse button index -> the name CDP expects. */
export function cdpMouseButton(button: number): string {
  if (button === 1) return "middle";
  if (button === 2) return "right";
  if (button === 3) return "back";
  if (button === 4) return "forward";
  return "left";
}
