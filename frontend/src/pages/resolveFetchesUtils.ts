import type {
  CaptureInputEvent,
  RepositorySourceTaskRequest,
  ResolveSourceRow,
  WatchFolderFile,
} from "../api/types";

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

/**
 * The next source to work on after resolving one.
 *
 * Must be given the row list as it was *before* the resolved row disappeared
 * from it — once the list refetches, "the one after this" has no anchor.
 */
export function nextUnresolvedId(
  rows: ResolveSourceRow[],
  currentId: string,
  resolvedIds: string[],
): string {
  const done = new Set(resolvedIds);
  const index = rows.findIndex((row) => row.id === currentId);
  const after = index < 0 ? rows : rows.slice(index + 1);
  const forward = after.find((row) => !done.has(row.id));
  if (forward) return forward.id;
  // Nothing left below; wrap so a pass that started mid-list still finishes.
  const wrapped = rows.find((row) => !done.has(row.id) && row.id !== currentId);
  return wrapped?.id ?? "";
}

/** "12s ago", "4m ago" — how fresh a watched file is. */
export function formatFileAge(modifiedMs: number, nowMs: number): string {
  const seconds = Math.max(0, Math.round((nowMs - modifiedMs) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

/** Compact byte size for the watch list. */
export function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${Math.round(bytes / 1024)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

/** One line describing a watched file, for the attach list. */
export function describeWatchFile(file: WatchFolderFile, nowMs: number): string {
  return `${formatFileSize(file.size_bytes)} · ${formatFileAge(file.modified_ms, nowMs)}`;
}

export const RESOLVE_PROCESSING_PHASES = [
  "cleanup",
  "title",
  "catalog",
  "citation_verify",
  "summary",
  "rating",
] as const;

export type ResolveProcessingPhase = (typeof RESOLVE_PROCESSING_PHASES)[number];

export const RESOLVE_PHASE_LABELS: Record<ResolveProcessingPhase, string> = {
  cleanup: "LLM Markdown Cleanup",
  title: "Title Resolution",
  catalog: "Catalog Metadata",
  citation_verify: "Citation Verification",
  summary: "Summary",
  rating: "Rating",
};

export interface ResolveProcessingQueueInput {
  sourceIds: string[];
  draft: RepositorySourceTaskRequest;
  defaultProjectProfileName: string;
  phases?: readonly ResolveProcessingPhase[];
  overwriteExisting?: boolean;
}

export interface ResolveQueuedTask {
  id: ResolveProcessingPhase;
  label: string;
  payload: RepositorySourceTaskRequest;
}

/**
 * The background work that finishes a hand-attached source.
 *
 * One job per phase, because only one repository operation may run at a time —
 * the caller feeds these to `startSourceTaskQueue`, which drains them in order.
 */
export function buildResolveProcessingQueue({
  sourceIds,
  draft,
  defaultProjectProfileName,
  phases = RESOLVE_PROCESSING_PHASES,
  overwriteExisting = false,
}: ResolveProcessingQueueInput): ResolveQueuedTask[] {
  const ids = sourceIds.map((value) => value.trim()).filter(Boolean);
  if (ids.length === 0 || phases.length === 0) return [];

  const basePayload: RepositorySourceTaskRequest = {
    ...draft,
    // `scope: "selected"` is not a scope the backend knows; selection is
    // expressed by `source_ids`, exactly as the browser page does it.
    scope: "all",
    import_id: "",
    source_ids: ids,
    selected_phases: [],
    rerun_failed_only: false,
    // Never re-download. The whole point of these rows is that fetching them
    // reproduces the block — and it would overwrite the file just attached.
    run_download: false,
    force_redownload: false,
    // The capture already wrote the markdown. Re-converting a Markdown or PDF
    // attach that has no raw HTML risks blanking it.
    run_convert: false,
    force_convert: false,
    run_catalog: false,
    run_citation_verify: false,
    run_llm_cleanup: false,
    run_llm_title: false,
    run_llm_summary: false,
    run_llm_rating: false,
    force_catalog: false,
    force_citation_verify: false,
    force_llm_cleanup: false,
    force_title: false,
    force_summary: false,
    force_rating: false,
    include_raw_file: true,
    include_rendered_html: true,
    include_rendered_pdf: true,
    include_markdown: true,
    include_ocr_pdf: true,
    extract_media_links: true,
    download_media_transcript: true,
    download_media_video: false,
    download_media_audio: true,
    download_media_thumbnail: true,
    project_profile_name: draft.project_profile_name || defaultProjectProfileName,
  };

  const ordered = RESOLVE_PROCESSING_PHASES.filter((phase) => phases.includes(phase));
  return ordered.map((phase) => {
    const payload: RepositorySourceTaskRequest = {
      ...basePayload,
      selected_phases: [phase],
    };
    if (phase === "cleanup") {
      payload.run_llm_cleanup = true;
      // Cleanup skips on the mere presence of its output file, with no digest
      // check — without force it would keep the block page's cleaned text.
      payload.force_llm_cleanup = true;
    } else if (phase === "title") {
      payload.run_llm_title = true;
      // Same shape: `if row.title and not force`. The wall's title would survive.
      payload.force_title = true;
    } else if (phase === "catalog") {
      payload.run_catalog = true;
      payload.force_catalog = overwriteExisting;
    } else if (phase === "citation_verify") {
      payload.run_citation_verify = true;
      payload.force_citation_verify = overwriteExisting;
    } else if (phase === "summary") {
      payload.run_llm_summary = true;
      payload.force_summary = overwriteExisting;
    } else {
      payload.run_llm_rating = true;
      payload.force_rating = overwriteExisting;
    }
    return { id: phase, label: RESOLVE_PHASE_LABELS[phase], payload };
  });
}
