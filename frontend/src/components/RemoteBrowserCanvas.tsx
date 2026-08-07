import { useCallback, useEffect, useRef, useState } from "react";

import { api, captureFrameUrl } from "../api/client";
import type { CaptureInputEvent } from "../api/types";
import {
  cdpMouseButton,
  coalesceInputEvents,
  domKeyEventToCdp,
  mapCanvasPointToViewport,
  modifierMask,
} from "../pages/resolveFetchesUtils";

const INPUT_FLUSH_MS = 30;
const ERROR_BACKOFF_MS = 1000;
const MAX_ERROR_BACKOFF_MS = 8000;

/**
 * A live view of the browser running on the server, with input forwarded back.
 *
 * Not an iframe: the sites worth resolving here send `X-Frame-Options` and
 * `frame-ancestors`, so they refuse to be framed. This draws JPEG frames the
 * server long-polls out of Chromium's compositor, which no header can block.
 */
export function RemoteBrowserCanvas({
  sessionId,
  viewportWidth,
  viewportHeight,
  onUrlChange,
  onError,
}: {
  sessionId: string;
  viewportWidth: number;
  viewportHeight: number;
  onUrlChange?: (url: string) => void;
  onError?: (message: string) => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const pendingEvents = useRef<CaptureInputEvent[]>([]);
  const flushTimer = useRef<number | null>(null);
  const buttonsDown = useRef(0);
  const [focused, setFocused] = useState(false);
  const [connected, setConnected] = useState(false);

  // ------------------------------------------------------------- frame loop

  useEffect(() => {
    if (!sessionId) return undefined;
    const controller = new AbortController();
    let seq = 0;
    let backoff = ERROR_BACKOFF_MS;
    let stopped = false;

    const draw = async (blob: Blob) => {
      const canvas = canvasRef.current;
      if (!canvas) return;
      const bitmap = await createImageBitmap(blob);
      const context = canvas.getContext("2d");
      if (context) {
        context.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
      }
      bitmap.close();
    };

    const loop = async () => {
      while (!stopped) {
        try {
          const response = await fetch(captureFrameUrl(sessionId, seq), {
            signal: controller.signal,
          });
          if (response.status === 204) {
            // Nothing repainted within the long-poll window; ask again.
            continue;
          }
          if (!response.ok) throw new Error(`frame request failed: ${response.status}`);

          seq = Number(response.headers.get("X-RA-Frame-Seq") || seq);
          const pageUrl = response.headers.get("X-RA-Page-Url");
          if (pageUrl && onUrlChange) onUrlChange(pageUrl);
          await draw(await response.blob());
          setConnected(true);
          backoff = ERROR_BACKOFF_MS;
        } catch (error) {
          if (stopped || controller.signal.aborted) return;
          setConnected(false);
          await new Promise((resolve) => setTimeout(resolve, backoff));
          backoff = Math.min(backoff * 2, MAX_ERROR_BACKOFF_MS);
        }
      }
    };

    void loop();
    return () => {
      stopped = true;
      controller.abort();
    };
  }, [sessionId, onUrlChange]);

  // ----------------------------------------------------------- input queue

  const flush = useCallback(async () => {
    flushTimer.current = null;
    const canvas = canvasRef.current;
    if (!canvas || pendingEvents.current.length === 0) return;
    const batch = coalesceInputEvents(pendingEvents.current);
    pendingEvents.current = [];
    try {
      await api.sendCaptureInput(sessionId, {
        canvas_width: canvas.width,
        canvas_height: canvas.height,
        events: batch,
      });
    } catch (error) {
      onError?.(error instanceof Error ? error.message : String(error));
    }
  }, [sessionId, onError]);

  const queue = useCallback(
    (event: CaptureInputEvent) => {
      pendingEvents.current.push(event);
      if (flushTimer.current === null) {
        flushTimer.current = window.setTimeout(() => void flush(), INPUT_FLUSH_MS);
      }
    },
    [flush],
  );

  const pointFrom = (event: { clientX: number; clientY: number }) => {
    const canvas = canvasRef.current;
    if (!canvas) return { x: 0, y: 0 };
    return mapCanvasPointToViewport(
      event.clientX,
      event.clientY,
      canvas.getBoundingClientRect(),
      canvas.width,
      canvas.height,
    );
  };

  // `wheel` must be non-passive to stop the page behind the canvas scrolling,
  // and React's synthetic handler registers it as passive.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return undefined;
    const onWheel = (event: WheelEvent) => {
      event.preventDefault();
      const point = pointFrom(event);
      queue({
        type: "mouseWheel",
        x: point.x,
        y: point.y,
        deltaX: event.deltaX,
        deltaY: event.deltaY,
        modifiers: modifierMask(event),
      });
    };
    canvas.addEventListener("wheel", onWheel, { passive: false });
    return () => canvas.removeEventListener("wheel", onWheel);
  }, [queue]);

  return (
    <div className="relative flex h-full w-full items-center justify-center overflow-hidden bg-surface-container-low">
      <canvas
        ref={canvasRef}
        width={viewportWidth}
        height={viewportHeight}
        tabIndex={0}
        className="max-h-full max-w-full cursor-default outline-none"
        onBlur={() => setFocused(false)}
        onContextMenu={(event) => event.preventDefault()}
        onFocus={() => setFocused(true)}
        onKeyDown={(event) => {
          event.preventDefault();
          queue(domKeyEventToCdp(event, "keyDown"));
        }}
        onKeyUp={(event) => {
          event.preventDefault();
          queue(domKeyEventToCdp(event, "keyUp"));
        }}
        onPaste={(event) => {
          event.preventDefault();
          const text = event.clipboardData.getData("text");
          if (text) queue({ type: "insertText", text });
        }}
        onPointerDown={(event) => {
          canvasRef.current?.focus();
          buttonsDown.current |= 1 << event.button;
          const point = pointFrom(event);
          queue({
            type: "mousePressed",
            x: point.x,
            y: point.y,
            button: cdpMouseButton(event.button),
            buttons: buttonsDown.current,
            clickCount: event.detail || 1,
            modifiers: modifierMask(event),
          });
        }}
        onPointerMove={(event) => {
          const point = pointFrom(event);
          queue({
            type: "mouseMoved",
            x: point.x,
            y: point.y,
            button: buttonsDown.current ? cdpMouseButton(event.button) : "none",
            buttons: buttonsDown.current,
            modifiers: modifierMask(event),
          });
        }}
        onPointerUp={(event) => {
          buttonsDown.current &= ~(1 << event.button);
          const point = pointFrom(event);
          queue({
            type: "mouseReleased",
            x: point.x,
            y: point.y,
            button: cdpMouseButton(event.button),
            buttons: buttonsDown.current,
            clickCount: event.detail || 1,
            modifiers: modifierMask(event),
          });
        }}
      />

      {!focused && (
        <button
          className="absolute inset-0 flex items-center justify-center bg-scrim/30 text-body-md font-semibold text-surface"
          type="button"
          onClick={() => canvasRef.current?.focus()}
        >
          <span className="rounded-md bg-surface-container px-4 py-2 text-on-surface">
            Click to interact with the page
          </span>
        </button>
      )}

      {!connected && (
        <div className="pointer-events-none absolute bottom-3 left-3 rounded-md bg-surface-container px-3 py-1 text-body-sm text-on-surface-variant">
          Connecting to the browser…
        </div>
      )}
    </div>
  );
}
