/**
 * Ways to hand a source the document you collected yourself.
 *
 * Sits beside the in-app browser rather than replacing it, because the sites
 * worth resolving by hand are exactly the ones the in-app browser also fails
 * on — the moment it hits a wall, this has to already be on screen.
 */
import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import type { WatchFolderFile } from "../api/types";
import { Button, SurfaceCard } from "./primitives";
import { describeWatchFile } from "../pages/resolveFetchesUtils";

const WATCH_POLL_MS = 4000;

interface AttachPanelProps {
  sourceId: string;
  finalUrl: string;
  /** Epoch ms of the last "open in my browser" — files newer than this are flagged. */
  sinceMs: number;
  /** A repository job is running, so captures would be refused. */
  disabled: boolean;
  busy: boolean;
  onAttachFile: (file: File) => void;
  onAttachPath: (path: string) => void;
}

export function AttachPanel({
  sourceId,
  finalUrl,
  sinceMs,
  disabled,
  busy,
  onAttachFile,
  onAttachPath,
}: AttachPanelProps) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [dragActive, setDragActive] = useState(false);
  const [nowMs, setNowMs] = useState(() => Date.now());

  // Ages are rendered relative to now, so they need a tick of their own or
  // "2s ago" stays "2s ago" until the next refetch.
  useEffect(() => {
    const timer = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  // Its own query key: the blocked-sources list can kick off a full
  // re-verification under the repository writer lock, which must never be put
  // on a polling interval.
  const watchQuery = useQuery({
    queryKey: ["watch-folder", sinceMs],
    queryFn: () => api.listWatchFolder(sinceMs),
    refetchInterval: WATCH_POLL_MS,
    enabled: Boolean(sourceId),
  });

  const files = watchQuery.data?.files ?? [];
  const newest = files.filter((file) => file.is_new);
  const rest = files.filter((file) => !file.is_new);

  const handleDrop = (event: React.DragEvent) => {
    event.preventDefault();
    setDragActive(false);
    if (disabled) return;
    const file = event.dataTransfer.files?.[0];
    if (file) onAttachFile(file);
  };

  const renderFile = (file: WatchFolderFile) => (
    <li
      key={file.path}
      className="flex items-center justify-between gap-2 rounded-md bg-surface-container-low p-2"
    >
      <div className="min-w-0">
        <div className="truncate text-body-md text-on-surface" title={file.name}>
          {file.name}
        </div>
        <div className="text-body-sm text-on-surface-variant">
          {describeWatchFile(file, nowMs)}
          {file.is_new ? " · new" : ""}
        </div>
      </div>
      <Button
        disabled={disabled || busy}
        variant={file.is_new ? "primary" : "secondary"}
        onClick={() => onAttachPath(file.path)}
      >
        Attach
      </Button>
    </li>
  );

  return (
    <SurfaceCard className="flex min-h-0 flex-col gap-3">
      <div>
        <div className="text-title-sm font-semibold text-on-surface">
          Attach a file to {sourceId}
        </div>
        <p className="mt-1 text-body-md text-on-surface-variant">
          Save the page in your own browser (File → Save Page As, or print to PDF). It is
          written into this source in place. Complete-page saves leave a <code>_files/</code>{" "}
          folder that is ignored, and <code>.mhtml</code> archives are not supported.
        </p>
      </div>

      {disabled && (
        <div className="rounded-md bg-warning/10 p-2 text-body-sm text-warning">
          A repository job is running. Attaching is paused until it finishes.
        </div>
      )}

      <div
        className={
          dragActive
            ? "rounded-lg border-2 border-dashed border-primary bg-primary/5 p-4 text-center"
            : "rounded-lg border-2 border-dashed border-outline-variant/50 p-4 text-center"
        }
        onDragLeave={() => setDragActive(false)}
        onDragOver={(event) => {
          event.preventDefault();
          if (!disabled) setDragActive(true);
        }}
        onDrop={handleDrop}
      >
        <div className="text-body-md text-on-surface-variant">
          Drop a saved page or PDF here
        </div>
        <input
          ref={fileInputRef}
          accept=".html,.htm,.xhtml,.pdf,.md,.markdown,.txt"
          className="hidden"
          type="file"
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) onAttachFile(file);
            event.target.value = "";
          }}
        />
        <Button
          className="mt-2"
          disabled={disabled || busy}
          variant="secondary"
          onClick={() => fileInputRef.current?.click()}
        >
          {busy ? "Attaching…" : "Choose a file"}
        </Button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <div className="mb-2 flex items-baseline justify-between gap-2">
          <span className="text-body-md font-semibold text-on-surface">Recent downloads</span>
          <span
            className="truncate text-body-sm text-on-surface-variant"
            title={watchQuery.data?.root || ""}
          >
            {watchQuery.data?.root || ""}
          </span>
        </div>

        {watchQuery.data?.error ? (
          <div className="rounded-md bg-error/10 p-2 text-body-sm text-error">
            {watchQuery.data.error}
          </div>
        ) : files.length === 0 ? (
          <div className="rounded-md bg-surface-container-low p-3 text-body-sm text-on-surface-variant">
            Nothing recent. Save the page in your browser and it will show up here.
          </div>
        ) : (
          <ul className="flex flex-col gap-2">
            {newest.map(renderFile)}
            {newest.length > 0 && rest.length > 0 && (
              <li className="pt-1 text-body-sm text-on-surface-variant">Earlier</li>
            )}
            {rest.map(renderFile)}
          </ul>
        )}
      </div>

      {finalUrl && (
        <div className="truncate text-body-sm text-on-surface-variant" title={finalUrl}>
          Recorded as: {finalUrl}
        </div>
      )}
    </SurfaceCard>
  );
}
