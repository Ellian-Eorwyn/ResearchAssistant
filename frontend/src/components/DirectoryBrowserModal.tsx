import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import type { DirectoryEntry } from "../api/types";

interface DirectoryBrowserModalProps {
  mode: "open" | "create" | "export";
  initialPath?: string;
  onSelect: (path: string) => void;
  onCancel: () => void;
}

export function DirectoryBrowserModal({
  mode,
  initialPath,
  onSelect,
  onCancel,
}: DirectoryBrowserModalProps) {
  const [currentPath, setCurrentPath] = useState(initialPath || "");
  const [entries, setEntries] = useState<DirectoryEntry[]>([]);
  const [parentPath, setParentPath] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [newFolderName, setNewFolderName] = useState("");
  const [pathInput, setPathInput] = useState(initialPath || "");
  const [showHidden, setShowHidden] = useState(false);
  const listRef = useRef<HTMLDivElement>(null);

  const fetchDirectory = useCallback(
    async (path: string) => {
      setLoading(true);
      setError("");
      try {
        const result = await api.browseDirectory(path, showHidden);
        setEntries(result.entries);
        setParentPath(result.parent_path);
        setCurrentPath(result.current_path);
        setPathInput(result.current_path);
        if (result.error) setError(result.error);
      } catch (err) {
        setError(String((err as Error).message || "Failed to browse directory"));
      } finally {
        setLoading(false);
      }
    },
    [showHidden],
  );

  useEffect(() => {
    fetchDirectory(currentPath);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [showHidden]);

  useEffect(() => {
    fetchDirectory(initialPath || "");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const navigateTo = useCallback(
    (path: string) => {
      fetchDirectory(path);
      if (listRef.current) listRef.current.scrollTop = 0;
    },
    [fetchDirectory],
  );

  const handlePathSubmit = useCallback(() => {
    const trimmed = pathInput.trim();
    if (trimmed) navigateTo(trimmed);
  }, [pathInput, navigateTo]);

  const handleSelect = useCallback(() => {
    if (mode === "create" && newFolderName.trim()) {
      const sep = currentPath.endsWith("/") ? "" : "/";
      onSelect(`${currentPath}${sep}${newFolderName.trim()}`);
    } else {
      onSelect(currentPath);
    }
  }, [mode, currentPath, newFolderName, onSelect]);

  const title =
    mode === "create"
      ? "Create New Repository"
      : mode === "export"
        ? "Select Export Destination"
        : "Open Repository";

  const selectLabel =
    mode === "create"
      ? "Create Here"
      : mode === "export"
        ? "Select Folder"
        : "Open";

  const pathSegments = currentPath.split("/").filter(Boolean);

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-surface/80 p-4 backdrop-blur-sm">
      <div
        className="flex w-full max-w-2xl flex-col rounded-xl border border-outline-variant/40 bg-surface-container shadow-2xl"
        style={{ maxHeight: "80vh" }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-outline-variant/30 px-5 py-4">
          <div className="text-title-sm font-semibold">{title}</div>
          <button
            className="rounded-sm px-2 py-1 text-label-sm text-on-surface-variant hover:text-on-surface"
            onClick={onCancel}
            type="button"
          >
            Close
          </button>
        </div>

        {/* Breadcrumb path */}
        <div className="flex items-center gap-1 overflow-x-auto border-b border-outline-variant/20 px-5 py-2 text-body-sm">
          <button
            className="shrink-0 rounded px-1.5 py-0.5 text-primary hover:bg-primary/10"
            onClick={() => navigateTo("/")}
            type="button"
          >
            /
          </button>
          {pathSegments.map((segment, i) => {
            const segmentPath = "/" + pathSegments.slice(0, i + 1).join("/");
            return (
              <span key={segmentPath} className="flex items-center gap-1">
                <span className="text-on-surface-variant/50">/</span>
                <button
                  className="shrink-0 rounded px-1.5 py-0.5 text-primary hover:bg-primary/10"
                  onClick={() => navigateTo(segmentPath)}
                  type="button"
                >
                  {segment}
                </button>
              </span>
            );
          })}
        </div>

        {/* Path input */}
        <div className="flex items-center gap-2 border-b border-outline-variant/20 px-5 py-2">
          <input
            className="flex-1 rounded border border-outline-variant/40 bg-surface-container-low px-3 py-1.5 text-body-sm text-on-surface outline-none focus:border-primary"
            value={pathInput}
            onChange={(e) => setPathInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") handlePathSubmit();
            }}
            placeholder="Enter path..."
          />
          <button
            className="shrink-0 rounded bg-primary/10 px-3 py-1.5 text-label-sm text-primary hover:bg-primary/20"
            onClick={handlePathSubmit}
            type="button"
          >
            Go
          </button>
          <label className="flex shrink-0 items-center gap-1.5 text-body-sm text-on-surface-variant">
            <input
              type="checkbox"
              checked={showHidden}
              onChange={(e) => setShowHidden(e.target.checked)}
            />
            Hidden
          </label>
        </div>

        {/* Directory listing */}
        <div ref={listRef} className="min-h-0 flex-1 overflow-y-auto px-2 py-1" style={{ maxHeight: "45vh" }}>
          {loading && (
            <div className="py-8 text-center text-body-sm text-on-surface-variant">
              Loading...
            </div>
          )}
          {error && (
            <div className="py-4 text-center text-body-sm text-error">{error}</div>
          )}
          {!loading && !error && entries.length === 0 && (
            <div className="py-8 text-center text-body-sm text-on-surface-variant">
              No subdirectories found
            </div>
          )}

          {/* Up button */}
          {!loading && parentPath && (
            <button
              className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left text-body-sm hover:bg-surface-container-high"
              onClick={() => navigateTo(parentPath)}
              type="button"
            >
              <span className="text-on-surface-variant">&#8593;</span>
              <span className="text-on-surface-variant">..</span>
            </button>
          )}

          {!loading &&
            entries.map((entry) => (
              <button
                key={entry.path}
                className="flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left text-body-sm hover:bg-surface-container-high"
                onDoubleClick={() => navigateTo(entry.path)}
                onClick={() => {
                  setCurrentPath(entry.path);
                  setPathInput(entry.path);
                }}
                type="button"
              >
                <span className="shrink-0">
                  {entry.is_ra_repo ? (
                    <span className="text-primary" title="ResearchAssistant repository">
                      &#128218;
                    </span>
                  ) : (
                    <span className="text-on-surface-variant">&#128193;</span>
                  )}
                </span>
                <span
                  className={
                    entry.is_ra_repo
                      ? "font-medium text-primary"
                      : "text-on-surface"
                  }
                >
                  {entry.name}
                </span>
                {entry.is_ra_repo && (
                  <span className="ml-auto text-label-sm text-on-surface-variant">
                    repository
                  </span>
                )}
              </button>
            ))}
        </div>

        {/* Create mode: new folder name */}
        {mode === "create" && (
          <div className="border-t border-outline-variant/20 px-5 py-3">
            <label className="text-label-sm text-on-surface-variant">
              New folder name
              <input
                className="mt-1 block w-full rounded border border-outline-variant/40 bg-surface-container-low px-3 py-1.5 text-body-sm text-on-surface outline-none focus:border-primary"
                value={newFolderName}
                onChange={(e) => setNewFolderName(e.target.value)}
                placeholder="my-research-project"
              />
            </label>
          </div>
        )}

        {/* Footer */}
        <div className="flex items-center justify-between border-t border-outline-variant/30 px-5 py-3">
          <div className="truncate text-body-sm text-on-surface-variant">
            {currentPath}
          </div>
          <div className="flex shrink-0 gap-2">
            <button
              className="rounded-lg px-4 py-2 text-label-md text-on-surface-variant hover:bg-surface-container-high"
              onClick={onCancel}
              type="button"
            >
              Cancel
            </button>
            <button
              className="rounded-lg bg-primary px-4 py-2 text-label-md text-on-primary hover:bg-primary/90"
              onClick={handleSelect}
              disabled={mode === "create" && !newFolderName.trim()}
              type="button"
            >
              {selectLabel}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
