import { describe, expect, it } from "vitest";

import {
  DEFAULT_REPOSITORY_CLOUD_BASE_URL,
  normalizeRepositoryBundleBaseUrl,
  validateRepositoryBundleBaseUrlInput,
} from "./repositoryExportUtils";

describe("repositoryExportUtils", () => {
  it("exposes a stable default cloud base URL", () => {
    expect(DEFAULT_REPOSITORY_CLOUD_BASE_URL).toBe("https://files.example.com/client-a/");
  });

  it("normalizes absolute and relative base URLs with one trailing slash", () => {
    expect(normalizeRepositoryBundleBaseUrl("https://cdn.example.com/client-a")).toBe(
      "https://cdn.example.com/client-a/",
    );
    expect(normalizeRepositoryBundleBaseUrl("./files")).toBe("./files/");
    expect(normalizeRepositoryBundleBaseUrl("files///")).toBe("files/");
  });

  it("rejects blank cloud base URLs", () => {
    expect(validateRepositoryBundleBaseUrlInput("   ")).toEqual({
      normalizedValue: "",
      error: "Enter the Base URL for the uploaded storage files.",
    });
  });

  it("accepts absolute production URLs and relative preview paths", () => {
    expect(validateRepositoryBundleBaseUrlInput("https://cdn.example.com/client-a")).toEqual({
      normalizedValue: "https://cdn.example.com/client-a/",
      error: "",
    });
    expect(validateRepositoryBundleBaseUrlInput("./files")).toEqual({
      normalizedValue: "./files/",
      error: "",
    });
    expect(validateRepositoryBundleBaseUrlInput("files/")).toEqual({
      normalizedValue: "files/",
      error: "",
    });
  });

  it("rejects malformed or unsupported base URL values", () => {
    expect(validateRepositoryBundleBaseUrlInput("ftp://files.example.com/client-a/").error).toContain(
      "Enter a valid http(s) URL",
    );
    expect(validateRepositoryBundleBaseUrlInput("/files/").error).toContain(
      "Enter a valid http(s) URL",
    );
    expect(validateRepositoryBundleBaseUrlInput("files here/").error).toContain(
      "Enter a valid http(s) URL",
    );
  });
});
