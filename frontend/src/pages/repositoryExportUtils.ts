export const DEFAULT_REPOSITORY_CLOUD_BASE_URL = "https://files.example.com/client-a/";

const RELATIVE_BASE_URL_PATTERN =
  /^(?:\.\.?\/)?[A-Za-z0-9._~!$&'()*+,;=:@%-]+(?:\/[A-Za-z0-9._~!$&'()*+,;=:@%-]+)*\/?$/;

export function normalizeRepositoryBundleBaseUrl(rawValue: string): string {
  const value = String(rawValue || "").trim();
  if (!value) {
    return "";
  }
  return value.replace(/\/+$/, "") + "/";
}

export function validateRepositoryBundleBaseUrlInput(rawValue: string): {
  normalizedValue: string;
  error: string;
} {
  const normalizedValue = normalizeRepositoryBundleBaseUrl(rawValue);
  if (!normalizedValue) {
    return {
      normalizedValue: "",
      error: "Enter the Base URL for the uploaded storage files.",
    };
  }

  if (/^https?:\/\//i.test(normalizedValue)) {
    try {
      const parsed = new URL(normalizedValue);
      if (!parsed.hostname) {
        throw new Error("missing-host");
      }
      return { normalizedValue, error: "" };
    } catch {
      return {
        normalizedValue: "",
        error: "Enter a valid http(s) URL or a relative preview path like ./files/.",
      };
    }
  }

  if (
    normalizedValue.startsWith("/") ||
    normalizedValue.startsWith("//") ||
    normalizedValue.includes("://") ||
    !RELATIVE_BASE_URL_PATTERN.test(normalizedValue)
  ) {
    return {
      normalizedValue: "",
      error: "Enter a valid http(s) URL or a relative preview path like ./files/.",
    };
  }

  return { normalizedValue, error: "" };
}
